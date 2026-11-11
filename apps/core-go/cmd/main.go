package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	fiberotel "github.com/gofiber/contrib/v3/otel"
	"github.com/gofiber/fiber/v3"
	"github.com/golang-jwt/jwt/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"golang.org/x/crypto/bcrypt"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "joint-purchases/core-go/proto"
	"joint-purchases/core-go/proto/protoconnect"
)

type User struct {
	ID        uint
	Username  string
	Password  string
	CreatedAt time.Time
}

type Claims struct {
	UserID string `json:"user_id"`
	jwt.RegisteredClaims
}

type AuthRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

var (
	jwtSecret    = []byte(os.Getenv("JWT_SECRET"))
	db           *sql.DB
	rdb          *redis.Client
	grpcClient   pb.CatalogServiceClient
	ctxBg        = context.Background()
	requestGroup singleflight.Group
)

// CatalogServer реализует бинарный контракт Connect-RPC для основного трафика
type CatalogServer struct{}

func (s *CatalogServer) GetProductDetails(ctx context.Context, req *connect.Request[pb.ProductRequest]) (*connect.Response[pb.ProductResponse], error) {
	id := req.Msg.ProductId
	cacheKey := fmt.Sprintf("catalog:product:%s", id)

	// 1. Быстрое чтение из Dragonfly
	if cached, err := rdb.Get(ctx, cacheKey).Result(); err == nil {
		var resp pb.ProductResponse
		if json.Unmarshal([]byte(cached), &resp) == nil {
			res := connect.NewResponse(&resp)
			res.Header().Set("X-Cache", "HIT")
			return res, nil
		}
	}

	// 2. Защита от «собачьей свалки» (Cache Stampede) через Singleflight
	v, err, _ := requestGroup.Do(cacheKey, func() (any, error) {
		if freshCache, cacheErr := rdb.Get(ctxBg, cacheKey).Result(); cacheErr == nil {
			var resp pb.ProductResponse
			if json.Unmarshal([]byte(freshCache), &resp) == nil {
				return &resp, nil
			}
		}

		detachedCtx := context.WithoutCancel(ctx)
		grpcCtx, cancel := context.WithTimeout(detachedCtx, 5*time.Second)
		defer cancel()

		resp, grpcErr := grpcClient.GetProductDetails(grpcCtx, req.Msg)
		if grpcErr != nil {
			return nil, grpcErr
		}

		if data, mErr := json.Marshal(resp); mErr == nil {
			_ = rdb.Set(ctxBg, cacheKey, data, 1*time.Hour).Err()
		}
		return resp, nil
	})

	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	res := connect.NewResponse(v.(*pb.ProductResponse))
	res.Header().Set("X-Cache", "MISS")
	return res, nil
}

func (s *CatalogServer) AddProduct(ctx context.Context, req *connect.Request[pb.AddProductRequest]) (*connect.Response[pb.AddProductResponse], error) {
	// Доверяем заголовку X-User-ID, который KrakenD проставил после проверки JWT
	userID := req.Header().Get("X-User-ID")
	if userID == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, fmt.Errorf("missing X-User-ID header"))
	}
	req.Msg.OwnerId = userID

	grpcCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := grpcClient.AddProduct(grpcCtx, req.Msg)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(resp), nil
}

// Инициализация сквозного трейсинга OpenTelemetry для отправки в Grafana LGTM
func initTracer() func(context.Context) error {
	exporter, err := otlptracegrpc.New(ctxBg, otlptracegrpc.WithInsecure())
	if err != nil {
		log.Printf("⚠️ Предупреждение: не удалось запустить OTel экспортер: %v", err)
		return func(context.Context) error { return nil }
	}

	res, _ := resource.New(ctxBg,
		resource.WithAttributes(semconv.ServiceNameKey.String("core-go-service")),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	return tp.Shutdown
}

func generateTokenPair(userID string) (string, string, error) {
	accessClaims := &Claims{
		UserID: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, accessClaims)
	// Добавляем kid для мгновенного поиска ключа в jwk.json шлюза KrakenD
	token.Header["kid"] = "core-go-hs256-key"

	accessToken, err := token.SignedString(jwtSecret)
	if err != nil {
		return "", "", err
	}

	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", err
	}
	refreshToken := hex.EncodeToString(b)

	redisKey := fmt.Sprintf("refresh:%s", refreshToken)
	err = rdb.Set(ctxBg, redisKey, userID, 30*24*time.Hour).Err()
	if err != nil {
		return "", "", err
	}

	return accessToken, refreshToken, nil
}

func main() {
	// Запускаем телеметрию OpenTelemetry с безопасным таймаутом при завершении
	shutdown := initTracer()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdown(ctx); err != nil {
			log.Printf("Ошибка при остановке OTel экспортера: %v", err)
		}
	}()

	systemDSN := "postgres://yugabyte:yugabyte@yugabytedb:5433/yugabyte"
	var err error

	for i := 0; i < 15; i++ {
		db, err = sql.Open("pgx", systemDSN)
		if err == nil {
			err = db.Ping()
		}
		if err == nil {
			break
		}
		log.Printf("Ожидание запуска кластера YugabyteDB (Попытка %d/15)...", i+1)
		time.Sleep(4 * time.Second)
	}
	if err != nil {
		log.Fatalf("Ошибка подключения к YugabyteDB: %v", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	_, _ = db.Exec("CREATE DATABASE catalog_db;")
	log.Println("Распределенная база данных 'catalog_db' для Python верифицирована.")

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(100) UNIQUE NOT NULL,
			password TEXT NOT NULL,
			created_at TIMESTAMP NOT NULL
		);
	`)
	if err != nil {
		log.Fatalf("Ошибка создания таблицы пользователей: %v", err)
	}
	log.Println("Нативная схема таблицы 'users' успешно верифицирована.")

	rdb = redis.NewClient(&redis.Options{
		Addr: os.Getenv("DRAGONFLY_URL"),
	})
	if err := rdb.Ping(ctxBg).Err(); err != nil {
		log.Fatalf("Ошибка подключения к Dragonfly: %v", err)
	}

	// Подключаем gRPC с перехватчиком OpenTelemetry для проброса Trace-ID в Python
	grpcConn, err := grpc.NewClient(
		"data-python:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Fatalf("Ошибка gRPC канала к data-python: %v", err)
	}
	grpcClient = pb.NewCatalogServiceClient(grpcConn)

	app := fiber.New(fiber.Config{
		ErrorHandler: func(ctx fiber.Ctx, err error) error {
			return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		},
	})

	// Включаем перехват входящих трейсов от шлюза KrakenD
	app.Use(fiberotel.Middleware())

	app.Post("/auth/register", func(c fiber.Ctx) error {
		var req AuthRequest
		if err := c.Bind().Body(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Bad request body"})
		}

		hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), 12)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Password encryption failed"})
		}

		query := `INSERT INTO users (username, password, created_at) VALUES ($1, $2, $3)`
		_, err = db.Exec(query, req.Username, string(hashedPassword), time.Now())
		if err != nil {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": "User registration failed or duplicate"})
		}
		return c.Status(fiber.StatusOK).JSON(fiber.Map{"status": "registered"})
	})

	app.Post("/auth/login", func(c fiber.Ctx) error {
		var req AuthRequest
		if err := c.Bind().Body(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Bad request body"})
		}

		var user User
		query := `SELECT id, username, password FROM users WHERE username = $1 LIMIT 1`
		err := db.QueryRow(query, req.Username).Scan(&user.ID, &user.Username, &user.Password)

		if err != nil {
			if err == sql.ErrNoRows {
				return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Invalid credentials"})
			}
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Database error"})
		}

		if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.Password)); err != nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Invalid credentials"})
		}

		accessToken, refreshToken, err := generateTokenPair(fmt.Sprintf("%d", user.ID))
		if err != nil {
			return err
		}

		c.Cookie(&fiber.Cookie{
			Name:     "refresh_token",
			Value:    refreshToken,
			Expires:  time.Now().Add(30 * 24 * time.Hour),
			HTTPOnly: true,
			Secure:   false,
			SameSite: "Strict",
			Path:     "/",
		})

		return c.JSON(fiber.Map{"access_token": accessToken})
	})


	app.Post("/auth/refresh", func(c fiber.Ctx) error {
		refreshTokenStr := c.Cookies("refresh_token")
		if refreshTokenStr == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Refresh token is missing"})
		}

		redisKey := fmt.Sprintf("refresh:%s", refreshTokenStr)
		storedUser, err := rdb.Get(ctxBg, redisKey).Result()
		if err != nil || storedUser == "" {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Session expired or revoked"})
		}

		rdb.Del(ctxBg, redisKey)

		newAccessToken, newRefreshToken, err := generateTokenPair(storedUser)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "Token generation failed"})
		}

		c.Cookie(&fiber.Cookie{
			Name:     "refresh_token",
			Value:    newRefreshToken,
			Expires:  time.Now().Add(30 * 24 * time.Hour),
			HTTPOnly: true,
			Secure:   false,
			SameSite: "Strict",
			Path:     "/",
		})

		return c.JSON(fiber.Map{"access_token": newAccessToken})
	})


		// Выход
	app.Post("/auth/logout", func(c fiber.Ctx) error {
		refreshTokenStr := c.Cookies("refresh_token")
		if refreshTokenStr != "" {
			rdb.Del(ctxBg, fmt.Sprintf("refresh:%s", refreshTokenStr))
		}
		c.Cookie(&fiber.Cookie{
			Name:     "refresh_token",
			Value:    "",
			Expires:  time.Now().Add(-24 * time.Hour),
			HTTPOnly: true,
			Path:     "/",
		})
		return c.JSON(fiber.Map{"status": "logged_out"})
	})

	// 🌟 CQRS ПОТОК ЧТЕНИЯ: Получение товара из Dragonfly с защитой Singleflight
	app.Get("/catalog/product/:id", func(c fiber.Ctx) error {
		authHeader := c.Get("Authorization")
		if len(authHeader) < 8 || authHeader[:7] != "Bearer " {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Missing or invalid access token"})
		}
		tokenStr := authHeader[7:]

		claims := &Claims{}
		token, err := jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (interface{}, error) {
			return jwtSecret, nil
		})
		if err != nil || !token.Valid {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Access token expired or malformed"})
		}

		id := c.Params("id")
		cacheKey := fmt.Sprintf("catalog:product:%s", id)

		// 1. Быстрая проверка кэша в Dragonfly
		cachedProduct, err := rdb.Get(ctxBg, cacheKey).Result()
		if err == nil {
			c.Set("X-Cache", "HIT")
			c.Set("Content-Type", "application/json")
			return c.Status(fiber.StatusOK).SendString(cachedProduct)
		}

		// 2. Cache MISS: объединяем все одновременные запросы одного товара в один
		v, err, _ := requestGroup.Do(cacheKey, func() (any, error) {
			// Повторная проверка: возможно, соседняя горутина уже успела заполнить кэш
			if freshCache, cacheErr := rdb.Get(ctxBg, cacheKey).Result(); cacheErr == nil {
				return freshCache, nil
			}

			// Сохраняем trace_id от c.UserContext(), но защищаем запрос от отмены при закрытии вкладки первым клиентом
			detachedCtx := context.WithoutCancel(c.UserContext())
			grpcCtx, cancel := context.WithTimeout(detachedCtx, 5*time.Second)
			defer cancel()

			resp, grpcErr := grpcClient.GetProductDetails(grpcCtx, &pb.ProductRequest{ProductId: id})
			if grpcErr != nil {
				return nil, grpcErr
			}

			productMap := fiber.Map{
				"product_id":       resp.ProductId,
				"title":            resp.Title,
				"jsonb_attributes": resp.JsonbAttributes,
			}

			actualJSON, marshalErr := json.Marshal(productMap)
			if marshalErr != nil {
				return nil, marshalErr
			}

			_ = rdb.Set(ctxBg, cacheKey, actualJSON, 1*time.Hour).Err()
			return string(actualJSON), nil
		})

		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}

		c.Set("X-Cache", "MISS")
		c.Set("Content-Type", "application/json")
		return c.Status(fiber.StatusOK).SendString(v.(string))
	})

	// 🌟 CQRS ПОТОК ЗАПИСИ: Роут добавления нового товара через gRPC в Python контур
	app.Post("/catalog/product", func(c fiber.Ctx) error {
		authHeader := c.Get("Authorization")
		if len(authHeader) < 8 || authHeader[:7] != "Bearer " {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Missing token"})
		}
		tokenStr := authHeader[7:]

		claims := &Claims{}
		token, err := jwt.ParseWithClaims(tokenStr, claims, func(token *jwt.Token) (interface{}, error) {
			return jwtSecret, nil
		})
		if err != nil || !token.Valid {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Invalid token"})
		}

		type ProductForm struct {
			ID              string `json:"product_id"`
			Title           string `json:"title"`
			JsonbAttributes string `json:"jsonb_attributes"`
		}
		var form ProductForm
		if err := c.Bind().JSON(&form); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Bad request body"})
		}

		// 🌟 ИСПРАВЛЕНИЕ: Передаем c.UserContext() для сквозной трассировки записи в Python
		grpcCtx, cancel := context.WithTimeout(c.UserContext(), 5*time.Second)
		defer cancel()

		resp, err := grpcClient.AddProduct(grpcCtx, &pb.AddProductRequest{
			ProductId:       form.ID,
			Title:           form.Title,
			OwnerId:         claims.UserID, 
			JsonbAttributes: form.JsonbAttributes,
		})
		if err != nil || !resp.Success {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": "gRPC write operation failed"})
		}

		return c.Status(fiber.StatusCreated).JSON(fiber.Map{
			"status":     "product_add_command_dispatched",
			"product_id": form.ID,
		})
	}) 

	// Запуск HTTP сервера
	log.Fatal(app.Listen(":8081"))
}
