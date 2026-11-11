package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "github.com/jackc/pgx/v5/stdlib"

	pb "joint-purchases/core-go/proto"
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
	jwtSecret   = []byte(os.Getenv("JWT_SECRET"))
	db          *sql.DB 
	rdb         *redis.Client
	grpcClient  pb.CatalogServiceClient
	ctxBg       = context.Background()
)

// 🌟 ИСПРАВЛЕНИЕ: Добавлена реализация генерации пары токенов с фиксацией Refresh в Dragonfly
func generateTokenPair(userID string) (string, string, error) {
	// Access Token (Жизнь 15 минут)
	accessClaims := &Claims{
		UserID: userID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(15 * time.Minute)),
		},
	}
	accessToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256, accessClaims).SignedString(jwtSecret)
	if err != nil {
		return "", "", err
	}

	// Криптографически стойкий случайный Refresh-токен взамен JWT (Best Practice для Dragonfly)
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", "", err
	}
	refreshToken := hex.EncodeToString(b)

	// Запись в Dragonfly с TTL 30 дней
	redisKey := fmt.Sprintf("refresh:%s", refreshToken)
	err = rdb.Set(ctxBg, redisKey, userID, 30*24*time.Hour).Err()
	if err != nil {
		return "", "", err
	}

	return accessToken, refreshToken, nil
}

func main() {
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

	// 🌟 ИСПРАВЛЕНИЕ: Используем актуальный конструктор NewClient согласно google.golang.org/grpc
	grpcConn, err := grpc.NewClient("data-python:50051", grpc.WithTransportCredentials(insecure.NewCredentials())) //
	if err != nil {
		log.Fatalf("Ошибка gRPC канала к data-python: %v", err)
	}
	// 🌟 КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ: Убрали defer grpcConn.Close() из корня main, чтобы не закрывать соединение мгновенно!
	grpcClient = pb.NewCatalogServiceClient(grpcConn)

	app := fiber.New(fiber.Config{
		ErrorHandler: func(ctx fiber.Ctx, err error) error {
			return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		},
	})

	app.Post("/auth/register", func(c fiber.Ctx) error {
		var req AuthRequest
		if err := c.Bind().Body(&req); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Bad request body"})
		}

		hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), 12) //
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

		if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.Password)); err != nil { //
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
		if err == redis.Nil {
			return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{"error": "Session expired or revoked"})
		}

		rdb.Del(ctxBg, redisKey)

		newAccessToken, newRefreshToken, err := generateTokenPair(storedUser)
		if err != nil {
			return err
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

	// 🌟 CQRS ПОТОК ЧТЕНИЯ: Получение товара из оперативной памяти Dragonfly
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
		
		// Проверяем наличие слепка товара в Dragonfly
		cachedProduct, err := rdb.Get(ctxBg, cacheKey).Result()
		if err == nil {
			c.Set("X-Cache", "HIT")
			c.Set("Content-Type", "application/json") // Явно указываем тип данных для кэша
			return c.Status(fiber.StatusOK).SendString(cachedProduct)
		}

		// Подстраховочный gRPC-вызов к Python при Cache MISS
		grpcCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, err := grpcClient.GetProductDetails(grpcCtx, &pb.ProductRequest{ProductId: id})
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}

		productMap := fiber.Map{
			"product_id":       resp.ProductId,
			"title":            resp.Title,
			"jsonb_attributes": resp.JsonbAttributes,
		}

		// 🌟 ИСПРАВЛЕНИЕ: Безопасный дефолтный маршалинг JSON для структуры Fiber v3
		actualJSON, err := json.Marshal(productMap)
		if err == nil {
			_ = rdb.Set(ctxBg, cacheKey, actualJSON, 1*time.Hour).Err()
		}

		c.Set("X-Cache", "MISS")
		return c.Status(fiber.StatusOK).JSON(productMap)
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
		// 🌟 ИСПРАВЛЕНИЕ ДЛЯ FIBER V3: Использование правильного синтаксиса нового биндинга
		if err := c.Bind().JSON(&form); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "Bad request body"})
		}

		grpcCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Передаем команду в Python, неявно закрепляя за owner_id из JWT claims.UserID
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