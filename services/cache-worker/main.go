package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Чистая структура самого товара для витрины кэша
type ProductModel struct {
	ID              string          `json:"id"`
	Title           string          `json:"title"`
	OwnerID         string          `json:"owner_id"`
	JsonbAttributes json.RawMessage `json:"jsonb_attributes"` // Использование RawMessage предотвратит искажение JSONB
}

// Универсальная структура, поддерживающая и Debezium-обертку, и прямой плоский JSON
type SmartCDCPayload struct {
	// Для формата Debezium
	Payload struct {
		After ProductModel `json:"after"`
	} `json:"payload"`
	
	// Для прямого плоского JSON от Redpanda Connect
	ProductModel
}

func main() {
	ctx := context.Background()

	// 1. Подключаемся к Dragonfly
	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("DRAGONFLY_URL"),
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Воркер не смог подключиться к Dragonfly: %v", err)
	}

	// 2. Инициализируем высокопроизводительный клиент franz-go для Redpanda
	brokers := []string{os.Getenv("REDPANDA_BROKERS")}
	topic := "yugabyte.catalog_db.public.products"

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup("cache-worker-group"),
	)
	if err != nil {
		log.Fatalf("Не удалось запустить Redpanda клиент: %v", err)
	}
	defer cl.Close()

	log.Println("🚀 Ультрасовременный CDC Кэш-Воркер на franz-go запущен. Слушаю Redpanda...")

	// 3. Бесконечный цикл чтения пакетов событий
	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Printf("Ошибка получения данных из Redpanda: %v", errs)
			continue
		}

		// Обрабатываем полученные записи
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()

			var event SmartCDCPayload
			if err := json.Unmarshal(record.Value, &event); err != nil {
				log.Printf("Ошибка декодирования CDC JSON: %v", err)
				continue
			}

			// Умное извлечение модели: если после Debezium пусто — берем корень (Redpanda Connect)
			var targetProduct ProductModel
			if event.Payload.After.ID != "" {
				targetProduct = event.Payload.After
			} else {
				targetProduct = event.ProductModel
			}

			if targetProduct.ID == "" {
				continue // Пропускаем пустые события или операции DELETE
			}

			// Формируем строгий валидный JSON-слепок товара
			cachedData, err := json.Marshal(targetProduct)
			if err != nil {
				log.Printf("Ошибка маршаллинга структуры товара: %v", err)
				continue
			}

			// ⚡ МГНОВЕННАЯ ИНВАЛИДАЦИЯ: Записываем данные в Dragonfly
			cacheKey := fmt.Sprintf("catalog:product:%s", targetProduct.ID)
			err = rdb.Set(ctx, cacheKey, cachedData, 24*time.Hour).Err()
			if err != nil {
				log.Printf("Ошибка обновления кэша в Dragonfly: %v", err)
			} else {
				log.Printf("⚡ CDC Магия [franz-go]: Кэш для товара %s обновлен в Dragonfly напрямую из WAL-лога БД!", targetProduct.ID)
			}
		}
	}
}
