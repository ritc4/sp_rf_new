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

type ProductModel struct {
	ID              string          `json:"id"`
	Title           string          `json:"title"`
	OwnerID         string          `json:"owner_id"`
	JsonbAttributes json.RawMessage `json:"jsonb_attributes"`
}

type SmartCDCPayload struct {
	Payload struct {
		After ProductModel `json:"after"`
	} `json:"payload"`
	ProductModel
}

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: os.Getenv("DRAGONFLY_URL"),
	})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("Воркер не смог подключиться к Dragonfly: %v", err)
	}

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

	log.Println("🚀 CDC Кэш-Воркер на franz-go запущен. Обновляю кэш Dragonfly из Redpanda...")

	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			log.Printf("Ошибка получения данных из Redpanda: %v", errs)
			continue
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()

			var event SmartCDCPayload
			if err := json.Unmarshal(record.Value, &event); err != nil {
				log.Printf("Ошибка декодирования CDC JSON: %v", err)
				continue
			}

			var targetProduct ProductModel
			if event.Payload.After.ID != "" {
				targetProduct = event.Payload.After
			} else {
				targetProduct = event.ProductModel
			}

			if targetProduct.ID == "" {
				continue
			}

			// Формируем структуру для кэша (синхронизировано с core-go)
			cachePayload := map[string]interface{}{
				"product_id":       targetProduct.ID,
				"title":            targetProduct.Title,
				"owner_id":         targetProduct.OwnerID,
				"jsonb_attributes": string(targetProduct.JsonbAttributes),
			}

			cachedData, err := json.Marshal(cachePayload)
			if err != nil {
				log.Printf("Ошибка маршаллинга структуры товара: %v", err)
				continue
			}

			// Обновляем кэш в Dragonfly
			cacheKey := fmt.Sprintf("catalog:product:%s", targetProduct.ID)
			if err := rdb.Set(ctx, cacheKey, cachedData, 24*time.Hour).Err(); err != nil {
				log.Printf("Ошибка обновления кэша в Dragonfly: %v", err)
			} else {
				log.Printf("⚡ CDC [franz-go]: Кэш для товара %s успешно обновлен в Dragonfly!", targetProduct.ID)
			}
		}
	}
}
