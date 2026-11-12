# Архитектура платформы (Enterprise Monorepo 2026)

Высоконагруженная полиглотная распределенная платформа (уровня FinTech / E-Commerce 2026), построенная на принципах **Zero-Trust Security**, **CQRS**, **Transactional Outbox**, **CDC-стриминга** и сквозной наблюдаемости (**OpenTelemetry**).

---

## 1. Структура монорепозитория

```text
sp_rf_new/
├── apps/                        # Исходный код приложений и микросервисов
│   ├── core-go/                 # Go 1.26: BFF-шлюз, REST Auth (Fiber v3) + Connect-RPC (h2c)
│   ├── data-python/             # Python 3.11: изолированный gRPC-домен каталога и Transactional Outbox
│   └── web-qwik/                # Qwik City (Node 22): SSR/SPA фронтенд, Connect-Web + Centrifugo SSE
├── infra/                       # Конфигурации инфраструктурных сервисов
│   ├── alloy/                   # Grafana Alloy v1.5.1: сбор логов всех Docker-контейнеров в Loki
│   ├── centrifugo/              # Centrifugo v5.4.9: конфигурация Real-Time Push (SSE/WS по JWT)
│   ├── krakend/                 # KrakenD 2.9.0: маршрутизация, Rate Limit, Stateless JWT (jwk.json)
│   └── redpanda-connect/        # Redpanda Connect 4.42.0: CDC-пайплайны (WAL -> Dragonfly / Centrifugo / Redpanda)
├── packages/                    # Общие контракты и схемы
│   └── proto/                   # Единый Protobuf-контракт (platform.proto) и buf.gen.yaml
├── docker-compose.yml           # Оркестрация всех 11 сервисов в изолированной сети platform-network
└── README.md              # Архитектурная документация проекта
```

---

## 2. Технологический стек

| Слой | Технология и версия | Назначение в системе |
| :--- | :--- | :--- |
| **Frontend** | Qwik City (Node 22) | Клиентский и SSR-интерфейс, бинарный Connect-Web клиент, подписка на SSE |
| **API Gateway** | KrakenD `2.9.0` | Единая точка входа (`:8080`), Rate Limiting, CORS, Stateless проверка JWT (`jwk.json`), проброс `X-User-ID` и `traceparent` |
| **BFF & Auth Core** | Go `1.26` (Fiber v3 + Connect-RPC) | Единственный фасад бэкенда (`:8081` REST Auth, `:8082` Connect-RPC `h2c`), `Singleflight`, чтение кэша, выпуск токенов |
| **Domain Service** | Python `3.11` (`grpc.aio`) | Изолированный доменный сервис (`:50051` без публикации наружу), владелец БД `catalog_db`, сборщик мусора Outbox |
| **Database** | YugabyteDB `2024.2.1.0-b185` | Распределенная NewSQL (PostgreSQL-совместимая) СУБД (`:5433`). Базы: `yugabyte` (users) и `catalog_db` (products, outbox) |
| **In-Memory Cache** | Dragonfly `v1.26.1` | Сверхбыстрый Redis-совместимый кэш товаров (`catalog:product:<id>`), хранилище сессий и Pub/Sub движок для Centrifugo |
| **CDC & Streaming** | Redpanda `v24.2.7` + Redpanda Connect `4.42.0` | Чтение WAL-лога YugabyteDB, прогрев кэша в Dragonfly, отправка HTTP-пушей в Centrifugo и архивация событий в Kafka-топики |
| **Real-Time Push** | Centrifugo `v5.4.9` | Выделенный сервер доставки событий в браузер (`:8000`) по одностороннему **SSE** (с фолбэком на WebSocket) и JWT-авторизацией |
| **Observability** | Grafana LGTM `0.8.2` + Grafana Alloy `v1.5.1` | Сквозной трейсинг OpenTelemetry (`:4317`), сбор логов контейнеров, метрики и единый дашборд (`:3001`) |

---

## 3. Схема потоков данных (End-to-End Flows)

```text
[ Браузер: Qwik (:3000) ]
   │  ├── 1. REST + HttpOnly Cookie (/api/auth/*)
   │  ├── 2. Binary Connect-RPC + Bearer JWT (/rpc/platform.CatalogService/*)
   │  └── 3. SSE / WebSocket + Centrifugo JWT (http://:8000/connection/sse)
   ▼
[ API Gateway: KrakenD 2.9 (:8080) ] ── Проверка JWT (jwk.json), Rate Limit, проброс X-User-ID
   │
   ▼
[ BFF & Auth Hub: core-go (:8081 REST / :8082 Connect-RPC) ]
   ├── Владеет БД пользователей (YugabyteDB: yugabyte) и сессиями (Dragonfly)
   ├── Выдает 2 токена при входе: Access JWT + Centrifugo WS/SSE JWT
   ├── Чтение каталога (CQRS Read): Dragonfly (<1 мс) ──(если MISS + Singleflight)──┐
   └── Запись каталога (CQRS Write): извлекает X-User-ID ──────────────────────────┼──┐
                                                                                   ▼  ▼
                                                           [ Домен данных: data-python (:50051, закрыт) ]
                                                              ├── Владеет БД каталога (catalog_db)
                                                              ├── Читает товары из YugabyteDB
                                                              ├── Пишет товар + событие Outbox в 1 ACID-транзакции
                                                              └── Раз в час удаляет старые записи из outbox_events
                                                                                   │
                                                                                   ▼
                                                           [ Распределенная NewSQL БД: YugabyteDB (:5433) ]
                                                                                   │ (Логический WAL-поток)
                                                                                   ▼
                                                           [ CDC-конвейер: Redpanda Connect ]
                                                              ├── 1. Прямой SET в Dragonfly (прогрев кэша)
                                                              ├── 2. HTTP Push в Centrifugo (мгновенный SSE)
                                                              └── 3. Запись в Redpanda (Kafka-архив для аналитики)
```

### Поток 1: Аутентификация и Zero-Trust сессия
1. Пользователь отправляет `POST /api/auth/login` (или `/register`, `/refresh`, `/logout`) с `credentials: 'include'`.
2. **KrakenD** маршрутизирует запрос в **core-go (`:8081`)**.
3. **core-go** проверяет пароль (`bcrypt`), генерирует случайный `refresh_token`, сохраняет его в **Dragonfly** (`refresh:<token> -> user_id`, TTL 30 дней) и устанавливает в браузер `HttpOnly; SameSite=Strict` куку.
4. В JSON-ответе `core-go` возвращает два подписанных JWT:
   * `access_token` (TTL 15 минут, `kid: core-go-hs256-key`) — хранится только в оперативной памяти браузера.
   * `centrifugo_token` (TTL 24 часа, `sub: user_id`, подписан `CENTRIFUGO_SECRET`) — для авторизации в Centrifugo.
5. При истечении `access_token` интерцептор Connect-RPC или `getToken` в Centrifugo прозрачно вызывает `POST /api/auth/refresh` с кукой, ротирует Refresh-токен в Dragonfly и обновляет оба JWT.

### Поток 2: Запись данных (Mutation + Transactional Outbox)
1. Клиент вызывает бинарный метод `CatalogService.AddProduct` через Connect-RPC (`POST /rpc/platform.CatalogService/AddProduct`) с заголовком `Authorization: Bearer <access_token>`.
2. **KrakenD** проверяет подпись JWT по `jwk.json`, записывает `user_id` в заголовок `X-User-ID` и в режиме `encoding: no-op` (без перекодирования Protobuf) проксирует бинарный поток в **core-go (`:8082`)**.
3. **core-go** извлекает владельца строго из `X-User-ID` (защита от подмены `owner_id`) и вызывает по внутреннему gRPC сервис **data-python (`:50051`)**.
4. **data-python** открывает единую ACID-транзакцию в **YugabyteDB (`catalog_db`)**:
   * Сохраняет или обновляет товар в таблице `public.products`.
   * В этой же транзакции записывает JSON-событие в таблицу `public.outbox_events` (`channel: "live-updates:products"`).
   * Фоновая задача в `data-python` раз в час очищает записи из `outbox_events` старше 24 часов.

### Поток 3: Реактивный CDC-конвейер (Cache Warm-up + Live SSE Push)
1. **Redpanda Connect** считывает логический WAL-поток базы `catalog_db` (`public.products` и `public.outbox_events`) и разветвляет поток (`broker -> fan_out`) на 3 направления:
   * **В Dragonfly:** При изменении `public.products` выполняет `SET catalog:product:<id> <json> EX 86400` (проактивный прогрев/инвалидация кэша без лишних воркеров).
   * **В Centrifugo:** При вставке в `public.outbox_events` отправляет HTTP POST в `http://centrifugo:8000/api` (`method: "publish"`) с заголовком `X-API-Key`.
   * **В Redpanda:** Дублирует события `products` в топик `yugabyte.catalog_db.public.products` для аналитики и будущих потребителей.
2. **Centrifugo** доставляет событие авторизованным клиентам Qwik по одностороннему **SSE** (`http://localhost:8000/connection/sse` + `/emulation`).

### Поток 4: Сверхбыстрое чтение (Query + Cache Stampede Protection)
1. Клиент вызывает бинарный `CatalogService.GetProduct` через `KrakenD -> core-go (:8082)`.
2. **core-go** проверяет ключ `catalog:product:<id>` в **Dragonfly**. При попадании (`X-Cache: HIT`) возвращает ответ за <1 мс с пометкой `source: "dragonfly-cache"`.
3. При промахе (`X-Cache: MISS`) срабатывает **`singleflight.Group`** в Go: тысячи одновременных запросов одного товара объединяются в **1 gRPC-вызов** в `data-python -> YugabyteDB`, остальные ожидают результат в памяти, после чего кэш в Dragonfly прогревается.

---

## 4. Правила масштабирования и разработки
1. **Изоляция сервисов:** Новые доменные микросервисы разворачиваются исключительно внутри `platform-network` без публикации портов наружу; внешний трафик принимает только `KrakenD -> core-go` (BFF).
2. **Contract-First:** Любые новые API-методы сначала описываются в `packages/proto/platform.proto`, после чего генерируются клиенты и серверы для Go, Python и TypeScript.
3. **Сквозная телеметрия:** Все межсервисные вызовы обязаны пробрасывать контекст OpenTelemetry (`traceparent`).

---

## 5. Быстрый старт и порты

```bash
# Сборка и запуск всех 11 контейнеров
docker compose up --build -d

# Проверка логов ключевых сервисов
docker compose logs -f core-go data-python redpanda-connect
```

* **Frontend (Qwik):** `http://localhost:3000`
* **API Gateway (KrakenD):** `http://localhost:8080`
* **Real-Time Server (Centrifugo):** `http://localhost:8000`
* **Observability UI (Grafana LGTM):** `http://localhost:3001` (логин/пароль: `admin` / `admin`)
