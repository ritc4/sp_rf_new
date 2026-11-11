FROM golang:1.26-alpine

RUN apk add --no-cache protobuf-dev protoc git

WORKDIR /app

# 🌟 Сначала копируем только файлы модулей для кэширования слоев Docker
COPY apps/cache-worker/go.mod apps/cache-worker/go.sum ./

# Скачиваем зависимости (этот слой закэшируется и будет выполняться мгновенно)
RUN go mod download

# Копируем остальной исходный код (main.go)
COPY apps/cache-worker/ .

# 🌟 Накатываем и фиксируем все зависимости прямо внутри контейнера
RUN go mod tidy

# Собираем оптимизированный бинарник без отладочной информации
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o worker main.go

CMD ["./worker"]
