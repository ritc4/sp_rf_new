# Этап 1: Сборка и компиляция Protobuf и Go
FROM golang:1.26-alpine AS builder
WORKDIR /build

# Включаем прокси для бесперебойной загрузки
ENV GOPROXY=https://goproxy.io,direct


RUN apk add --no-cache protobuf-dev protoc git

# Скачиваем плагины генерации кода Go
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.33.0 && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

# 🌟 РЕШЕНИЕ: Скопируем структуру так, чтобы контекст внутри контейнера был ПЛОСКИМ
COPY services/core-go/go.mod ./
COPY services/core-go/go.sum* ./

# Копируем исходный код бэкенда Go и контракты
COPY services/core-go/cmd/ ./cmd/
COPY services/proto/ ./proto/

# Генерируем контракты в корень сборки (теперь префикс совпадает идеально)
RUN protoc --plugin=protoc-gen-go=$(go env GOPATH)/bin/protoc-gen-go \
           --plugin=protoc-gen-go-grpc=$(go env GOPATH)/bin/protoc-gen-go-grpc \
           --go_out=. --go_opt=module=joint-purchases/core-go \
           --go-grpc_out=. --go-grpc_opt=module=joint-purchases/core-go \
           -I. ./proto/platform.proto

# 🌟 Накатываем и фиксируем все зависимости прямо внутри контейнера
RUN go mod tidy

# Собираем статичный бинарник из плоской структуры
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o core-engine ./cmd/main.go

# Этап 2: Минимальный Продакшен образ
FROM alpine:3.19
WORKDIR /app
RUN apk --no-cache add ca-certificates tzdata
COPY --from=builder /build/core-engine .
EXPOSE 8081
CMD ["./core-engine"]
