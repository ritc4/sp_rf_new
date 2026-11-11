# Этап 1: Сборка и компиляция Protobuf, gRPC, Connect-RPC и Go
FROM golang:1.26-alpine AS builder
WORKDIR /build

# Настраиваем прокси и явный путь для установки бинарников Go
ENV GOPROXY=https://goproxy.io,direct
ENV GOBIN=/go/bin
ENV PATH="$PATH:/go/bin"

RUN apk add --no-cache protobuf-dev protoc git

# Скачиваем плагины генерации кода Go (Protobuf, gRPC и Connect-RPC)
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest && \
    go install connectrpc.com/connect/cmd/protoc-gen-connect-go@latest

# Копируем манифесты зависимостей (звездочка защищает от ошибки, если go.sum еще не создан)
COPY apps/core-go/go.mod ./
COPY apps/core-go/go.sum* ./

# Копируем исходный код Go и общие контракты из packages/proto
COPY apps/core-go/cmd/ ./cmd/
COPY packages/proto/ ./proto/

# 1. Сначала генерируем Go-код из .proto (pb, grpc и connect)
RUN protoc --plugin=protoc-gen-go=/go/bin/protoc-gen-go \
           --plugin=protoc-gen-go-grpc=/go/bin/protoc-gen-go-grpc \
           --plugin=protoc-gen-connect-go=/go/bin/protoc-gen-connect-go \
           --go_out=. --go_opt=module=joint-purchases/core-go \
           --go-grpc_out=. --go-grpc_opt=module=joint-purchases/core-go \
           --connect-go_out=. --connect-go_opt=module=joint-purchases/core-go \
           -I. ./proto/platform.proto

# 2. Только после генерации пакетов подтягиваем и фиксируем зависимости в go.mod / go.sum
RUN go mod tidy

# 3. Собираем статичный бинарник
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o core-engine ./cmd/main.go

# Этап 2: Минимальный Продакшен-образ
FROM alpine:3.19
WORKDIR /app
RUN apk --no-cache add ca-certificates tzdata
COPY --from=builder /build/core-engine .
EXPOSE 8081 8082
CMD ["./core-engine"]
