module joint-purchases/core-go

go 1.26

require (
	github.com/gofiber/contrib/v3/otel v1.0.0
	github.com/gofiber/fiber/v3 v3.0.0-beta.3
	github.com/golang-jwt/jwt/v5 v5.2.1
	github.com/jackc/pgx/v5 v5.10.0
	github.com/redis/go-redis/v9 v9.5.1
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.57.0
	go.opentelemetry.io/otel v1.32.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.32.0
	go.opentelemetry.io/otel/sdk v1.32.0
	golang.org/x/crypto v0.32.0
	google.golang.org/grpc v1.85.0
	google.golang.org/protobuf v1.36.12
)
