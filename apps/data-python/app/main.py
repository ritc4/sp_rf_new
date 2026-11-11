import asyncio
import json
import logging
import os
import sys
import time
import uuid
import psycopg2
from psycopg2.extras import RealDictCursor, Json
import grpc
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.grpc import GrpcAioInstrumentorServer

# Добавляем текущую директорию и подпапку proto в пути поиска модулей Python
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.join(current_dir, "proto"))

# Настройка OpenTelemetry для Python gRPC
resource = Resource.create({"service.name": "data-python-service"})
provider = TracerProvider(resource=resource)
otlp_exporter = OTLPSpanExporter(insecure=True)
provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
trace.set_tracer_provider(provider)

# Автоматический перехват всех входящих асинхронных gRPC вызовов
GrpcAioInstrumentorServer().instrument()

# Импортируем модули, сгенерированные из platform.proto
import platform_pb2 as pb2
import platform_pb2_grpc as pb2_grpc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("data-python-service")

DB_URL = os.environ.get("DATABASE_URL", "postgresql://yugabyte:yugabyte@yugabytedb:5433/catalog_db")


def db_insert_product(product_id, title, owner_id, attributes):
    """Атомарная транзакционная запись товара и события Transactional Outbox в YugabyteDB."""
    conn = psycopg2.connect(DB_URL)
    # Отключаем autocommit для гарантии атомарности ACID между таблицами products и outbox_events
    conn.autocommit = False
    try:
        parsed_attrs = attributes
        if isinstance(attributes, str):
            try:
                parsed_attrs = json.loads(attributes)
            except Exception:
                parsed_attrs = {"raw": attributes}

        event_id = str(uuid.uuid4())
        outbox_payload = {
            "event_type": "PRODUCT_UPSERTED",
            "product_id": str(product_id),
            "title": title,
            "owner_id": owner_id,
            "attributes": parsed_attrs,
        }

        # Блок 'with conn:' в psycopg2 автоматически делает COMMIT при успехе или ROLLBACK при ошибке
        with conn:
            with conn.cursor() as cur:
                # 1. Сохраняем или обновляем товар в основной таблице
                cur.execute(
                    """
                    INSERT INTO products (id, title, owner_id, jsonb_attributes, created_at)
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT (id) DO UPDATE 
                    SET title = EXCLUDED.title, jsonb_attributes = EXCLUDED.jsonb_attributes;
                    """,
                    (product_id, title, owner_id, Json(parsed_attrs)),
                )

                # 2. В той же транзакции пишем событие в Transactional Outbox для Redpanda Connect -> Centrifugo
                cur.execute(
                    """
                    INSERT INTO outbox_events (id, channel, payload, created_at)
                    VALUES (%s, %s, %s, NOW());
                    """,
                    (event_id, "live-updates:products", Json(outbox_payload)),
                )
        return True
    except Exception as e:
        logger.error(f"Database transactional insert error: {e}")
        return False
    finally:
        conn.close()


def db_select_product(product_id):
    """Синхронное чтение из YugabyteDB, изолированное в потоке."""
    conn = psycopg2.connect(DB_URL)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT id, title, owner_id, jsonb_attributes FROM products WHERE id = %s LIMIT 1",
                (product_id,),
            )
            return cur.fetchone()
    except Exception as e:
        logger.error(f"Database select error: {e}")
        return None
    finally:
        conn.close()


class CatalogService(pb2_grpc.CatalogServiceServicer):
    # Имя метода строго соответствует 'rpc GetProduct' из platform.proto
    async def GetProduct(self, request, context):
        logger.info(f"gRPC Read Request for product: {request.product_id}")
        row = await asyncio.to_thread(db_select_product, request.product_id)

        if not row:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Product not found")
            return

        attrs_str = (
            json.dumps(row["jsonb_attributes"], ensure_ascii=False)
            if isinstance(row["jsonb_attributes"], (dict, list))
            else str(row["jsonb_attributes"])
        )

        return pb2.ProductResponse(
            product_id=str(row["id"]),
            title=row["title"],
            jsonb_attributes=attrs_str,
            source="yugabytedb",
        )

    async def AddProduct(self, request, context):
        logger.info(f"gRPC Write Command: Dispatched item {request.product_id}")
        success = await asyncio.to_thread(
            db_insert_product,
            request.product_id,
            request.title,
            request.owner_id,
            request.jsonb_attributes,
        )

        if not success:
            await context.abort(grpc.StatusCode.INTERNAL, "Failed to persist product and outbox event")
            return

        # Поля ответа строго соответствуют AddProductResponse в platform.proto
        return pb2.AddProductResponse(
            status="created",
            product_id=str(request.product_id),
        )


def ensure_database_exists():
    """Создает базу catalog_db при холодном старте YugabyteDB, если она еще не существует."""
    base_url = DB_URL.rsplit("/", 1)[0] + "/yugabyte"
    target_db = DB_URL.rsplit("/", 1)[1].split("?")[0]
    conn = psycopg2.connect(base_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
            if not cur.fetchone():
                cur.execute(f"CREATE DATABASE {target_db};")
                logger.info(f"Создана база данных '{target_db}' в YugabyteDB.")
    finally:
        conn.close()


def bootstrap_db():
    """Синхронный запуск схемы с циклом ожидания доступности базы данных."""
    logger.info("Проверка доступности СУБД для наката схемы...")

    for i in range(15):
        try:
            ensure_database_exists()
            conn = psycopg2.connect(DB_URL)
            conn.autocommit = True
            with conn.cursor() as cur:
                # 1. Основная таблица каталога товаров
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS products (
                        id VARCHAR(100) PRIMARY KEY,
                        title VARCHAR(255) NOT NULL,
                        owner_id VARCHAR(100) NOT NULL,
                        jsonb_attributes JSONB NOT NULL,
                        created_at TIMESTAMP NOT NULL
                    );
                    """
                )
                # 2. Таблица Transactional Outbox для CDC-стриминга в Centrifugo
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS outbox_events (
                        id VARCHAR(64) PRIMARY KEY,
                        channel VARCHAR(128) NOT NULL,
                        payload JSONB NOT NULL,
                        created_at TIMESTAMP NOT NULL
                    );
                    """
                )
            conn.close()
            logger.info("Схемы таблиц 'products' и 'outbox_events' успешно верифицированы.")
            return
        except Exception as e:
            logger.warning(f"База данных пока недоступна (Попытка {i+1}/15): {e}")
            time.sleep(4)

    logger.critical("Не удалось накатить схему данных: превышено время ожидания YugabyteDB.")
    sys.exit(1)


async def serve():
    bootstrap_db()
    server = grpc.aio.server()
    pb2_grpc.add_CatalogServiceServicer_to_server(CatalogService(), server)
    server.add_insecure_port("[::]:50051")
    logger.info("🚀 Чистый асинхронный gRPC Python сервер запущен на порту :50051")
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    asyncio.run(serve())
