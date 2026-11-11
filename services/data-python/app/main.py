import asyncio
import json
import logging
import os
import sys
import time
from concurrent import futures
import psycopg2
from psycopg2.extras import RealDictCursor, Json
import grpc

# 🌟 ИСПРАВЛЕНИЕ: Добавляем текущую директорию и подпапку proto в пути поиска модулей Python
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)
sys.path.append(os.path.join(current_dir, "proto"))

# 🌟 ИСПРАВЛЕНИЕ: Импортируем модули, сгенерированные из platform.proto
import platform_pb2 as pb2
import platform_pb2_grpc as pb2_grpc

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("data-python-service")

# 🌟 ИСПРАВЛЕНИЕ: Переключили дефолтную базу на нативную 'yugabyte' (базы catalog_db нет при холодном старте СУБД)
DB_URL = os.environ.get("DATABASE_URL", "postgresql://yugabyte:yugabyte@yugabytedb:5433/yugabyte")

def db_insert_product(product_id, title, owner_id, attributes):
    """Синхронная запись в YugabyteDB, изолированная в потоке."""
    conn = psycopg2.connect(DB_URL)
    conn.autocommit = True
    try:
        if isinstance(attributes, str):
            try:
                attributes = json.loads(attributes)
            except Exception:
                pass

        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO products (id, title, owner_id, jsonb_attributes, created_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (id) DO UPDATE 
                SET title = EXCLUDED.title, jsonb_attributes = EXCLUDED.jsonb_attributes;
                """,
                (product_id, title, owner_id, Json(attributes))
            )
        return True
    except Exception as e:
        logger.error(f"Database insert error: {e}")
        return False
    finally:
        conn.close()

def db_select_product(product_id):
    """Синхронное чтение из YugabyteDB, изолированное в потоке."""
    conn = psycopg2.connect(DB_URL)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, title, owner_id, jsonb_attributes FROM products WHERE id = %s LIMIT 1", (product_id,))
            return cur.fetchone()
    except Exception as e:
        logger.error(f"Database select error: {e}")
        return None
    finally:
        conn.close()

class CatalogService(pb2_grpc.CatalogServiceServicer):
    async def GetProductDetails(self, request, context):
        logger.info(f"gRPC Read Request for product: {request.product_id}")
        row = await asyncio.to_thread(db_select_product, request.product_id)
        
        if not row:
            await context.abort(grpc.StatusCode.NOT_FOUND, "Product not found")
            return

        return pb2.ProductResponse(
            product_id=str(row["id"]),
            title=row["title"],
            jsonb_attributes=json.dumps(row["jsonb_attributes"]) if isinstance(row["jsonb_attributes"], (dict, list)) else str(row["jsonb_attributes"])
        )

    async def AddProduct(self, request, context):
        logger.info(f"gRPC Write Command: Dispatched item {request.product_id}")
        success = await asyncio.to_thread(
            db_insert_product, 
            request.product_id, 
            request.title, 
            request.owner_id, 
            request.jsonb_attributes
        )
        return pb2.AddProductResponse(success=success)

def bootstrap_db():
    """Синхронный запуск схемы с циклом ожидания доступности базы данных."""
    logger.info("Проверка доступности СУБД для наката схемы...")
    
    for i in range(15):
        try:
            conn = psycopg2.connect(DB_URL)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS products (
                        id VARCHAR(100) PRIMARY KEY,
                        title VARCHAR(255) NOT NULL,
                        owner_id VARCHAR(100) NOT NULL,
                        jsonb_attributes JSONB NOT NULL,
                        created_at TIMESTAMP NOT NULL
                    );
                """)
            conn.close()
            logger.info("Схема распределенной таблицы 'products' успешно верифицирована.")
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
