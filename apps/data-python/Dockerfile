FROM python:3.11-slim

WORKDIR /app

# Оптимизация кэша: Ставим системные зависимости (build-essential нужен для компиляции grpcio/psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 1. Зависимости (копируем и ставим отдельно, чтобы не пересобирать их при изменении кода)
COPY apps/data-python/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Копируем исходный код микросервиса и gRPC контракты
COPY apps/data-python/app/ .
COPY packages/proto/ ./proto/

# 🌟 ГЕНЕРАЦИЯ: Файлы platform_pb2.py создаются прямо в корне /app/
RUN python -m grpc_tools.protoc -Iproto --python_out=. --grpc_python_out=. proto/platform.proto

# 🌟 ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ: Запрещаем Python писать файлы кэша .pyc на диск (экономит I/O в WSL2)
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

EXPOSE 50051

CMD ["python", "main.py"]
