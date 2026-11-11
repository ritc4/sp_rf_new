FROM node:22-alpine

WORKDIR /app

# Системные утилиты для нативных модулей Node.js и скачивания Buf CLI
RUN apk add --no-cache libc6-compat git python3 make g++ curl

# Устанавливаем актуальный Buf CLI для генерации TypeScript-клиента из .proto
RUN BIN="/usr/local/bin" && \
    VERSION="1.47.2" && \
    curl -sSL "https://github.com{VERSION}/buf-$(uname -s)-$(uname -m)" -o "${BIN}/buf" && \
    chmod +x "${BIN}/buf"

# Копируем манифесты зависимостей фронтенда
COPY apps/web-qwik/package*.json ./

# Устанавливаем пакеты
RUN npm install

# Копируем общие контракты Protobuf из монорепозитория
COPY packages/proto/ /packages/proto/

# Копируем исходный код Qwik
COPY apps/web-qwik/ .

# 1. Генерируем строго типизированный Connect-RPC клиент в src/gen
RUN cd /packages/proto && buf generate

# 2. Собираем клиентский и SSR бандлы на этапе сборки образа
RUN npx vite build && npx vite build --ssr src/entry.preview.tsx

EXPOSE 3000

# Запускаем только готовый сервер без пересборки при рестарте
CMD ["npx", "vite", "preview", "--host", "0.0.0.0", "--port", "3000"]
