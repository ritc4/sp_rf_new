FROM node:22-alpine

WORKDIR /app

# Системные утилиты для нативных модулей Node.js
RUN apk add --no-cache libc6-compat git python3 make g++

# Устанавливаем Buf CLI и локальный генератор TypeScript-кода через npm
RUN npm install -g @bufbuild/buf @bufbuild/protoc-gen-es

# Копируем манифесты зависимостей фронтенда
COPY apps/web-qwik/package*.json ./

# Устанавливаем пакеты
RUN npm install

# Копируем общие контракты Protobuf из монорепозитория
COPY packages/proto/ /packages/proto/

# Копируем исходный код Qwik
COPY apps/web-qwik/ .

# Создаем зеркало пути /apps/web-qwik -> /app, чтобы относительный путь ../../apps/web-qwik/src/gen из buf.gen.yaml попал точно в /app/src/gen
RUN mkdir -p /apps && ln -s /app /apps/web-qwik && \
    cd /packages/proto && buf generate && \
    ls -la /app/src/gen

# Собираем клиентский и SSR бандлы на этапе сборки образа
RUN npx vite build && npx vite build --ssr src/entry.preview.tsx

EXPOSE 3000

# Запускаем готовый сервер
CMD ["npx", "vite", "preview", "--host", "0.0.0.0", "--port", "3000"]
