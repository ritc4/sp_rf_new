FROM node:22-alpine

WORKDIR /app

RUN apk add --no-cache libc6-compat git python3 make g++

# Копируем манифесты
COPY qwik-app/package*.json ./

# Чистая установка зафиксированных версий пакетов
RUN npm install

# Копируем остальной код
COPY qwik-app/ .

EXPOSE 3000

# Нативная компиляция и запуск стабильного превью-сервера
CMD ["sh", "-c", "npx vite build && npx vite build --ssr src/entry.preview.tsx && npx vite preview --host 0.0.0.0 --port 3000"]

