FROM node:20-bookworm-slim

WORKDIR /app

# Пакеты, чтобы нормально ставились зависимости и работал playwright install-deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json* ./
RUN npm ci

# Устанавливаем системные зависимости для Chromium + сам Chromium
RUN npx playwright install-deps chromium
RUN npx playwright install chromium

COPY . .

ENV PORT=3000
EXPOSE 3000

CMD ["node", "server.js"]
