FROM node:20-bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY package.json ./
RUN npm install

RUN npx playwright install-deps chromium
RUN npx playwright install chromium

COPY . .

ENV PORT=3000
EXPOSE 3000

CMD ["node", "server.js"]
