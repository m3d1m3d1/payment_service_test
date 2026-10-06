# Payment processing service

Асинхронный микросервис обработки платежей: FastAPI + SQLAlchemy 2.0 (async) + PostgreSQL + RabbitMQ (FastStream) + Alembic.

## Архитектура

```
                 одна транзакция
Client ──POST──> API ───────────────> payments + outbox (PostgreSQL)
                  │                         │
                  │  outbox relay (в API)   │ SELECT ... FOR UPDATE SKIP LOCKED
                  └─────────────────────────┘
                                │ publish (persistent + publisher confirms)
                                v
                 exchange `payments` ──payments.new──> queue `payments.new`
                                                            │
                                                     Consumer (FastStream)
                                                            │ 1. эмуляция шлюза 2–5 c (90% / 10%)
                                                            │ 2. UPDATE статуса в БД
                                                            │ 3. webhook клиенту
                              ошибка (попытка 1, 2)         │
        payments.new.retry.1 (TTL 2c) / .retry.2 (TTL 4c) <─┤
                  └── dead-letter обратно в payments.new    │
                              ошибка (попытка 3)            │
                 exchange `payments.dlx` ──> queue `payments.dlq` <─ reject
```

## Запуск

```bash
docker compose up --build
```

Поднимаются: `postgres`, `rabbitmq` (UI: http://localhost:15672, guest/guest), `api` (http://localhost:8000, Swagger: `/docs`; миграции применяются при старте), `consumer` и вспомогательный `webhook-receiver` (http://localhost:9000) для проверки уведомлений.

API-ключ по умолчанию: `secret-api-key` (переменная `API_KEY`).

## Примеры

Создание платежа (внутри docker-сети приемник webhook доступен как `webhook-receiver`):

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H "X-API-Key: secret-api-key" \
  -H "Idempotency-Key: order-1001" \
  -H "Content-Type: application/json" \
  -d '{
    "amount": "100.50",
    "currency": "RUB",
    "description": "Order #1001",
    "metadata": {"order_id": 1001},
    "webhook_url": "http://webhook-receiver:9000/hook"
  }'
```

```
HTTP/1.1 202 Accepted
{"payment_id":"…","status":"pending","created_at":"2026-10-06T01:10:57.277804Z"}
```

Повтор запроса с тем же `Idempotency-Key` вернет тот же `payment_id` и не создаст дубль.

Получение платежа:

```bash
curl http://localhost:8000/api/v1/payments/<payment_id> -H "X-API-Key: secret-api-key"
```

Результат обработки приходит на `webhook_url` (смотрите `docker compose logs -f webhook-receiver`):

```json
{"event":"payment.processed","payment_id":"…","status":"succeeded","amount":"100.50",
 "currency":"RUB","description":"Order #1001","metadata":{"order_id":1001},
 "created_at":"…","processed_at":"…"}
```

### Проверка retry и DLQ

Укажите `"webhook_url": "http://webhook-receiver:9000/fail"` (всегда отвечает 500). В логах consumer будут 3 попытки (через 2 с и 4 с), после чего сообщение окажется в очереди `payments.dlq` (видно в RabbitMQ UI → Queues).

## Коды ответов

| Код | Когда |
|-----|-------|
| 202 | платеж принят (в т.ч. повтор с тем же Idempotency-Key) |
| 401 | нет или неверный `X-API-Key` |
| 404 | платеж не найден |
| 409 | `Idempotency-Key` уже использован с другим телом запроса |
| 422 | невалидное тело, нет заголовка `Idempotency-Key` |

## Как реализованы требования

- **Outbox.** Платеж и событие `payment.new` пишутся в одной транзакции. Relay (фоновая задача в процессе API) публикует события пачками с `FOR UPDATE SKIP LOCKED` и помечает `published_at` только после publisher confirm; unroutable-сообщения тоже считаются ошибкой публикации (`mandatory` + `on_return_raises`). Если RabbitMQ недоступен или сообщение нельзя маршрутизировать, событие остаётся в таблице для повторной попытки. Гарантия — at-least-once.
- **Идемпотентность.** API: уникальный индекс по `idempotency_key` + хэш тела запроса. Повтор с тем же ключом и тем же телом возвращает существующий платеж, с другим телом — `409`; гонка параллельных запросов обрабатывается через `IntegrityError`. Consumer: `UPDATE ... WHERE status='pending'` — платеж не обрабатывается дважды, а после повторной доставки заново выполняется только отправка webhook.
- **Семантика webhook: at-least-once.** Колонка `webhook_sent_at` убирает лишние повторы при обычной редоставке, но не гарантирует ровно одну отправку: при параллельной обработке одного сообщения двумя consumer'ами или падении процесса между ответом получателя и записью отметки webhook может уйти повторно. Поэтому каждое событие содержит стабильный `event_id` (также заголовок `X-Event-Id`) — получатель должен дедуплицировать по нему.
- **Retry.** Всего 3 попытки. Ошибка (в основном недоступный webhook, не-2xx ответ, сбой БД) → сообщение публикуется в очередь задержки `payments.new.retry.N` с TTL `base * 2^(N-1)` (2 с, 4 с), по истечении TTL возвращается в `payments.new`. Номер попытки хранится в заголовке `x-attempt`. При повторе статус платежа уже финальный, поэтому заново повторяется только отправка webhook.
- **DLQ.** После 3-й неудачи (и для битых сообщений) consumer делает `reject` без requeue → dead-letter exchange `payments.dlx` → очередь `payments.dlq`.
- **10% ошибок шлюза** — это бизнес-исход (`status = failed` + webhook), а не технический сбой, поэтому retry не запускается.

## Настройки (env)

`DATABASE_URL`, `RABBITMQ_URL`, `API_KEY`, а также `MAX_ATTEMPTS` (3), `RETRY_BASE_DELAY_MS` (2000), `PROCESSING_MIN_SECONDS` / `PROCESSING_MAX_SECONDS` (2 / 5), `PROCESSING_SUCCESS_RATE` (0.9), `OUTBOX_POLL_INTERVAL` (1), `WEBHOOK_TIMEOUT` (5).

> Если меняете `MAX_ATTEMPTS` или `RETRY_BASE_DELAY_MS` на уже созданном брокере, очереди задержки нужно пересоздать (RabbitMQ не позволяет менять аргументы очереди): `docker compose down -v`.

## Локальный запуск без Docker

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env            # Postgres и RabbitMQ должны быть запущены
alembic upgrade head
uvicorn app.main:app --reload   # API + outbox relay
faststream run app.consumer:app # consumer (в другом терминале)
python tools/webhook_receiver.py
```

## Тесты

Нужен PostgreSQL (используется отдельная БД, её таблицы пересоздаются). RabbitMQ не нужен — брокер подменяется.

```bash
createdb payments_test
pip install -r requirements-dev.txt
TEST_DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments_test pytest
```

Или через docker compose:

```bash
docker compose up -d postgres
docker compose exec postgres createdb -U payments payments_test
docker compose run --rm -e TEST_DATABASE_URL=postgresql+asyncpg://payments:payments@postgres:5432/payments_test api \
  sh -c "pip install -q -r requirements-dev.txt && pytest"
```

Покрыто: атомарность платеж + outbox, идемпотентность (повтор, конфликт тела, параллельные запросы), авторизация и валидация, публикация outbox и сохранение события при недоступном брокере, обработка платежа (успех, отказ шлюза, ошибка webhook и повторная доставка без повторной эмуляции), retry/DLQ в consumer.

## Структура

```
app/main.py        API, lifespan с outbox relay
app/outbox.py      outbox relay
app/messaging.py   топология RabbitMQ (exchange, очереди, retry, DLQ)
app/consumer.py    FastStream consumer, retry/DLQ
app/processing.py  эмуляция шлюза, обновление статуса, webhook
app/models.py      SQLAlchemy модели (payments, outbox)
alembic/           миграции
tools/             приемник webhook для проверки
tests/             pytest-тесты
```

## Ограничения и что можно улучшить

- **SSRF.** `webhook_url` проверяется только на формат http(s) и длину. В реальном окружении нужен allowlist/блокировка внутренних адресов и защита от редиректов и DNS-подмены; здесь это сознательно не сделано, т.к. тестовый сценарий использует localhost и docker-сеть.
- Для DLQ и outbox не описаны политики очистки и повторного запуска (redrive); сообщение о несуществующем платеже сразу попадает в DLQ без retry.
- `API_KEY` по умолчанию — dev-значение; вне разработки секрет нужно задавать явно.
- Для строгой «ровно одной» отправки webhook нужен claim в БД перед отправкой; сейчас гарантия at-least-once + `event_id`.
- Outbox relay можно вынести в отдельный процесс; добавить метрики (глубина очередей/outbox, retry, DLQ), подпись webhook (HMAC), интеграционные тесты с реальным RabbitMQ.
