# telemetry-platform

Событийная платформа для телеметрии промышленного оборудования: станки
стримят показания датчиков по gRPC, платформа сохраняет их, держит
read-модель в реальном времени, проверяет правила алертинга и уведомляет
людей. Набор Go-микросервисов на Kafka, Redis и TimescaleDB с полной
наблюдаемостью.

```
шлюз ──gRPC──► ingest ──Kafka──► processor ──► TimescaleDB / Redis
                                     │
                                     └──Kafka──► alerter ──► webhook / Telegram
                           query (REST + gRPC) ◄── дашборды, интеграции
```

Подробная схема и таблица отказов: [docs/architecture.md](docs/architecture.md).
Архитектурные решения: [docs/adr](docs/adr).

## Состав

| Сервис | Роль | Зависимости |
| --- | --- | --- |
| `ingest` | gRPC bidi-stream и unary API, авторизация по API-ключу, валидация, дедупликация, продюсер с `acks=all` | Kafka, Redis, Postgres |
| `processor` | Consumer group над `telemetry.raw`: идемпотентная вставка, последние значения и скользящие окна, движок правил, алерты, DLQ | Kafka, TimescaleDB, Redis |
| `alerter` | Consumer group над `telemetry.alerts`: сохранение, cooldown, уведомления (лог / webhook с HMAC / Telegram) | Kafka, Postgres, Redis |
| `query` | REST (`chi`) и gRPC для чтения, CRUD правил с мгновенной доставкой в процессоры | Postgres, Redis |
| `simulator` | Парк станков с дрейфом, шумом и инцидентами; он же генератор нагрузки | ingest |
| `migrate` | Одноразовые миграции `goose` (SQL встроен в бинарник) | Postgres |

Инфраструктура в Compose: Kafka 3.9 (KRaft), Redis 7, TimescaleDB 2.17 / PG 16,
Jaeger, Prometheus, Grafana (дашборд подключается автоматически), Kafka UI.

## Быстрый старт

Нужны Docker с Compose v2. Go 1.26 только если запускать сервисы вне контейнеров.

```bash
make up            
make ps
```

`make up-local` делает то же, но компилирует бинарники на хосте и только
копирует их в образ (быстрая итерация, сборке в Docker не нужна сеть). Если
порт 8080 занят: `QUERY_HTTP_PORT=8083 make up`.

Дальше открыть:

| URL | Что |
| --- | --- |
| http://localhost:3000 | Grafana, дашборд *Telemetry Platform* (анонимный админ) |
| http://localhost:16686 | Jaeger (`service = ingest`, проследить батч до processor) |
| http://localhost:8090 | Kafka UI (топики, consumer group'ы, лаг) |
| http://localhost:9090 | Prometheus |
| http://localhost:8080/v1/devices | Query API |

Симулятор запускает 20 станков по 5 метрик раз в секунду и подбрасывает
инциденты, так что алерты появляются в первые минуты.

### Потрогать API

```bash
# последние значения и последнее закрытое минутное окно
curl -s localhost:8080/v1/devices/cnc-001/latest | jq

# история за 5 минут по 30-секундным корзинам
curl -s "localhost:8080/v1/devices/cnc-001/history?metric=temperature&step=30" | jq

# алерты
curl -s "localhost:8080/v1/alerts?state=firing" | jq

# добавить правило; процессоры подхватят его через Redis pub/sub за секунду
curl -s -X POST localhost:8080/v1/rules -H 'content-type: application/json' -d '{
  "name": "RPM too low", "metric": "rpm", "op": "lt", "threshold": 8500,
  "for_seconds": 5, "severity": "warning", "enabled": true
}'
```

gRPC с включённым reflection, например через `grpcurl`:

```bash
grpcurl -plaintext -d '{"device_id":"cnc-001"}' localhost:8082 query.v1.QueryService/GetLatest
```

Ingest требует метаданные `x-api-key` (демо-ключ: `demo-gateway-key`):

```bash
grpcurl -plaintext -H 'x-api-key: demo-gateway-key' \
  -d '{"measurements":[{"device_id":"lathe-9","metric":"temperature","value":61.5,"seq":1,"ts":"2026-01-01T00:00:00Z"}]}' \
  localhost:8081 telemetry.v1.IngestService/PublishBatch
```

### Увидеть гарантии в деле

* **Дедупликация:** отправь один и тот же `seq` дважды, второй ack будет
  `ACK_STATUS_DUPLICATE`. Или `docker compose restart ingest` под работающим
  симулятором: он переподключится и перешлёт неподтверждённый батч; панель
  *Duplicates rejected at ingest* в Grafana вырастет, база нет.
* **At-least-once:** `docker compose stop timescaledb` на минуту. Ingest
  продолжает подтверждать (Kafka буферизует), processor логирует повторы с
  backoff, после возврата базы ничего не потеряно.
* **Масштабирование:** `docker compose up -d --scale processor=4`, в Kafka UI
  виден cooperative rebalance. Потолок равен числу партиций (6).
* **DLQ:** отправь мусор в `telemetry.raw` из Kafka UI, он окажется в
  `telemetry.dlq` с заголовками `dlq-reason` и исходным оффсетом.

## Разработка

```bash
make test               # юнит-тесты с race-детектором
make test-integration   # testcontainers: Kafka, Redis, TimescaleDB
make lint               # golangci-lint + buf lint
make proto              # перегенерировать из api/proto
make bench
```

Структура:

```
api/proto/         контракты protobuf (gRPC API и формат сообщений Kafka)
gen/go/            сгенерированный код (в репозитории, CI проверяет актуальность)
cmd/<service>/     тонкие main: конфиг → бутстрап → сборка зависимостей → запуск
internal/app       общий бутстрап: логгер, трейсинг, admin-сервер, сигналы
internal/kafkax    обёртки franz-go, цикл батч-консьюмера
internal/redisx    схема ключей, пайплайны, Lua-скрипты
internal/postgres  пул и миграции
internal/domain    базовые типы, чистая логика, без зависимостей
internal/<svc>     логика сервисов
migrations/        SQL для goose, встроен в бинарники
deploy/            Dockerfile, compose, Prometheus, Grafana
test/integration/  тесты на testcontainers (build tag `integration`)
docs/              архитектура, ADR, нагрузка
```

Конфигурация только через переменные окружения (twelve-factor); все ручки и
значения по умолчанию в структуре `Config` в начале каждого `cmd/*/main.go`.

## Нагрузка

`make load` запускает парк из 200 станков (1000 измерений каждые 500 мс)
против локального стека. Цифры, снятые на ноутбуке с дефолтным Compose
(один брокер Kafka, 2 реплики processor), лежат в [docs/load.md](docs/load.md).
