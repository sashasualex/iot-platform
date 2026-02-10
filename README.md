# events-collector-service

Микросервис для обработки Avro-событий из Kafka, сохранения в ClickHouse и публикации уникальных `device_id` через Transactional Outbox.

## Архитектура
- Consumer (`events`) читает **по одному сообщению** (`max.poll.records=1`, manual ack).
- Каждое событие сохраняется в `device_events`.
- Redis `SADD devices:seen` выполняет глобальную дедупликацию device.
- Для нового device создается запись в `device_outbox` (status=NEW).
- CRON-публикатор:
  - захватывает `outbox:lock` в Redis;
  - читает NEW outbox;
  - применяет дополнительную идемпотентность через `SADD devices:published`;
  - публикует `device_id` в топик `devices`;
  - помечает запись SENT.

## Запуск локально
```bash
make up
```

## Остановка
```bash
make down
```

## Тесты
```bash
make test
```

## Конфигурация
Ключевые параметры через env:
- `KAFKA_BOOTSTRAP_SERVERS`
- `SCHEMA_REGISTRY_URL`
- `CLICKHOUSE_JDBC_URL`
- `REDIS_HOST`, `REDIS_PORT`

Профили:
- `default/local`
- `docker`

## Actuator
- `/actuator/health`
- `/actuator/metrics`
- `/actuator/prometheus`

## Диаграммы
- `diagrams/containers.puml`
- `diagrams/events-collector-components.puml`
- `diagrams/events-collector-sequence.puml`

## Troubleshooting
- Если не стартует десериализация Avro — проверьте `SCHEMA_REGISTRY_URL`.
- Если outbox не публикуется — проверьте наличие ключа `outbox:lock` в Redis и метрики `outbox.publish.fail.total`.
