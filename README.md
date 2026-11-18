# Payment processing service

Асинхронная обработка платежей: FastAPI, PostgreSQL, SQLAlchemy 2, RabbitMQ/FastStream.
Платёжный шлюз эмулируется, реальных списаний нет.

## Запуск

Нужны Docker Engine и Docker Compose v2.

```bash
cp .env.example .env
# Замените API_KEY в .env. Не коммитьте этот файл.
docker compose up --build -d
docker compose logs -f api consumer
```

API запускается после PostgreSQL и применяет миграции. Consumer запускается после API
и RabbitMQ. В основном Compose четыре сервиса: postgres, rabbitmq, api, consumer.
Outbox relay работает фоновой задачей в процессе consumer, отдельный контейнер не нужен.

Для проверки webhook можно запустить тестовый получатель:

```bash
docker compose -f compose.yaml -f compose.demo.yaml up --build -d
export API_KEY=local-development-key-change-me  # значение из вашего .env
curl -i http://localhost:8000/api/v1/payments \
  -H "X-API-Key: $API_KEY" -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{"amount":"1250.00","currency":"RUB","description":"Order 42","metadata":{"order_id":42},"webhook_url":"http://webhook-receiver:8080/hook"}'

curl http://localhost:8000/api/v1/payments/REPLACE_WITH_PAYMENT_ID \
  -H "X-API-Key: $API_KEY"
docker compose -f compose.yaml -f compose.demo.yaml logs webhook-receiver
```

Создание возвращает 202 с `payment_id`, `status`, `created_at`. Повтор того же ключа
и тела возвращает тот же платёж с его текущим статусом, без нового события. Другой
payload с тем же ключом — 409. Отсутствующий ключ — 422, неверный API key — 401,
несуществующий платёж — 404. GET возвращает также исходный `idempotency_key`.

Сумма должна быть положительной, не больше 16 знаков до запятой и 2 после.
Она передаётся и возвращается строкой. Валюты: RUB, USD, EUR.
Swagger/OpenAPI отключены: все HTTP-эндпоинты требуют X-API-Key.

## Как проходит платёж

1. API в одной транзакции вставляет payment и outbox. Уникальный индекс БД защищает
   Idempotency-Key при конкурентных запросах; hash нормализованного тела выявляет конфликт.
2. Relay выбирает готовые записи `FOR UPDATE SKIP LOCKED`, публикует persistent
   сообщение в durable exchange `payments` / очередь `payments.new` с publisher
   confirms и mandatory routing. Только затем отмечает запись опубликованной.
3. Один обработчик получает событие, блокирует строку платежа и эмулирует шлюз:
   2–5 секунд, 90% succeeded, 10% failed. Статус сохраняется до отправки webhook.
4. Тот же обработчик отправляет webhook. После успеха сохраняет время доставки.
   Повторная доставка сообщения не запускает обработанный платёж заново.
5. Ошибка webhook сохраняет номер попытки и следующую outbox-запись **в одной
   транзакции**. Задержки между тремя попытками: 1 и 2 секунды. После третьей
   ошибки создаётся outbox-событие для `payments.dead` / `payments.dlq`.
   ACK исходного сообщения происходит только после commit.

Расписание повторов хранится в PostgreSQL (`available_at`), а не в `asyncio.sleep`.
Запланированные повторы сохраняются после перезапуска consumer. Три попытки — это
первая отправка и два повтора, а не четыре отправки. Любой не-2xx ответ (включая
redirect) и HTTP timeout считается ошибкой доставки. Отказ эмулированного шлюза
является финальным бизнес-результатом `failed`, он тоже отправляется клиенту.
Ошибка webhook не превращает успешный платёж в failed. Непредвиденные ошибки
обработки тоже учитываются в сохранённом счётчике попыток. Если результат шлюза
так и не получен, платёж остаётся pending, а после третьей ошибки событие попадает в DLQ для разбора.

Для повреждённых/неизвестных сообщений применяется reject без requeue и RabbitMQ DLX.
Некорректный JSON перехватывается до валидации, поэтому не блокирует единственный consumer.
При недоступности БД сохранение счётчика невозможно: сообщение возвращается в очередь,
а не теряется. Такие повторы не расходуют три попытки обработки.
Relay продолжает перепубликацию при недоступности брокера без ограничения попыток.

## Гарантии и границы

Доставка **at least once**, не exactly once. Если RabbitMQ подтвердил публикацию,
а commit relay не произошёл, событие придёт повторно. Платёж и номер попытки
проверяются под блокировкой строки, дубликат уже выполненной попытки игнорируется.

Webhook содержит стабильный `event_id` (ID платежа), а также заголовки `X-Event-ID`
и `Idempotency-Key`. Если клиент принял webhook, но ответ потерялся, уведомление
придёт повторно. Получатель обязан дедуплицировать его по event_id.

Эмулятор не имеет внешнего состояния. При аварии после вызова эмулятора до commit
он может вызваться повторно. С реальным PSP нужен его idempotency key, проверка
статуса и сверка операций. Одной транзакцией PostgreSQL этого не обеспечить.
На весь HTTP-вызов действует общий таймаут `WEBHOOK_TIMEOUT` (по умолчанию 5 секунд),
а не только отдельные сетевые операции. Тело ответа webhook не буферизуется.
Блокировка строки удерживается во время эмуляции шлюза и отправки webhook.
Это упрощение для одного consumer; для высокой нагрузки подход нужно пересмотреть.

Разрешённые хосты webhook задаются в `WEBHOOK_ALLOWED_HOSTS` (через запятую),
redirect и proxy из окружения отключены. В production нужны проверенные адреса,
контроль DNS/egress, TLS, безопасное хранение секретов и
наблюдаемость. Allowlist не должен включать произвольные пользовательские домены.
Локальный HTTP receiver разрешён только для демонстрации. Брокер и PostgreSQL не
открыты наружу; management RabbitMQ доступен на localhost:15672 (payments/payments).
Учётные данные compose предназначены только для локального тестового окружения.

Таблица outbox не очищается автоматически, чтобы можно было проверить историю.
Для длительной работы нужны очистка старых записей, мониторинг возраста outbox/DLQ
и резервное копирование. После устранения причины ошибки сообщения из DLQ нужно
переотправлять вручную с новым счётчиком попыток. Автоматической переотправки нет.
Повтор POST с прежним ключом не перезапускает платёж.

## Подпись webhook и эксплуатация

`WEBHOOK_SIGNING_SECRET` включает HMAC-SHA256 подпись уведомлений. Это отдельный
секрет не короче 32 символов, не API key. Передайте его получателю по защищённому
каналу. Для генерации можно использовать `openssl rand -hex 32`.

Сервис отправляет `X-Webhook-Timestamp` (Unix seconds) и `X-Webhook-Signature`
(`v1=` и hex HMAC). Подписываются байты `timestamp + "." + raw_body`. Получатель
должен проверить подпись через constant-time compare, свежесть timestamp
(например, не старше 5 минут) и дедуплицировать стабильный `event_id`.
При повторной отправке timestamp и подпись обновляются. Demo receiver проверяет
подпись и 5-минутное окно, если ему передан тот же секрет через Compose.
Без переменной подпись отключена для совместимости с произвольным получателем;
для внешних интеграций включите её явно. Текущий сервис рассчитан на одного
оператора, а не на разные секреты нескольких независимых мерчантов.

`GET /health/ready` защищён тем же X-API-Key и проверяет PostgreSQL с двухсекундным
таймаутом. Compose использует эту проверку вместо проверки открытого TCP-порта.
RabbitMQ не входит в API readiness: при недоступном брокере API продолжает
надёжно записывать платежи и outbox. Доступность consumer, возраст неопубликованного
outbox и размер DLQ нужно контролировать отдельно.

Контейнеры получают 30 секунд на остановку. API запускается через `exec`, поэтому
SIGTERM доходит до uvicorn; consumer завершает активную обработку через lifecycle
FastStream. Неподтверждённые сообщения после остановки возвращаются брокеру.

## Проверка retry и DLQ

```bash
WEBHOOK_STATUS=503 docker compose -f compose.yaml -f compose.demo.yaml up -d --force-recreate webhook-receiver
# Создайте платёж с новым Idempotency-Key.
docker compose logs -f consumer
docker compose exec rabbitmq rabbitmqctl list_queues name messages
```

После трёх ошибок отправки в `payments.dlq` появится сообщение. Платёж сохранит
результат шлюза. Перед следующей проверкой верните получателю статус ответа 200.

## Тесты и разработка

```bash
python -m venv .venv
. .venv/bin/activate
pip install -c requirements.lock -e '.[dev]'
pytest -q
ruff check .
ruff format --check .
```

Без `TEST_DATABASE_URL` тесты PostgreSQL явно пропускаются. Для полной проверки
укажите доступную **тестовую** PostgreSQL; тесты создают отдельную временную схему
для каждого сценария и удаляют только её. Нужны права CREATE SCHEMA.

```bash
TEST_DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments pytest -q
```

Проверяются конкурентная идемпотентность, конфликт payload, авторизация, валидация,
сохранение outbox при неудачной публикации, дубликаты consumer, успешные/отклонённые
платежи, три попытки webhook и запись события для DLQ. Брокер в этих тестах
заменён mock. Для отдельного live smoke-теста с настоящими PostgreSQL, RabbitMQ и
HTTP webhook:

```bash
TEST_DATABASE_URL=postgresql+asyncpg://USER:PASS@localhost:5432/postgres \
RABBITMQ_URL=amqp://USER:PASS@localhost:5672/TEST_VHOST \
python scripts/live_smoke.py --processes
```

Нужен тестовый PostgreSQL с правом CREATEDB и отдельный пустой RabbitMQ vhost:
скрипт создаёт/удаляет временную БД, использует стандартные очереди сервиса и
забирает сообщения из DLQ. Не запускайте его против production. Проверяются
реальная публикация outbox, consumer, HTTP webhook, дубли, три ошибки доставки,
DLQ и продолжение работы после повреждённого JSON.

Режим `--processes` запускает API и consumer отдельными процессами через uvicorn
и FastStream CLI. Без флага API вызывается через ASGITransport.

Проверено при подготовке:

- Dockerfile собирается, Compose запускает все сервисы и проходит healthchecks
  на macOS ARM64 через Colima: PostgreSQL 16.16 и RabbitMQ 3.13.7.
- В собранном контейнере проходят 50 unit/integration тестов, Ruff и проверка
  форматирования. `alembic check` не обнаруживает расхождений схемы.
- Сквозная проверка Compose: API key, восемь конкурентных запросов с одним ключом,
  конфликт payload, GET с idempotency_key, шлюз, webhook с проверкой HMAC получателем,
  три неудачные попытки доставки и сообщение в DLQ.
- При остановленном RabbitMQ API сохраняет платёж и outbox. После запуска брокера
  событие публикуется, платёж обрабатывается и webhook доставляется.
- Отдельно проверены общий deadline webhook, отсутствие буферизации ответа,
  readiness при ошибке БД, миграции upgrade/check/downgrade/upgrade и обработка
  повреждённого JSON и повторных сообщений RabbitMQ.

## Структура проекта

```text
app/
  main.py                 # сборка FastAPI-приложения
  api/
    dependencies.py       # API key и получение сервиса
    payments.py           # HTTP-контракты и преобразование ошибок
  services/
    payments.py           # создание платежа и идемпотентность
    processing.py         # шлюз, статус, webhook и retry
    outbox.py             # публикация событий и подтверждение доставки
  repositories/
    payments.py           # INSERT/SELECT, конфликт ключа, row lock
    outbox.py             # добавление и выборка событий с SKIP LOCKED
  integrations/
    gateway.py            # эмулятор внешнего шлюза
    webhooks.py           # исходящий HTTP-запрос
  consumer.py             # lifecycle и ACK/NACK
  broker.py               # RabbitMQ topology
  db.py                   # engine и session factory
  models.py               # ORM-модели
  schemas.py              # Pydantic-контракты
  config.py               # настройки
migrations/
tests/
```

HTTP-слой не строит SQL-запросы. Репозитории получают существующий
`AsyncSession` и выполняют запросы без `commit`. Границами
транзакций управляют сервисы: payment и outbox создаются на одной сессии внутри
одного `session.begin()`. То же относится к сохранению попытки webhook и retry/DLQ
события. Relay отмечает публикацию после broker confirm в своей транзакции.
Изменения загруженных ORM-объектов сохраняются SQLAlchemy при commit сервиса.

Для двух таблиц используются конкретные репозитории, без общего CRUD и отдельного
Unit of Work. HTTP-вызовы вынесены в integrations, подтверждение сообщений остаётся
в consumer.
