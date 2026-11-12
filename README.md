# Интернет-магазин: Orders + Notifications + RabbitMQ

Практическое задание «Способы межсервисного взаимодействия», вариант 1.
Python, Flask, gunicorn, SQLite, RabbitMQ. Ответы на вопросы — в [REPORT.md](REPORT.md).

## Файлы

| Файл | Что это |
|---|---|
| `orders.py` | сервис Orders: `POST /orders`, `GET /orders`, `GET /health` |
| `notifications.py` | сервис Notifications: письмо = строчка в лог |
| `steps.py` | настройки шагов задания |
| `docker-compose.yml` | RabbitMQ и два сервиса |

## Запуск

```bash
docker compose up --build
```

Запускается итоговое состояние (шаг 3).

- Orders: http://localhost:8000/orders (список заказов)
- Панель RabbitMQ: http://localhost:15672 (guest / guest)

## Шаги

Шаг выбирается переменной `STEP` (команды для Git Bash):

```bash
docker compose down -v               # очистить базы и очередь
STEP=2-timeout docker compose up -d  # запустить нужный шаг
```

В PowerShell: `$env:STEP="2-timeout"; docker compose up -d`.

| STEP | Что включено |
|---|---|
| `1` | HTTP-вызов, Notifications отвечает за 1,5 с |
| `2` | Notifications отвечает за 30 с, без защиты |
| `2-timeout` | то же + таймаут 1 с |
| `2-retry` | то же + таймаут 1 с и 3 попытки |
| `3-no-dedup` | события через RabbitMQ, без защиты от повторов |
| `3` | события через RabbitMQ, с защитой от повторов (по умолчанию) |

Логи: `docker compose logs -f orders notifications`.

## Шаг 1

```bash
# без ключа: обрыв через 1 с и повтор -> два заказа
curl --max-time 1 -X POST localhost:8000/orders -H "Content-Type: application/json" -d '{"email":"ivanov@example.com"}'
curl -X POST localhost:8000/orders -H "Content-Type: application/json" -d '{"email":"ivanov@example.com"}'
curl localhost:8000/orders

# с ключом: повтор отдаёт прежний ответ
curl -X POST localhost:8000/orders -H "Content-Type: application/json" -H "Idempotency-Key: key-1" -d '{"email":"petrov@example.com"}'
```

## Шаг 2

В одном окне — `/health` раз в секунду:
```bash
for s in $(seq 1 45); do curl -s -o /dev/null -w "%{http_code} " --max-time 1 localhost:8000/health; sleep 1; done; echo
```
В другом — 10 заказов одновременно, у каждого свой ключ:
```bash
for i in $(seq 1 10); do curl -s -o /dev/null -w "order $i: %{http_code} %{time_total}s\n" --max-time 5 -X POST localhost:8000/orders -H "Content-Type: application/json" -H "Idempotency-Key: load-$RANDOM-$i" -d '{"email":"buyer@example.com"}' & done; wait
```
`000` — ответа за 5 с не было.

## Шаг 3

```bash
docker compose stop notifications    # заказы создаются, события копятся в очереди
docker compose start notifications   # письма уходят задним числом
```

Повторное сообщение — панель RabbitMQ → **Exchanges → shop.events → Publish message**:
routing key `order.created`, свойство `message_id` = `dup-1`,
payload `{"order_id": 100, "email": "buyer@example.com"}`. Нажать два раза.
При `STEP=3-no-dedup` уйдёт два письма, при `STEP=3` — одно.
