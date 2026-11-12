"""Orders: приём заказов.

sync  (шаги 1–2): сохраняет заказ и вызывает Notifications по HTTP.
async (шаг 3):    сохраняет заказ и публикует событие OrderCreated в RabbitMQ.
"""
import json
import logging
import sqlite3
import time
import uuid

import pika
import requests
from flask import Flask, jsonify, request
from tenacity import Retrying, stop_after_attempt, wait_exponential_jitter

from steps import CALL_TIMEOUT, MODE, RETRY_ATTEMPTS, STEP

NOTIFICATIONS_URL = "http://notifications:8001/notify"
RABBITMQ_HOST = "rabbitmq"
DB_PATH = "/data/orders.db"
EXCHANGE = "shop.events"
QUEUE = "notifications.order-created"
ROUTING_KEY = "order.created"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [orders] %(message)s",
                    datefmt="%H:%M:%S", force=True)
logging.getLogger("pika").setLevel(logging.WARNING)
log = logging.getLogger("orders")

app = Flask(__name__)
app.json.ensure_ascii = False


def get_db():
    return sqlite3.connect(DB_PATH, timeout=10)  # ждать, если базу пишет другой воркер


conn = get_db()
conn.executescript("""
    CREATE TABLE IF NOT EXISTS orders (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        email      TEXT NOT NULL,
        item       TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    -- response пустой, пока первый запрос с этим ключом выполняется
    CREATE TABLE IF NOT EXISTS idempotency_keys (
        key      TEXT PRIMARY KEY,
        response TEXT
    );
""")
conn.close()


# ---------- Шаги 1–2: HTTP-вызов Notifications ----------
def call_notifications(order_id, email):
    retrying = Retrying(
        stop=stop_after_attempt(RETRY_ATTEMPTS),
        wait=wait_exponential_jitter(initial=0.5, max=4, jitter=0.5),  # 0.5 с, 1 с, 2 с… + до 0.5 с случайно
        reraise=True,
    )
    try:
        for attempt in retrying:
            with attempt:
                log.info("заказ #%s: вызываю Notifications (попытка %s из %s)",
                         order_id, attempt.retry_state.attempt_number, RETRY_ATTEMPTS)
                response = requests.post(NOTIFICATIONS_URL, json={"order_id": order_id, "email": email},
                                         timeout=CALL_TIMEOUT or None)  # None — ждать без ограничения
                response.raise_for_status()
        log.info("заказ #%s: Notifications ответил, письмо отправлено", order_id)
    except requests.RequestException as error:
        # заказ уже сохранён, клиент всё равно получит 201
        log.error("заказ #%s сохранён, но письмо НЕ отправлено: %s", order_id, type(error).__name__)


# ---------- Шаг 3: событие в RabbitMQ ----------
def declare_queue(channel):
    # durable, не exclusive, не auto-delete: очередь копит события, пока Notifications остановлен
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE, durable=True, exclusive=False, auto_delete=False)
    channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=ROUTING_KEY)


def publish_order_created(order_id, email):
    message_id = str(uuid.uuid4())
    event = {"event": "OrderCreated", "message_id": message_id, "order_id": order_id, "email": email}
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
        channel = connection.channel()
        declare_queue(channel)
        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=ROUTING_KEY,
            body=json.dumps(event),
            properties=pika.BasicProperties(
                message_id=message_id,  # по нему Notifications узнаёт повторы
                delivery_mode=2,        # сообщение переживает перезапуск брокера
                content_type="application/json",
            ),
        )
        connection.close()
        log.info("заказ #%s: опубликовано событие OrderCreated, message_id=%s", order_id, message_id)
    except pika.exceptions.AMQPError as error:
        log.error("заказ #%s сохранён, но событие НЕ опубликовано: %r", order_id, error)


if MODE == "async":  # очередь должна существовать до первого заказа
    _connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    declare_queue(_connection.channel())
    _connection.close()


# ---------- HTTP API ----------
@app.post("/orders")
def create_order():
    started = time.monotonic()
    data = request.get_json(silent=True) or {}
    email = data.get("email")
    if not email:
        return jsonify(error="в теле запроса нужно поле email"), 400

    key = request.headers.get("Idempotency-Key")
    conn = get_db()
    try:
        # 1. Запоминаем ключ до создания заказа
        if key:
            try:
                with conn:  # транзакция
                    conn.execute("INSERT INTO idempotency_keys (key, response) VALUES (?, NULL)", (key,))
            except sqlite3.IntegrityError:  # ключ уже был — это повтор
                saved = conn.execute("SELECT response FROM idempotency_keys WHERE key = ?",
                                     (key,)).fetchone()[0]
                if saved is None:
                    log.info("ключ %s: первый запрос ещё выполняется -> 409", key)
                    return jsonify(error="запрос с этим ключом ещё выполняется, повторите позже"), 409
                log.info("ключ %s: повтор запроса, отдаю сохранённый ответ", key)
                return app.response_class(saved, status=201, mimetype="application/json")

        # 2. Сохраняем заказ
        with conn:
            order_id = conn.execute("INSERT INTO orders (email, item) VALUES (?, ?)",
                                    (email, data.get("item"))).lastrowid
        log.info("заказ #%s сохранён (ключ: %s)", order_id, key or "нет")

        # 3. Уведомляем Notifications
        if MODE == "async":
            publish_order_created(order_id, email)
        else:
            call_notifications(order_id, email)

        # 4. Сохраняем ответ для повторов с этим ключом
        response = json.dumps({"order_id": order_id, "status": "created"})
        if key:
            with conn:
                conn.execute("UPDATE idempotency_keys SET response = ? WHERE key = ?", (response, key))
        log.info("заказ #%s: ответ 201 через %.1f с", order_id, time.monotonic() - started)
        return app.response_class(response, status=201, mimetype="application/json")
    finally:
        conn.close()


@app.get("/orders")
def list_orders():
    conn = get_db()
    rows = conn.execute("SELECT id, email, item, created_at FROM orders ORDER BY id").fetchall()
    conn.close()
    return jsonify(count=len(rows),
                   orders=[dict(zip(("id", "email", "item", "created_at"), row)) for row in rows])


@app.get("/health")
def health():
    return jsonify(status="ok", step=STEP)
