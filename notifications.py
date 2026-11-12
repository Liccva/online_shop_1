"""Notifications: письмо покупателю (вместо письма — строчка в лог).

sync  (шаги 1–2): HTTP-сервер, принимает POST /notify от Orders.
async (шаг 3):    читает события OrderCreated из очереди RabbitMQ.
"""
import json
import logging
import sqlite3
import time

import pika
from flask import Flask, jsonify, request

from steps import IDEMPOTENT, MODE, NOTIFY_DELAY, STEP

RABBITMQ_HOST = "rabbitmq"
DB_PATH = "/data/notifications.db"
EXCHANGE = "shop.events"
QUEUE = "notifications.order-created"
ROUTING_KEY = "order.created"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [notifications] %(message)s",
                    datefmt="%H:%M:%S", force=True)
logging.getLogger("werkzeug").setLevel(logging.WARNING)
logging.getLogger("pika").setLevel(logging.WARNING)
log = logging.getLogger("notifications")


def get_db():
    return sqlite3.connect(DB_PATH, timeout=10)


conn = get_db()
conn.executescript("""
    CREATE TABLE IF NOT EXISTS notifications (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id   INTEGER,
        email      TEXT,
        message_id TEXT,
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS processed_messages (
        message_id   TEXT PRIMARY KEY,
        processed_at TEXT DEFAULT CURRENT_TIMESTAMP
    );
""")
conn.close()


def send_email(order_id, email):
    log.info("ПИСЬМО отправлено на %s: заказ #%s оформлен", email, order_id)


# ---------- Шаги 1–2: HTTP ----------
app = Flask(__name__)


@app.post("/notify")
def notify():
    data = request.get_json(silent=True) or {}
    order_id, email = data.get("order_id"), data.get("email")
    conn = get_db()
    with conn:
        conn.execute("INSERT INTO notifications (order_id, email) VALUES (?, ?)", (order_id, email))
    conn.close()
    send_email(order_id, email)
    time.sleep(NOTIFY_DELAY)  # письмо уже ушло, а ответ задерживается
    return jsonify(status="sent")


# ---------- Шаг 3: очередь ----------
def handle_message(channel, method, properties, body):
    event = json.loads(body)
    message_id = properties.message_id or event.get("message_id")
    order_id, email = event.get("order_id"), event.get("email")
    log.info("получено событие OrderCreated: заказ #%s, message_id=%s", order_id, message_id)

    conn = get_db()
    try:
        with conn:  # одна транзакция: отметка об обработке + запись письма
            if IDEMPOTENT:
                conn.execute("INSERT INTO processed_messages (message_id) VALUES (?)", (message_id,))
            conn.execute("INSERT INTO notifications (order_id, email, message_id) VALUES (?, ?, ?)",
                         (order_id, email, message_id))
    except sqlite3.IntegrityError:  # message_id уже обработан
        log.info("message_id=%s уже обработан — повтор, письмо НЕ отправляю", message_id)
        channel.basic_ack(delivery_tag=method.delivery_tag)
        return
    finally:
        conn.close()

    send_email(order_id, email)                          # письмо — после коммита
    channel.basic_ack(delivery_tag=method.delivery_tag)  # ack — после обработки


def consume():
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=QUEUE, durable=True, exclusive=False, auto_delete=False)
    channel.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key=ROUTING_KEY)
    channel.basic_qos(prefetch_count=1)  # по одному сообщению
    channel.basic_consume(queue=QUEUE, on_message_callback=handle_message, auto_ack=False)
    channel.start_consuming()


if __name__ == "__main__":
    log.info("шаг %s: режим %s, задержка ответа %s с, защита от повторов: %s",
             STEP, MODE, NOTIFY_DELAY, "да" if IDEMPOTENT else "нет")
    if MODE == "async":
        consume()
    else:
        app.run(host="0.0.0.0", port=8001, threaded=True)
