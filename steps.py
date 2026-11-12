"""Настройки шагов задания. Шаг выбирается при запуске: STEP=2-timeout docker compose up -d"""
import os

STEPS = {
    #              режим            задержка Notifications, с  таймаут, с         попыток             защита от повторов
    "1":          dict(mode="sync",  notify_delay=1.5,          call_timeout=0,    retry_attempts=1,   idempotent=False),
    "2":          dict(mode="sync",  notify_delay=30,           call_timeout=0,    retry_attempts=1,   idempotent=False),
    "2-timeout":  dict(mode="sync",  notify_delay=30,           call_timeout=1,    retry_attempts=1,   idempotent=False),
    "2-retry":    dict(mode="sync",  notify_delay=30,           call_timeout=1,    retry_attempts=3,   idempotent=False),
    "3-no-dedup": dict(mode="async", notify_delay=0,            call_timeout=0,    retry_attempts=1,   idempotent=False),
    "3":          dict(mode="async", notify_delay=0,            call_timeout=0,    retry_attempts=1,   idempotent=True),
}

STEP = os.getenv("STEP") or "3"
if STEP not in STEPS:
    raise SystemExit(f"Неизвестный STEP={STEP}. Допустимо: {', '.join(STEPS)}")

MODE = STEPS[STEP]["mode"]                      # sync — HTTP-вызов, async — RabbitMQ
NOTIFY_DELAY = STEPS[STEP]["notify_delay"]      # через сколько секунд отвечает Notifications
CALL_TIMEOUT = STEPS[STEP]["call_timeout"]      # таймаут вызова Notifications; 0 — без таймаута
RETRY_ATTEMPTS = STEPS[STEP]["retry_attempts"]  # всего попыток вызова Notifications
IDEMPOTENT = STEPS[STEP]["idempotent"]          # отбрасывать повторные сообщения
