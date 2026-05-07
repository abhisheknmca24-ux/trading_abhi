import threading
import time
from queue import Queue, Full
from typing import Tuple
import requests
import os

# Config
MIN_SEND_INTERVAL = 1.2  # seconds between Telegram messages
MAX_QUEUE_SIZE = 100

# Allowed message types (only important messages)
ALLOWED_TYPES = {"PRE_SIGNAL", "CONFIRMED", "SKIPPED", "RESULT", "DAILY_REPORT"}

BOT_TOKEN = None
CHAT_ID = None
if os.getenv("RAILWAY_ENVIRONMENT"):
    try:
        from config_prod import BOT_TOKEN as _bt, CHAT_ID as _cid
        BOT_TOKEN = _bt
        CHAT_ID = _cid
    except Exception:
        BOT_TOKEN = None
        CHAT_ID = None
else:
    try:
        from config_local import BOT_TOKEN as _bt, CHAT_ID as _cid
        BOT_TOKEN = _bt
        CHAT_ID = _cid
    except Exception:
        BOT_TOKEN = None
        CHAT_ID = None


_queue: Queue[Tuple[str, str]] = Queue(maxsize=MAX_QUEUE_SIZE)
_last_sent = 0.0


def _worker():
    global _last_sent
    while True:
        try:
            typ, text = _queue.get()
        except Exception:
            time.sleep(0.1)
            continue

        # rate limit
        now = time.time()
        delta = now - _last_sent
        if delta < MIN_SEND_INTERVAL:
            time.sleep(MIN_SEND_INTERVAL - delta)

        if BOT_TOKEN is None or CHAT_ID is None:
            # Not configured — just print
            print(f"TELEGRAM [{typ}]: {text}")
            _last_sent = time.time()
            _queue.task_done()
            continue

        try:
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            res = requests.post(url, data={"chat_id": CHAT_ID, "text": text, "parse_mode": "Markdown"}, timeout=10)
            print("Telegram send:", res.status_code)
        except Exception as e:
            print("Telegram send error:", e)

        _last_sent = time.time()
        _queue.task_done()


# Start background worker
_thread = threading.Thread(target=_worker, daemon=True)
_thread.start()


def send(typ: str, text: str) -> bool:
    """Enqueue a typed message for Telegram. Returns True if queued."""
    typ = str(typ).upper()
    if typ not in ALLOWED_TYPES:
        # Drop messages not in allowed types to reduce spam (silent)
        return False

    try:
        _queue.put_nowait((typ, text))
        return True
    except Full:
        # Queue full — drop oldest to make room (best-effort)
        try:
            _queue.get_nowait()
            _queue.put_nowait((typ, text))
            return True
        except Exception:
            return False


def send_raw(text: str) -> bool:
    """Send a raw message as DAILY_REPORT (fallback)."""
    return send("DAILY_REPORT", text)
