"""telegram_queue.py — Queued Telegram message delivery.

Features
--------
* Async-style queue: messages are enqueued and sent in order.
* Silent mode: suppress non-critical messages when enabled.
* Grouped reporting: collect messages and send as a single grouped update.
* Queue diagnostics: track send attempts, failures, queue depth.
* Automatic cleanup of stale queued messages (> 5 minutes old).

Usage
-----
    from telegram_queue import enqueue, flush, set_silent_mode

    enqueue("Hello!")          # always sent
    enqueue("Debug info", level="DEBUG")  # skipped in silent mode

    flush(send_fn)             # send all pending messages via send_fn(text)
"""

from __future__ import annotations

import logging
import time
from collections import deque
from typing import Callable, Deque, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_SILENT_MODE = False
_MAX_QUEUE_SIZE = 50
_MESSAGE_TTL_SECONDS = 300       # discard unsent messages older than 5 min
_GROUPED_SEPARATOR = "\n─────────────\n"

# Priority levels
LEVEL_CRITICAL = "CRITICAL"   # always sent even in silent mode
LEVEL_INFO = "INFO"           # sent in normal mode
LEVEL_DEBUG = "DEBUG"         # skipped in silent mode

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

class _QueueEntry:
    __slots__ = ("text", "level", "enqueued_at")

    def __init__(self, text: str, level: str) -> None:
        self.text = text
        self.level = level
        self.enqueued_at = time.monotonic()


_queue: Deque[_QueueEntry] = deque(maxlen=_MAX_QUEUE_SIZE)

# Diagnostics
_total_enqueued: int = 0
_total_sent: int = 0
_total_failed: int = 0
_total_discarded: int = 0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def set_silent_mode(enabled: bool) -> None:
    """Enable or disable silent mode (suppresses DEBUG/INFO messages)."""
    global _SILENT_MODE
    _SILENT_MODE = enabled
    logger.info("telegram_queue: silent_mode=%s", enabled)


def enqueue(text: str, level: str = LEVEL_INFO) -> None:
    """Add a message to the queue.

    In silent mode, only CRITICAL messages are accepted.
    """
    global _total_enqueued, _total_discarded

    if _SILENT_MODE and level not in (LEVEL_CRITICAL,):
        _total_discarded += 1
        logger.debug("telegram_queue: discarded (silent) — %s", text[:60])
        return

    if not text or not text.strip():
        return

    _queue.append(_QueueEntry(text.strip(), level))
    _total_enqueued += 1
    logger.debug("telegram_queue: enqueued (level=%s, depth=%d)", level, len(_queue))


def flush(send_fn: Callable[[str], None], group: bool = False) -> int:
    """Send all pending messages via send_fn.

    Parameters
    ----------
    send_fn : callable
        Function that accepts a single string and sends it (e.g. send_telegram).
    group : bool
        If True, combine all pending messages into one grouped message.

    Returns the number of messages sent.
    """
    global _total_sent, _total_failed, _total_discarded

    now = time.monotonic()
    pending: List[_QueueEntry] = []

    while _queue:
        entry = _queue.popleft()
        age = now - entry.enqueued_at
        if age > _MESSAGE_TTL_SECONDS:
            _total_discarded += 1
            logger.debug("telegram_queue: discarded stale message (age=%.0fs)", age)
            continue
        pending.append(entry)

    if not pending:
        return 0

    if group and len(pending) > 1:
        combined = _GROUPED_SEPARATOR.join(e.text for e in pending)
        try:
            send_fn(combined)
            _total_sent += len(pending)
            return len(pending)
        except Exception as exc:
            logger.warning("telegram_queue: grouped send failed — %s", exc)
            _total_failed += 1
            return 0

    sent = 0
    for entry in pending:
        try:
            send_fn(entry.text)
            _total_sent += 1
            sent += 1
        except Exception as exc:
            logger.warning("telegram_queue: send failed — %s", exc)
            _total_failed += 1

    return sent


def get_diagnostics() -> dict:
    """Return queue health metrics."""
    return {
        "queue_depth": len(_queue),
        "total_enqueued": _total_enqueued,
        "total_sent": _total_sent,
        "total_failed": _total_failed,
        "total_discarded": _total_discarded,
        "silent_mode": _SILENT_MODE,
    }


def cleanup_stale() -> int:
    """Remove messages that have been in the queue too long without being sent."""
    global _total_discarded
    now = time.monotonic()
    before = len(_queue)
    fresh = deque(
        (e for e in _queue if (now - e.enqueued_at) <= _MESSAGE_TTL_SECONDS),
        maxlen=_MAX_QUEUE_SIZE,
    )
    removed = before - len(fresh)
    if removed:
        _queue.clear()
        _queue.extend(fresh)
        _total_discarded += removed
        logger.debug("telegram_queue: cleaned %d stale messages", removed)
    return removed
