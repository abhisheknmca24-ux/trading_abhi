"""learning_engine.py — Soft adaptive learning for signal sources.

Design
------
* In-memory cache — JSON loaded ONCE at startup, saved periodically (not on
  every operation).
* Penalties are soft and small:  0 / 3 / 7 / 12  (not 0 / 5 / 15 / 25).
* Learning adjusts *probability weight*, it does NOT fully block trades.
* Source ranking boosts/reduces confidence slightly instead of hard blocking.
* Automatic cleanup: at startup, after every 50 trades, and at daily reset.

Public API
----------
    get_confidence_adjustment(source: str) -> int
        Returns a small integer to add/subtract from a signal's confidence.
        Never returns a value that would drop confidence by more than 12.

    record_outcome(source: str, result: str, confidence: float) -> None
        Call after a trade resolves.  result must be "WIN" or "LOSS".

    maybe_periodic_save() -> None
        Call occasionally (e.g. every bot cycle) — saves only when pending.

    reset_daily() -> None
        Call at start of each trading day.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_MEMORY_FILE = "learning_memory.json"
_SAVE_INTERVAL_SECONDS = 300   # save at most every 5 minutes
_CLEANUP_EVERY_N_TRADES = 50
_MAX_HISTORY_PER_SOURCE = 100  # keep only the most recent outcomes

# Penalty scale (indexed 0–3 by consecutive-loss count)
_PENALTY_SCALE = [0, 3, 7, 12]

# Source boost range: a strong source can add up to +5, weak source -5.
_MAX_SOURCE_BOOST = 5
_MAX_SOURCE_PENALTY = 5

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------
_memory: Dict[str, dict] = {}   # source_id → {wins, losses, history:[...]}
_loaded: bool = False
_dirty: bool = False             # True when unsaved changes exist
_last_save_time: float = 0.0
_trades_since_cleanup: int = 0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ensure_loaded() -> None:
    global _loaded
    if not _loaded:
        _load_from_disk()
        _loaded = True
        _cleanup_stale_entries()


def _load_from_disk() -> None:
    global _memory
    try:
        if os.path.exists(_MEMORY_FILE):
            with open(_MEMORY_FILE, "r") as fh:
                _memory = json.load(fh)
            logger.info("learning_engine: loaded %d sources from %s", len(_memory), _MEMORY_FILE)
        else:
            _memory = {}
    except Exception as exc:
        logger.warning("learning_engine: load failed — %s", exc)
        _memory = {}


def _save_to_disk() -> None:
    global _dirty, _last_save_time
    try:
        with open(_MEMORY_FILE, "w") as fh:
            json.dump(_memory, fh, indent=2)
        _dirty = False
        _last_save_time = time.monotonic()
        logger.debug("learning_engine: saved %d sources", len(_memory))
    except Exception as exc:
        logger.warning("learning_engine: save failed — %s", exc)


def _cleanup_stale_entries() -> None:
    """Remove sources with very old / small histories to save memory."""
    global _memory
    removed = 0
    for source in list(_memory.keys()):
        data = _memory[source]
        history = data.get("history", [])
        # Trim to max history length
        if len(history) > _MAX_HISTORY_PER_SOURCE:
            _memory[source]["history"] = history[-_MAX_HISTORY_PER_SOURCE:]
            removed += 1
        # Drop sources with zero activity
        if data.get("wins", 0) + data.get("losses", 0) == 0:
            del _memory[source]
            removed += 1
    if removed:
        logger.debug("learning_engine: cleanup removed/trimmed %d entries", removed)


def _get_source_data(source: str) -> dict:
    if source not in _memory:
        _memory[source] = {"wins": 0, "losses": 0, "history": []}
    return _memory[source]


def _compute_win_rate(source: str) -> Optional[float]:
    data = _get_source_data(source)
    total = data["wins"] + data["losses"]
    if total < 5:
        return None   # not enough data
    return data["wins"] / total


def _consecutive_losses(source: str, lookback: int = 5) -> int:
    history = _get_source_data(source).get("history", [])
    count = 0
    for outcome in reversed(history[-lookback:]):
        if outcome == "LOSS":
            count += 1
        else:
            break
    return count


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_confidence_adjustment(source: str) -> int:
    """Return a small confidence delta (negative = penalty, positive = boost).

    This is *soft*: maximum impact is ±12 points so learning adjusts
    probability rather than controlling execution.
    """
    _ensure_loaded()

    win_rate = _compute_win_rate(source)
    consec_losses = _consecutive_losses(source)

    # Penalty from consecutive losses
    penalty_idx = min(consec_losses, len(_PENALTY_SCALE) - 1)
    penalty = -_PENALTY_SCALE[penalty_idx]

    # Boost / penalty from historical win rate
    source_adj = 0
    if win_rate is not None:
        if win_rate > 0.70:
            source_adj = _MAX_SOURCE_BOOST
        elif win_rate > 0.60:
            source_adj = 2
        elif win_rate < 0.40:
            source_adj = -_MAX_SOURCE_PENALTY
        elif win_rate < 0.50:
            source_adj = -2

    total = penalty + source_adj
    # Clamp so learning never causes more than -12 or +5 shift
    return max(-12, min(_MAX_SOURCE_BOOST, total))


def record_outcome(source: str, result: str, confidence: float = 0.0) -> None:
    """Record a trade outcome for the given source."""
    global _dirty, _trades_since_cleanup

    _ensure_loaded()

    if result not in ("WIN", "LOSS"):
        logger.warning("learning_engine: unknown result '%s'", result)
        return

    data = _get_source_data(source)
    if result == "WIN":
        data["wins"] += 1
    else:
        data["losses"] += 1

    data["history"].append(result)
    if len(data["history"]) > _MAX_HISTORY_PER_SOURCE:
        data["history"] = data["history"][-_MAX_HISTORY_PER_SOURCE:]

    _dirty = True
    _trades_since_cleanup += 1

    if _trades_since_cleanup >= _CLEANUP_EVERY_N_TRADES:
        _cleanup_stale_entries()
        _trades_since_cleanup = 0

    logger.debug("learning_engine: %s → %s (conf %.0f)", source, result, confidence)


def maybe_periodic_save() -> None:
    """Save to disk if there are pending changes and enough time has passed."""
    if not _dirty:
        return
    elapsed = time.monotonic() - _last_save_time
    if elapsed >= _SAVE_INTERVAL_SECONDS:
        _save_to_disk()


def reset_daily() -> None:
    """Called at the start of each trading day to reset per-day counters."""
    global _trades_since_cleanup
    _cleanup_stale_entries()
    _trades_since_cleanup = 0
    logger.info("learning_engine: daily reset complete")
