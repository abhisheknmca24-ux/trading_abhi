"""risk_management.py — Risk / exposure management for the trading bot.

Responsibilities
----------------
* Daily trade count limit (default 5–6, configurable)
* Dynamic confidence threshold adjustment (capped at +10 total)
* Gradual mode scaling  NORMAL → CAUTIOUS → SAFE (no sudden jumps)
* Debounced state persistence (every 30–60 s, not after every action)
* Mode history pruning (keep last 50 entries only)

Public API
----------
    is_trade_allowed() -> bool
    record_trade_open(confidence: float) -> None
    record_trade_close(result: str) -> None
    get_dynamic_threshold(base: int) -> int
    get_mode() -> str
    maybe_save_state() -> None
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
_STATE_FILE = "risk_state.json"
_MAX_DAILY_TRADES = 6           # reduced from 8 → safer long-term
_MAX_THRESHOLD_ADJUSTMENT = 10  # cap total dynamic threshold increase
_SAVE_DEBOUNCE_SECONDS = 45     # save at most every 45 s
_MODE_HISTORY_LIMIT = 50        # keep only recent mode entries

# Mode thresholds (consecutive losses to escalate)
_CAUTIOUS_AFTER = 2
_SAFE_AFTER = 4

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------
_state: dict = {
    "mode": "NORMAL",           # NORMAL | CAUTIOUS | SAFE
    "daily_trades": 0,
    "consecutive_losses": 0,
    "mode_history": [],
}
_loaded: bool = False
_dirty: bool = False
_last_save_time: float = 0.0


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _ensure_loaded() -> None:
    global _loaded
    if not _loaded:
        _load_state()
        _loaded = True


def _load_state() -> None:
    global _state
    try:
        if os.path.exists(_STATE_FILE):
            with open(_STATE_FILE, "r") as fh:
                loaded = json.load(fh)
                _state.update(loaded)
            logger.info("risk_management: state loaded")
    except Exception as exc:
        logger.warning("risk_management: load failed — %s", exc)


def _mark_dirty() -> None:
    global _dirty
    _dirty = True


def _update_mode() -> None:
    """Gradually escalate or de-escalate mode based on consecutive losses."""
    losses = _state["consecutive_losses"]
    current = _state["mode"]

    if losses >= _SAFE_AFTER:
        target = "SAFE"
    elif losses >= _CAUTIOUS_AFTER:
        target = "CAUTIOUS"
    else:
        target = "NORMAL"

    if target != current:
        logger.info("risk_management: mode %s → %s (consec_losses=%d)", current, target, losses)
        _state["mode"] = target
        _state["mode_history"].append({"from": current, "to": target, "losses": losses})
        # Prune history
        if len(_state["mode_history"]) > _MODE_HISTORY_LIMIT:
            _state["mode_history"] = _state["mode_history"][-_MODE_HISTORY_LIMIT:]
        _mark_dirty()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def is_trade_allowed() -> bool:
    """Return True if daily trade limit has not been reached."""
    _ensure_loaded()
    allowed = _state["daily_trades"] < _MAX_DAILY_TRADES
    if not allowed:
        logger.info(
            "risk_management: trade blocked — daily limit %d reached", _MAX_DAILY_TRADES
        )
    return allowed


def record_trade_open(confidence: float = 0.0) -> None:
    """Register that a new trade was opened."""
    _ensure_loaded()
    _state["daily_trades"] += 1
    _mark_dirty()
    logger.debug("risk_management: trade opened (daily=%d, conf=%.0f)", _state["daily_trades"], confidence)


def record_trade_close(result: str) -> None:
    """Register trade result and update mode gradually."""
    _ensure_loaded()

    if result == "WIN":
        # Each win reduces consecutive-loss counter by one (gradual recovery)
        _state["consecutive_losses"] = max(0, _state["consecutive_losses"] - 1)
    else:
        _state["consecutive_losses"] += 1

    _update_mode()
    _mark_dirty()


def get_dynamic_threshold(base: int) -> int:
    """Return a confidence threshold adjusted for current risk mode.

    Total adjustment is capped at +_MAX_THRESHOLD_ADJUSTMENT to prevent the
    threshold from becoming unreachably high.
    """
    _ensure_loaded()
    mode = _state["mode"]
    consec = _state["consecutive_losses"]

    adj = 0
    if mode == "CAUTIOUS":
        adj += 3
    elif mode == "SAFE":
        adj += 6

    # Additional adjustment based on consecutive losses (1 per loss, capped)
    adj += min(consec, 4)

    adj = min(adj, _MAX_THRESHOLD_ADJUSTMENT)
    result = base + adj

    logger.debug("risk_management: threshold %d + %d = %d (mode=%s)", base, adj, result, mode)
    return result


def get_mode() -> str:
    """Return the current risk mode string."""
    _ensure_loaded()
    return _state["mode"]


def reset_daily() -> None:
    """Reset per-day counters.  Call once at market open each day."""
    _ensure_loaded()
    _state["daily_trades"] = 0
    _mark_dirty()
    logger.info("risk_management: daily counters reset")


def maybe_save_state() -> None:
    """Debounced state save — only writes when dirty and enough time has passed."""
    if not _dirty:
        return
    elapsed = time.monotonic() - _last_save_time
    if elapsed < _SAVE_DEBOUNCE_SECONDS:
        return
    _save_state_now()


def _save_state_now() -> None:
    global _dirty, _last_save_time
    try:
        with open(_STATE_FILE, "w") as fh:
            json.dump(_state, fh, indent=2)
        _dirty = False
        _last_save_time = time.monotonic()
        logger.debug("risk_management: state saved")
    except Exception as exc:
        logger.warning("risk_management: save failed — %s", exc)
