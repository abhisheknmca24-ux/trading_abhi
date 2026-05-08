"""market_safety.py — Lightweight market-condition safety checks.

Responsibilities
----------------
* Session filtering  (London / NY / overlap allowed; others soft-skip)
* Dangerous volatility spikes (extreme ATR only)
* Sideways / choppy market detection (relaxed thresholds)
* Dangerous wick structures (only extreme wicks)

NOT responsible for
-------------------
* RSI thresholds (belongs to signal_validator)
* EMA alignment checks (belongs to signal_validator)
* Confidence scoring (belongs to signal_list / calculate_confidence)

All checks return ``(allowed: bool, reason: str)`` so callers can log reasons.
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Session windows (UTC+5:30 = Asia/Kolkata)
# ---------------------------------------------------------------------------
# London:  08:00–16:30 IST   → 13:30–22:00 IST in Kolkata terms:
#   Actually forex sessions in IST:
#   Tokyo   : 05:30 – 14:30
#   London  : 13:30 – 22:30
#   New York: 18:30 – 03:00 (next day)
#   Overlap : 18:30 – 22:30

_ALLOWED_SESSIONS = {"london", "newyork", "overlap"}


def _get_session(hour_ist: int) -> str:
    """Return the current forex session name based on IST hour."""
    if 5 <= hour_ist < 14:
        return "tokyo"
    if 13 <= hour_ist < 22:
        if 18 <= hour_ist < 23:
            return "overlap"
        return "london"
    if 18 <= hour_ist or hour_ist < 4:
        return "newyork"
    return "off"


def check_session(now_ist: Optional[pd.Timestamp] = None) -> tuple[bool, str]:
    """Return (True, session) when in an allowed trading session."""
    if now_ist is None:
        now_ist = pd.Timestamp.now(tz="Asia/Kolkata")

    session = _get_session(now_ist.hour)

    if session in _ALLOWED_SESSIONS or session == "london":
        return True, session

    # Tokyo is allowed with a lighter confidence boost (caller decides)
    if session == "tokyo":
        return True, "tokyo"  # allowed but suboptimal

    return False, f"off-session ({session})"


# ---------------------------------------------------------------------------
# Volatility checks
# ---------------------------------------------------------------------------

def check_volatility(df: pd.DataFrame) -> tuple[bool, str]:
    """Block only *extreme* volatility spikes (news events / flash crashes).

    A spike is defined as ATR > 4× its 20-period mean.  Mild ATR elevation is
    explicitly allowed to avoid over-filtering.
    """
    try:
        atr = float(df.iloc[-1]["ATR"])
        atr_mean = float(df["ATR"].tail(20).mean())

        if pd.isna(atr) or pd.isna(atr_mean) or atr_mean == 0:
            return True, "atr unavailable"

        ratio = atr / atr_mean

        if ratio > 4.0:
            return False, f"extreme volatility spike ATR×{ratio:.1f}"

        return True, f"volatility ok (ATR×{ratio:.1f})"
    except Exception as exc:
        logger.debug("check_volatility error: %s", exc)
        return True, "volatility check skipped"


# ---------------------------------------------------------------------------
# Sideways / choppy market
# ---------------------------------------------------------------------------

def check_sideways(df: pd.DataFrame) -> tuple[bool, str]:
    """Block only clearly sideways markets.

    Uses relaxed thresholds so medium-quality trends are allowed through.
    A market is considered sideways when:
    - EMA50/200 gap is extremely narrow (< 0.5× ATR) AND
    - the 20-candle range is less than 1.0× ATR
    """
    try:
        last = df.iloc[-1]
        atr = float(last["ATR"])
        if pd.isna(atr) or atr == 0:
            return True, "atr unavailable"

        ema_gap = abs(float(last["EMA50"]) - float(last["EMA200"]))
        ema_gap_threshold = atr * 0.5  # relaxed from typical 1.0

        recent = df.tail(20)
        price_range = float(recent["High"].max() - recent["Low"].min())
        range_threshold = atr * 1.0  # relaxed

        if ema_gap < ema_gap_threshold and price_range < range_threshold:
            return False, "sideways market detected"

        return True, "trending ok"
    except Exception as exc:
        logger.debug("check_sideways error: %s", exc)
        return True, "sideways check skipped"


def check_choppy(df: pd.DataFrame, lookback: int = 8) -> tuple[bool, str]:
    """Block only very choppy markets (many rapid direction flips).

    Reduced sensitivity: require at least 6 direction changes in 8 candles to
    block (down from typical 5 in 6), so small pullbacks inside trends pass.
    """
    try:
        closes = df["Close"].tail(lookback + 1).values
        if len(closes) < lookback + 1:
            return True, "insufficient data"

        direction_changes = sum(
            1
            for i in range(1, len(closes) - 1)
            if (closes[i] - closes[i - 1]) * (closes[i + 1] - closes[i]) < 0
        )

        max_allowed = lookback - 2  # e.g. 6 changes in 8 candles

        if direction_changes >= max_allowed:
            return False, f"choppy market ({direction_changes} direction flips)"

        return True, f"directional ok ({direction_changes} flips)"
    except Exception as exc:
        logger.debug("check_choppy error: %s", exc)
        return True, "choppy check skipped"


# ---------------------------------------------------------------------------
# Wick structure
# ---------------------------------------------------------------------------

def check_wick(df: pd.DataFrame) -> tuple[bool, str]:
    """Block only *extreme* wick rejections.

    A candle is dangerous when the wick is more than 4× the body size AND the
    body is small (< 0.3× ATR).  Strong-body breakout candles with medium
    wicks are explicitly allowed.
    """
    try:
        last = df.iloc[-1]
        atr = float(last["ATR"])
        if pd.isna(atr) or atr == 0:
            return True, "atr unavailable"

        body = abs(float(last["Close"]) - float(last["Open"]))
        candle_range = float(last["High"]) - float(last["Low"])
        wick = candle_range - body

        if body == 0:
            return True, "doji — wick check skipped"

        wick_ratio = wick / body

        # Only reject truly extreme wick with a small body
        if wick_ratio > 4.0 and body < atr * 0.3:
            return False, f"extreme wick rejection (wick×{wick_ratio:.1f} body)"

        return True, f"wick ok (wick×{wick_ratio:.1f} body)"
    except Exception as exc:
        logger.debug("check_wick error: %s", exc)
        return True, "wick check skipped"


# ---------------------------------------------------------------------------
# Combined check
# ---------------------------------------------------------------------------

def check_market_safety(
    df: pd.DataFrame,
    now_ist: Optional[pd.Timestamp] = None,
) -> tuple[bool, str]:
    """Run all market-safety checks.  Returns (True, reason) when safe to trade.

    Breakout override: if volatility is moderate-high AND no wick danger, bypass
    sideways/choppy blocks (but never bypass extreme volatility or extreme wick).
    """
    # 1. Session check
    session_ok, session_reason = check_session(now_ist)
    if not session_ok:
        return False, session_reason

    # 2. Extreme volatility — always blocks, no override
    vol_ok, vol_reason = check_volatility(df)
    if not vol_ok:
        return False, vol_reason

    # 3. Wick danger — blocks only extreme wick
    wick_ok, wick_reason = check_wick(df)
    if not wick_ok:
        return False, wick_reason

    # 4. Sideways / choppy — can be overridden by breakout momentum
    sideways_ok, sideways_reason = check_sideways(df)
    choppy_ok, choppy_reason = check_choppy(df)

    if not sideways_ok or not choppy_ok:
        # Breakout override: allow if ATR is meaningfully elevated
        try:
            atr = float(df.iloc[-1]["ATR"])
            atr_mean = float(df["ATR"].tail(20).mean())
            if not pd.isna(atr) and not pd.isna(atr_mean) and atr_mean > 0:
                if atr > atr_mean * 1.3:
                    logger.debug("market_safety: breakout override (%s / %s)", sideways_reason, choppy_reason)
                    return True, "breakout override"
        except Exception:
            pass

        block_reason = sideways_reason if not sideways_ok else choppy_reason
        return False, block_reason

    return True, f"market safe — {session_reason}"
