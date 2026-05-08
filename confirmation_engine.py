"""confirmation_engine.py — Lightweight live-signal confirmation.

Purpose: detect only *dangerous* last-second conditions before entry.
Does NOT repeat the full validation stack (EMA alignment, RSI levels,
ATR strength, candle size).

Blocks only on:
  1. Sudden reversal (sharp opposing move in the last 1–2 candles)
  2. Dangerous volatility spike (ATR > 3.5× mean)
  3. Immediate momentum collapse (price flat after prior move)

Everything else is allowed through to avoid late-stage over-filtering.

Public API
----------
    validate_live_signal(df, direction) -> (bool, str)
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def _detect_sudden_reversal(df: pd.DataFrame, direction: str) -> tuple[bool, str]:
    """Block if the most recent candle is a sharp opposing move."""
    try:
        last = df.iloc[-1]
        atr = float(last["ATR"])
        if pd.isna(atr) or atr == 0:
            return True, "reversal check skipped"

        candle_move = float(last["Close"]) - float(last["Open"])
        # A reversal is a large opposing candle (> 0.8× ATR in wrong direction)
        if direction == "CALL" and candle_move < -atr * 0.8:
            return False, f"sudden bearish reversal ({candle_move:.5f})"
        if direction == "PUT" and candle_move > atr * 0.8:
            return False, f"sudden bullish reversal ({candle_move:.5f})"

        return True, "no reversal"
    except Exception as exc:
        logger.debug("_detect_sudden_reversal error: %s", exc)
        return True, "reversal check skipped"


def _detect_volatility_spike(df: pd.DataFrame) -> tuple[bool, str]:
    """Block on extreme ATR spike (> 3.5× mean)."""
    try:
        atr = float(df.iloc[-1]["ATR"])
        atr_mean = float(df["ATR"].tail(20).mean())
        if pd.isna(atr) or pd.isna(atr_mean) or atr_mean == 0:
            return True, "spike check skipped"

        ratio = atr / atr_mean
        if ratio > 3.5:
            return False, f"volatility spike (ATR×{ratio:.1f})"
        return True, f"volatility ok (ATR×{ratio:.1f})"
    except Exception as exc:
        logger.debug("_detect_volatility_spike error: %s", exc)
        return True, "spike check skipped"


def _detect_momentum_collapse(df: pd.DataFrame, direction: str) -> tuple[bool, str]:
    """Block if price has gone completely flat after a prior directional move.

    Minor pullbacks inside strong trends are explicitly allowed — we only reject
    near-zero movement after the signal was already trending.
    """
    try:
        last = df.iloc[-1]
        atr = float(last["ATR"])
        if pd.isna(atr) or atr == 0:
            return True, "momentum check skipped"

        # Net move of last 3 candles in signal direction
        if len(df) < 4:
            return True, "insufficient data"

        recent_close = float(df.iloc[-1]["Close"])
        base_close = float(df.iloc[-4]["Close"])
        net_move = recent_close - base_close

        min_net_move = atr * 0.05   # very lenient — only reject near-zero

        if direction == "CALL" and net_move < -min_net_move:
            return False, f"momentum collapsed bearish ({net_move:.5f})"
        if direction == "PUT" and net_move > min_net_move:
            return False, f"momentum collapsed bullish ({net_move:.5f})"

        return True, "momentum ok"
    except Exception as exc:
        logger.debug("_detect_momentum_collapse error: %s", exc)
        return True, "momentum check skipped"


def validate_live_signal(df: pd.DataFrame, direction: str) -> tuple[bool, str]:
    """Lightweight live validation.

    Returns (True, reason) when it is safe to proceed with the entry.
    Blocks only on: sudden reversal, extreme spike, or total momentum collapse.
    Does NOT check EMA, RSI levels, candle size, or ATR vs mean.
    """
    if df is None or len(df) < 5:
        return True, "validation skipped (insufficient data)"

    ok, reason = _detect_sudden_reversal(df, direction)
    if not ok:
        return False, reason

    ok, reason = _detect_volatility_spike(df)
    if not ok:
        return False, reason

    ok, reason = _detect_momentum_collapse(df, direction)
    if not ok:
        return False, reason

    return True, "live validation passed"
