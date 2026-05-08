"""martingale_safety.py — Lightweight Martingale safety checks.

The previous implementation called validate_sniper_signal() (which already
checked EMA / RSI / ATR / candle / distance) and then added *more* RSI and
candle-direction checks on top — making MG confirmations extremely rare.

This module replaces that with a focused three-part check:
  1. No dangerous reversal (price moving against entry direction)
  2. Sufficient momentum (not completely flat)
  3. Acceptable volatility (not a spike, not dead)

No EMA alignment, no RSI threshold, no candle-size gating.

Public API
----------
    is_mg_safe(df, direction) -> (bool, str)
    mg_confidence_adjustment(df, direction) -> int
        Returns a small bonus/penalty to add to the base confidence.
        Range: -5 to +5.  Does NOT reject trades on its own.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Minimum confidence to trigger MG entry (reduced from 75 → 70)
# ---------------------------------------------------------------------------
MG_MIN_CONFIDENCE = 70


# ---------------------------------------------------------------------------
# 1. Reversal detection
# ---------------------------------------------------------------------------

def _no_dangerous_reversal(df: pd.DataFrame, direction: str) -> tuple[bool, str]:
    """Return True when the last two candles do NOT show a clear reversal."""
    try:
        last = df.iloc[-1]
        prev = df.iloc[-2]
        c0 = float(last["Close"])
        c1 = float(prev["Close"])
        c2 = float(df.iloc[-3]["Close"]) if len(df) >= 3 else c1

        if direction == "CALL":
            # Reversal = two consecutive down candles after an up move
            if c0 < c1 < c2:
                return False, "bearish reversal detected"
        else:
            # Reversal = two consecutive up candles after a down move
            if c0 > c1 > c2:
                return False, "bullish reversal detected"

        return True, "no reversal"
    except Exception as exc:
        logger.debug("_no_dangerous_reversal error: %s", exc)
        return True, "reversal check skipped"


# ---------------------------------------------------------------------------
# 2. Momentum check
# ---------------------------------------------------------------------------

def _has_sufficient_momentum(df: pd.DataFrame, direction: str) -> tuple[bool, str]:
    """Require at least *some* movement — reject only completely flat markets."""
    try:
        last = df.iloc[-1]
        atr = float(last["ATR"])
        if pd.isna(atr) or atr == 0:
            return True, "momentum check skipped"

        # Close must have moved at least 10% of ATR from the previous close
        momentum = abs(float(last["Close"]) - float(df.iloc[-2]["Close"]))
        min_momentum = atr * 0.10

        if momentum < min_momentum:
            return False, f"momentum too low ({momentum:.5f} < {min_momentum:.5f})"

        return True, "momentum ok"
    except Exception as exc:
        logger.debug("_has_sufficient_momentum error: %s", exc)
        return True, "momentum check skipped"


# ---------------------------------------------------------------------------
# 3. Volatility acceptability
# ---------------------------------------------------------------------------

def _acceptable_volatility(df: pd.DataFrame) -> tuple[bool, str]:
    """Block extreme spikes (>3× mean ATR) and dead flat markets (<0.3× mean ATR)."""
    try:
        atr = float(df.iloc[-1]["ATR"])
        atr_mean = float(df["ATR"].tail(20).mean())

        if pd.isna(atr) or pd.isna(atr_mean) or atr_mean == 0:
            return True, "volatility check skipped"

        ratio = atr / atr_mean

        if ratio > 3.0:
            return False, f"volatility spike (ATR×{ratio:.1f})"
        if ratio < 0.3:
            return False, f"dead market (ATR×{ratio:.1f})"

        return True, f"volatility ok (ATR×{ratio:.1f})"
    except Exception as exc:
        logger.debug("_acceptable_volatility error: %s", exc)
        return True, "volatility check skipped"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def is_mg_safe(df: pd.DataFrame, direction: str) -> tuple[bool, str]:
    """Return (True, reason) when MG entry is safe to proceed.

    Only blocks on: reversal, no momentum, or extreme volatility.
    Does NOT re-check EMA alignment, RSI levels, or candle sizes.
    """
    if df is None or len(df) < 10:
        return False, "insufficient data"

    reversal_ok, rev_reason = _no_dangerous_reversal(df, direction)
    if not reversal_ok:
        return False, rev_reason

    momentum_ok, mom_reason = _has_sufficient_momentum(df, direction)
    if not momentum_ok:
        return False, mom_reason

    vol_ok, vol_reason = _acceptable_volatility(df)
    if not vol_ok:
        return False, vol_reason

    return True, "mg safe"


def mg_confidence_adjustment(df: pd.DataFrame, direction: str) -> int:
    """Return a small confidence bonus/penalty based on MG-specific conditions.

    Range: -5 to +5.  Never used to fully reject a trade.
    """
    if df is None or len(df) < 5:
        return 0

    adj = 0

    try:
        # Bonus: trend structure supports direction
        last = df.iloc[-1]
        if direction == "CALL":
            if float(last["EMA50"]) > float(last["EMA200"]):
                adj += 3
        else:
            if float(last["EMA50"]) < float(last["EMA200"]):
                adj += 3
    except Exception:
        pass

    try:
        # Small penalty if RSI is approaching extreme opposite
        rsi = float(df.iloc[-1]["RSI"])
        if direction == "CALL" and rsi > 75:
            adj -= 3
        elif direction == "PUT" and rsi < 25:
            adj -= 3
    except Exception:
        pass

    return max(-5, min(5, adj))
