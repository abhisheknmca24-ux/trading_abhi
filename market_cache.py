"""market_cache.py — Centralised indicator cache for 1-minute OHLCV data.

Prevents repeated recalculation of EMA / RSI / ATR across multiple validation
layers on every bot cycle.

Usage
-----
    from market_cache import get_cached_1m_df, set_cached_1m_df

    # Fetched once per minute in bot.py / signal_list.py:
    raw_df = fetch_1m_data()
    set_cached_1m_df(raw_df)

    # Every other module reads the already-processed copy:
    df = get_cached_1m_df()   # EMA50/200, RSI, ATR already attached
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal state
# ---------------------------------------------------------------------------
_cached_df: Optional[pd.DataFrame] = None
_cached_at: float = 0.0          # Unix timestamp of last update
_cache_ttl_seconds: float = 75.0  # Consider stale after ~1 candle

# Diagnostics
_cache_hits: int = 0
_cache_misses: int = 0
_refresh_count: int = 0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def set_cached_1m_df(raw_df: Optional[pd.DataFrame]) -> None:
    """Store a fresh 1-minute dataframe (indicators will be added lazily)."""
    global _cached_df, _cached_at, _refresh_count

    if raw_df is None or len(raw_df) < 20:
        logger.debug("market_cache: received empty/small df — not cached")
        return

    # Import here to avoid circular imports at module level
    from indicators import add_indicators

    try:
        _cached_df = add_indicators(raw_df.copy())
        _cached_at = time.monotonic()
        _refresh_count += 1
        logger.debug("market_cache: refreshed 1m df (%d rows)", len(_cached_df))
    except Exception as exc:
        logger.warning("market_cache: indicator calculation failed — %s", exc)
        _cached_df = None


def get_cached_1m_df(max_age_seconds: Optional[float] = None) -> Optional[pd.DataFrame]:
    """Return the cached 1-minute dataframe (with indicators), or None if stale/empty."""
    global _cache_hits, _cache_misses

    if _cached_df is None:
        _cache_misses += 1
        logger.debug("market_cache: miss (no data)")
        return None

    age = time.monotonic() - _cached_at
    ttl = max_age_seconds if max_age_seconds is not None else _cache_ttl_seconds

    if age > ttl:
        _cache_misses += 1
        logger.debug("market_cache: miss (stale %.1fs > %.1fs)", age, ttl)
        return None

    _cache_hits += 1
    logger.debug("market_cache: hit (age %.1fs)", age)
    return _cached_df


def invalidate_cache() -> None:
    """Force the next call to get_cached_1m_df to return None."""
    global _cached_df, _cached_at
    _cached_df = None
    _cached_at = 0.0
    logger.debug("market_cache: invalidated")


def get_diagnostics() -> dict:
    """Return cache health metrics."""
    age = time.monotonic() - _cached_at if _cached_at > 0 else None
    total = _cache_hits + _cache_misses
    hit_rate = (_cache_hits / total * 100) if total > 0 else 0.0
    return {
        "hits": _cache_hits,
        "misses": _cache_misses,
        "hit_rate_pct": round(hit_rate, 1),
        "refreshes": _refresh_count,
        "cache_age_seconds": round(age, 1) if age is not None else None,
        "has_data": _cached_df is not None,
        "rows": len(_cached_df) if _cached_df is not None else 0,
    }
