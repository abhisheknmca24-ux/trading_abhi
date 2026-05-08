"""
Market Cache Manager - Optimized for single computation
Fetches market data once, computes indicators once, reuses processed_df everywhere.
Keeps 2000-3000 candles to maintain memory efficiency.
"""

import os
import time
import pandas as pd
import requests
import hashlib
from datetime import datetime
from typing import Optional, Tuple

if os.getenv("RAILWAY_ENVIRONMENT"):
    from config_prod import BOT_TOKEN, CHAT_ID, TD_API_KEY
else:
    from config_local import BOT_TOKEN, CHAT_ID, TD_API_KEY

from indicators import add_indicators

PAIR = "EUR/USD"
MIN_CANDLES = 2000
MAX_CANDLES = 3000

# ===== CACHING STATE =====
cached_5m_df = None
cached_1m_df = None
processed_df = None  # SINGLE SOURCE OF TRUTH for processed 5m data
processed_df_hash = None  # Track if data changed

last_5m_fetch_time = None
last_1m_fetch_time = None
last_processed_update_time = None

API_CALL_TIMES = []
API_CALLS_PER_MINUTE_LIMIT = 6

# ===== API FAILURE SAFETY MODE =====
_api_failure_count = 0          # Consecutive failure counter
_api_failure_threshold = 3      # Alert + pause after this many consecutive failures
_api_safety_mode_active = False # When True, skip all trades
_api_safety_mode_since = None   # Timestamp when safety mode activated
_api_safety_alert_sent = False  # Avoid spamming Telegram


def _send_api_failure_alert(failure_count: int) -> None:
    """Send Telegram alert when API fails consecutively."""
    try:
        msg = (
            f"🚨 *API SAFETY MODE ACTIVATED*\n\n"
            f"Market data API has failed *{failure_count} consecutive times*.\n\n"
            f"⛔ Trading is now PAUSED automatically.\n"
            f"📡 Will retry every 5 minutes.\n"
            f"🔄 Will resume when data is restored."
        )
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=8)
    except Exception as e:
        print(f"Failed to send API failure alert: {e}")


def _send_api_recovery_alert() -> None:
    """Send Telegram alert when API recovers from safety mode."""
    try:
        msg = (
            f"✅ *API Recovered — Trading Resumed*\n\n"
            f"Market data feed restored successfully.\n"
            f"Bot is back to normal operation."
        )
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=8)
    except Exception as e:
        print(f"Failed to send API recovery alert: {e}")


def is_api_safety_mode_active() -> bool:
    """Return True if API safety mode is active (trading should be paused)."""
    return _api_safety_mode_active


def _record_api_success() -> None:
    """Called when an API call succeeds — resets failure counter."""
    global _api_failure_count, _api_safety_mode_active, _api_safety_alert_sent, _api_safety_mode_since
    was_in_safety_mode = _api_safety_mode_active
    _api_failure_count = 0
    _api_safety_mode_active = False
    _api_safety_alert_sent = False
    _api_safety_mode_since = None
    if was_in_safety_mode:
        print("✅ API recovered — safety mode deactivated.")
        _send_api_recovery_alert()


def _record_api_failure() -> None:
    """Called when an API call fails — increments counter and activates safety mode."""
    global _api_failure_count, _api_safety_mode_active, _api_safety_alert_sent, _api_safety_mode_since
    _api_failure_count += 1
    print(f"⚠️ API failure #{_api_failure_count}")
    if _api_failure_count >= _api_failure_threshold:
        _api_safety_mode_active = True
        if _api_safety_mode_since is None:
            _api_safety_mode_since = datetime.now()
        if not _api_safety_alert_sent:
            _api_safety_alert_sent = True
            _send_api_failure_alert(_api_failure_count)



def track_api_call():
    """Track API call times for rate limiting."""
    global API_CALL_TIMES
    now = time.time()
    API_CALL_TIMES.append(now)
    API_CALL_TIMES = [t for t in API_CALL_TIMES if now - t < 60]


def is_api_call_allowed() -> bool:
    """Check if API call is allowed under rate limit."""
    global API_CALL_TIMES
    now = time.time()
    recent_calls = [t for t in API_CALL_TIMES if now - t < 60]
    return len(recent_calls) < API_CALLS_PER_MINUTE_LIMIT


# ===== DATA FETCHING =====

def fetch_data(interval: str) -> Optional[pd.DataFrame]:
    """
    Fetch data from API, update global cache, and return the dataframe.
    
    Args:
        interval: Either "5min" or "1min"
    
    Returns:
        DataFrame with OHLC data or None if fetch fails
    """
    global cached_5m_df, cached_1m_df, last_5m_fetch_time, last_1m_fetch_time
    
    now_key = pd.Timestamp.now(tz="Asia/Kolkata").floor(interval)
    
    # Check if we already fetched for this candle
    if interval == "5min":
        if last_5m_fetch_time == now_key and cached_5m_df is not None:
            return cached_5m_df.copy()
    else:
        if last_1m_fetch_time == now_key and cached_1m_df is not None:
            return cached_1m_df.copy()

    # Rate limit check
    if not is_api_call_allowed():
        if interval == "5min":
            return cached_5m_df.copy() if cached_5m_df is not None else None
        else:
            return cached_1m_df.copy() if cached_1m_df is not None else None

    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": PAIR,
        "interval": interval,
        "apikey": TD_API_KEY,
        "outputsize": 200
    }

    try:
        res = requests.get(url, params=params, timeout=15).json()
        track_api_call()

        if "values" not in res:
            print(f"[Cache] API ERROR for {interval}:", res)
            if res.get("code") == 429:
                print("[Cache] Rate limit hit")
            _record_api_failure()  # ← track failure
            if interval == "5min":
                return cached_5m_df.copy() if cached_5m_df is not None else None
            else:
                return cached_1m_df.copy() if cached_1m_df is not None else None

        # Parse response
        df = pd.DataFrame(res["values"])

        if "datetime" in df.columns:
            df["CandleTime"] = pd.to_datetime(df["datetime"])

        price_columns = ["open", "high", "low", "close"]
        df[price_columns] = df[price_columns].astype(float)
        df = df.iloc[::-1].reset_index(drop=True)

        df.rename(columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close"
        }, inplace=True)

        # Merge with existing cache and keep only recent candles
        if interval == "5min":
            if cached_5m_df is not None:
                merged = pd.concat([cached_5m_df, df]).drop_duplicates(subset=["CandleTime"], keep="last")
            else:
                merged = df
            
            # Keep between MIN_CANDLES and MAX_CANDLES
            merged = merged.sort_values(by="CandleTime").tail(MAX_CANDLES).reset_index(drop=True)
            if len(merged) > MAX_CANDLES:
                merged = merged.tail(MAX_CANDLES).reset_index(drop=True)
            
            cached_5m_df = merged
            last_5m_fetch_time = now_key
            _record_api_success()  # ← reset failure counter
            return cached_5m_df.copy()
        else:
            if cached_1m_df is not None:
                merged = pd.concat([cached_1m_df, df]).drop_duplicates(subset=["CandleTime"], keep="last")
            else:
                merged = df
            
            merged = merged.sort_values(by="CandleTime").tail(MAX_CANDLES).reset_index(drop=True)
            if len(merged) > MAX_CANDLES:
                merged = merged.tail(MAX_CANDLES).reset_index(drop=True)
            
            cached_1m_df = merged
            last_1m_fetch_time = now_key
            _record_api_success()  # ← reset failure counter
            return cached_1m_df.copy()

    except Exception as e:
        print(f"[Cache] Error fetching {interval} data:", e)
        _record_api_failure()  # ← track failure
        if interval == "5min":
            return cached_5m_df.copy() if cached_5m_df is not None else None
        else:
            return cached_1m_df.copy() if cached_1m_df is not None else None


# ===== PROCESSED DATA CACHING =====

def _compute_data_hash(df: pd.DataFrame) -> str:
    """Compute hash of dataframe to detect changes."""
    if df is None or len(df) == 0:
        return ""
    
    # Hash the last few rows to detect new candles
    key_data = df[['CandleTime', 'Open', 'High', 'Low', 'Close']].tail(10).to_string()
    return hashlib.md5(key_data.encode()).hexdigest()


def update_processed_df() -> Optional[pd.DataFrame]:
    """
    Fetch 5m data and compute indicators ONCE.
    This is the single source of truth for processed data.
    
    Returns:
        Processed dataframe with indicators or None if no data
    """
    global processed_df, processed_df_hash, last_processed_update_time
    
    # Get raw 5m data
    raw_df = get_5m_data()
    
    if raw_df is None or len(raw_df) == 0:
        return None
    
    # Check if data changed using hash
    current_hash = _compute_data_hash(raw_df)
    
    if processed_df is not None and current_hash == processed_df_hash:
        # Data hasn't changed, no need to recompute
        return processed_df.copy()
    
    # Data changed, recompute indicators ONCE
    print("[Cache] Computing indicators (new data detected)")
    
    try:
        # Add indicators to the raw data
        processed_df = add_indicators(raw_df.copy())
        processed_df_hash = current_hash
        last_processed_update_time = datetime.now()
        
        return processed_df.copy()
    except Exception as e:
        print(f"[Cache] Error computing indicators: {e}")
        return None


def get_5m_data() -> Optional[pd.DataFrame]:
    """Get raw 5m OHLC data."""
    return fetch_data("5min")


def get_1m_data() -> Optional[pd.DataFrame]:
    """Get raw 1m OHLC data."""
    return fetch_data("1min")


def get_data(interval: str) -> Optional[pd.DataFrame]:
    """
    Get raw market data.
    
    Args:
        interval: "5min" or "1min"
    
    Returns:
        DataFrame with OHLC data
    """
    if interval == "5min":
        return get_5m_data()
    return get_1m_data()


def get_processed_df(force_update: bool = False) -> Optional[pd.DataFrame]:
    """
    Get processed dataframe with indicators.
    SINGLE SOURCE OF TRUTH for all analysis.
    
    Args:
        force_update: Force recomputation even if data unchanged
    
    Returns:
        DataFrame with OHLC + indicators or None
    """
    global processed_df
    
    if force_update or processed_df is None:
        return update_processed_df()
    
    # Return cached processed data
    return processed_df.copy() if processed_df is not None else None


def set_processed_df(df: Optional[pd.DataFrame]) -> None:
    """
    Explicitly set processed dataframe (used for testing).
    
    Args:
        df: DataFrame to set or None to clear
    """
    global processed_df, processed_df_hash
    
    processed_df = df.copy() if df is not None else None
    processed_df_hash = _compute_data_hash(df) if df is not None else None


def get_cache_stats() -> dict:
    """Get cache statistics for debugging."""
    stats = {
        'cached_5m_candles': len(cached_5m_df) if cached_5m_df is not None else 0,
        'cached_1m_candles': len(cached_1m_df) if cached_1m_df is not None else 0,
        'processed_df_candles': len(processed_df) if processed_df is not None else 0,
        'last_5m_fetch': last_5m_fetch_time,
        'last_1m_fetch': last_1m_fetch_time,
        'last_processed_update': last_processed_update_time,
        'api_calls_in_minute': len(API_CALL_TIMES),
    }
    return stats
