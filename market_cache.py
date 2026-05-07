import time
from datetime import timedelta

import pandas as pd
import requests
import os

# Keep only this many candles to limit memory and file sizes
MAX_CANDLES = 3000


# Load API key like the rest of the project
if os.getenv("RAILWAY_ENVIRONMENT"):
    from config_prod import TD_API_KEY
else:
    from config_local import TD_API_KEY


API_CALL_TIMES = []
API_CALLS_PER_MINUTE_LIMIT = 6


def _track_api_call():
    now = time.time()
    API_CALL_TIMES.append(now)
    API_CALL_TIMES[:] = [t for t in API_CALL_TIMES if now - t < 60]


def _can_call_api():
    now = time.time()
    recent_calls = [t for t in API_CALL_TIMES if now - t < 60]
    return len(recent_calls) < API_CALLS_PER_MINUTE_LIMIT


# Module-level cache
cached_5m_df = None
cached_1m_df = None
last_5m_update = None
last_1m_update = None

# Keep last successful snapshots as fallback when API fails
last_successful_5m = None
last_successful_1m = None


def _fetch_from_api(pair, interval):
    if not TD_API_KEY:
        raise ValueError("TD_API_KEY is missing or empty")

    if not _can_call_api():
        # Rate limit reached; do not attempt call
        return None

    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": pair,
        "interval": interval,
        "apikey": TD_API_KEY,
        "outputsize": 200,
    }

    try:
        res = requests.get(url, params=params, timeout=20).json()
    except Exception as e:
        print("market_cache: network error fetching", interval, e)
        return None

    _track_api_call()

    if "values" not in res:
        print("market_cache: API error", res)
        # If rate limited, give the caller a hint by sleeping here
        if res.get("code") == 429:
            try:
                time.sleep(65)
            except Exception:
                pass
        return None

    df = pd.DataFrame(res["values"])

    if "datetime" in df.columns:
        df["CandleTime"] = pd.to_datetime(df["datetime"])  # keep original

    price_columns = ["open", "high", "low", "close"]
    for col in price_columns:
        if col in df.columns:
            df[col] = df[col].astype(float)

    df = df.iloc[::-1]
    df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
    }, inplace=True)

    # Trim to a rolling window to keep memory bounded
    try:
        df = df.tail(MAX_CANDLES)
    except Exception:
        pass

    return df


def get_5m_df(pair="EUR/USD"):
    """Return a cached 5min dataframe. Fetches only once per 5-minute candle.

    On API failure the last successful dataframe is returned if available.
    """
    global cached_5m_df, last_5m_update, last_successful_5m

    now = pd.Timestamp.now(tz="Asia/Kolkata")
    candle_key = now.floor("5min")

    if last_5m_update is not None and candle_key == last_5m_update and cached_5m_df is not None:
        return cached_5m_df.copy()

    # Attempt fetch
    new_df = _fetch_from_api(pair, "5min")

    if new_df is not None and len(new_df) > 0:
        # ensure rolling window when caching
        try:
            new_df = new_df.tail(MAX_CANDLES)
        except Exception:
            pass
        cached_5m_df = new_df.copy()
        last_5m_update = candle_key
        last_successful_5m = cached_5m_df.copy()
        return cached_5m_df.copy()

    # Fallback to last successful
    if last_successful_5m is not None:
        print("market_cache: using last successful 5m data as fallback")
        return last_successful_5m.copy()

    return None


def get_1m_df(pair="EUR/USD"):
    """Return a cached 1min dataframe. Fetches only once per minute.

    On API failure the last successful dataframe is returned if available.
    """
    global cached_1m_df, last_1m_update, last_successful_1m

    now = pd.Timestamp.now(tz="Asia/Kolkata")
    minute_key = now.floor("min")

    if last_1m_update is not None and minute_key == last_1m_update and cached_1m_df is not None:
        return cached_1m_df.copy()

    new_df = _fetch_from_api(pair, "1min")

    if new_df is not None and len(new_df) > 0:
        try:
            new_df = new_df.tail(MAX_CANDLES)
        except Exception:
            pass
        cached_1m_df = new_df.copy()
        last_1m_update = minute_key
        last_successful_1m = cached_1m_df.copy()
        return cached_1m_df.copy()

    if last_successful_1m is not None:
        print("market_cache: using last successful 1m data as fallback")
        return last_successful_1m.copy()

    return None


def force_clear_cache():
    global cached_5m_df, cached_1m_df, last_5m_update, last_1m_update
    cached_5m_df = None
    cached_1m_df = None
    last_5m_update = None
    last_1m_update = None
