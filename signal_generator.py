<<<<<<< HEAD
"""Analyze historical EURUSD 5-minute data and generate recurring signal timings."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

import pandas as pd
import requests

from indicators import add_indicators

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

TIMEZONE_NAME = "Asia/Kolkata"
GENERATED_SIGNALS_FILE = "generated_signals.json"
LAST_GENERATION_FILE = "last_generation_date.txt"
DATA_DAYS_LOOKBACK = 14
MIN_DATA_DAYS = 7
MAX_SIGNALS = 15
MIN_SIGNALS = 5
MIN_CONFIDENCE = 70
API_URL = "https://api.twelvedata.com/time_series"
OUTPUT_SIZE = 5000


def _tz():
    return ZoneInfo(TIMEZONE_NAME) if ZoneInfo else None


def _now() -> datetime:
    return datetime.now(_tz())


def _today() -> date:
    return _now().date()


def _is_generated_today() -> bool:
    if not os.path.exists(LAST_GENERATION_FILE):
        return False
    try:
        with open(LAST_GENERATION_FILE) as f:
            return f.read().strip() == _today().isoformat()
    except Exception:
        return False


def _mark_generated_today():
    with open(LAST_GENERATION_FILE, "w") as f:
        f.write(_today().isoformat())


def _load_generated_signals() -> List[dict]:
    if not os.path.exists(GENERATED_SIGNALS_FILE):
        return []
    try:
        with open(GENERATED_SIGNALS_FILE) as f:
            return json.load(f)
    except Exception:
        return []


def _save_generated_signals(signals: List[dict]):
    with open(GENERATED_SIGNALS_FILE, "w") as f:
        json.dump(signals, f, indent=2)


def fetch_historical_data(api_key: str) -> Optional[pd.DataFrame]:
    params = {
        "symbol": "EUR/USD",
        "interval": "5min",
        "apikey": api_key,
        "outputsize": OUTPUT_SIZE,
    }
    res = requests.get(API_URL, params=params)
    data = res.json()

    if "values" not in data:
        print("SignalGenerator API error:", data.get("message", data))
        return None

    df = pd.DataFrame(data["values"])
    df["datetime"] = pd.to_datetime(df["datetime"])

    for col in ["open", "high", "low", "close"]:
        df[col] = df[col].astype(float)

    df = df.sort_values("datetime").reset_index(drop=True)
    df.rename(
        columns={
            "datetime": "CandleTime",
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
        },
        inplace=True,
    )
    return df


def _slot_key(candle_time: pd.Timestamp) -> str:
    return candle_time.strftime("%H:%M")


def _get_market_slots() -> List[str]:
    slots = []
    for hour in range(13, 22):
        for minute in range(0, 60, 5):
            if hour == 13 and minute < 30:
                continue
            if hour == 21 and minute > 30:
                continue
            slots.append(f"{hour:02d}:{minute:02d}")
    return slots


def _score_candle(row: pd.Series, prev_row: pd.Series, direction: str) -> int:
    score = 0

    if pd.isna(row.get("EMA50")) or pd.isna(row.get("EMA200")):
        return 0
    if pd.isna(row.get("RSI")) or pd.isna(prev_row.get("RSI")):
        return 0
    if pd.isna(row.get("ATR")):
        return 0

    atr = float(row["ATR"])
    atr_mean = float(row.get("_atr_mean", atr))

    if direction == "CALL":
        if row["EMA50"] > row["EMA200"]:
            score += 25
        rsi = float(row["RSI"])
        if rsi > 55 and rsi > float(prev_row["RSI"]):
            score += 20
        if float(row["Close"]) > float(prev_row["Close"]):
            score += 15
    else:
        if row["EMA50"] < row["EMA200"]:
            score += 25
        rsi = float(row["RSI"])
        if rsi < 45 and rsi < float(prev_row["RSI"]):
            score += 20
        if float(row["Close"]) < float(prev_row["Close"]):
            score += 15

    if atr > atr_mean:
        score += 20

    candle_size = abs(float(row["Close"]) - float(row["Open"]))
    avg_candle = float(row.get("_avg_candle_size", candle_size))
    if candle_size > avg_candle:
        score += 20

    return min(score, 100)


def _is_bullish(row: pd.Series, prev_row: pd.Series) -> bool:
    if pd.isna(row.get("EMA50")) or pd.isna(row.get("EMA200")):
        return False
    if pd.isna(row.get("RSI")) or pd.isna(prev_row.get("RSI")):
        return False
    return (
        row["EMA50"] > row["EMA200"]
        and float(row["RSI"]) > float(prev_row["RSI"])
        and float(row["RSI"]) > 50
    )


def _is_bearish(row: pd.Series, prev_row: pd.Series) -> bool:
    if pd.isna(row.get("EMA50")) or pd.isna(row.get("EMA200")):
        return False
    if pd.isna(row.get("RSI")) or pd.isna(prev_row.get("RSI")):
        return False
    return (
        row["EMA50"] < row["EMA200"]
        and float(row["RSI"]) < float(prev_row["RSI"])
        and float(row["RSI"]) < 50
    )


def _calculate_slot_stats(slot_df: pd.DataFrame, atr_mean: float, avg_candle_size: float) -> dict:
    slot_df = slot_df.copy()
    slot_df["_atr_mean"] = atr_mean
    slot_df["_avg_candle_size"] = avg_candle_size

    total = len(slot_df)
    if total < MIN_DATA_DAYS:
        return {
            "total_occurrences": total,
            "bullish_count": 0,
            "bearish_count": 0,
            "bullish_win_rate": 0.0,
            "bearish_win_rate": 0.0,
            "avg_bullish_score": 0,
            "avg_bearish_score": 0,
            "confidence": 0,
            "dominant_direction": None,
        }

    bullish_count = 0
    bearish_count = 0
    bullish_scores = []
    bearish_scores = []

    for i in range(1, len(slot_df)):
        row = slot_df.iloc[i]
        prev = slot_df.iloc[i - 1]

        if _is_bullish(row, prev):
            bullish_count += 1
            bullish_scores.append(_score_candle(row, prev, "CALL"))

        if _is_bearish(row, prev):
            bearish_count += 1
            bearish_scores.append(_score_candle(row, prev, "PUT"))

    bullish_win_rate = (bullish_count / total) * 100 if total else 0.0
    bearish_win_rate = (bearish_count / total) * 100 if total else 0.0
    avg_bullish_score = int(sum(bullish_scores) / len(bullish_scores)) if bullish_scores else 0
    avg_bearish_score = int(sum(bearish_scores) / len(bearish_scores)) if bearish_scores else 0

    if bullish_win_rate > bearish_win_rate and bullish_count > 0:
        dominant = "CALL"
        confidence = int(bullish_win_rate * (avg_bullish_score / 100))
    elif bearish_win_rate > bullish_win_rate and bearish_count > 0:
        dominant = "PUT"
        confidence = int(bearish_win_rate * (avg_bearish_score / 100))
    else:
        dominant = None
        confidence = 0

    return {
        "total_occurrences": total,
        "bullish_count": bullish_count,
        "bearish_count": bearish_count,
        "bullish_win_rate": round(bullish_win_rate, 1),
        "bearish_win_rate": round(bearish_win_rate, 1),
        "avg_bullish_score": avg_bullish_score,
        "avg_bearish_score": avg_bearish_score,
        "confidence": min(confidence, 100),
        "dominant_direction": dominant,
    }


def generate_signals(api_key: str) -> List[dict]:
    if _is_generated_today():
        print("Signals already generated today. Loading from file.")
        return _load_generated_signals()

    print("Fetching historical 5-minute data...")
    df = fetch_historical_data(api_key)
    if df is None or len(df) < 200:
        print("Insufficient historical data.")
        return []

    df = add_indicators(df)
    df = df.dropna(subset=["EMA50", "EMA200", "RSI", "ATR"]).reset_index(drop=True)

    cutoff = _now() - timedelta(days=DATA_DAYS_LOOKBACK)
    if df["CandleTime"].min() > cutoff:
        print(f"Warning: Only {int((_now() - df['CandleTime'].min()).days)} days of data available.")

    df = df[df["CandleTime"] >= cutoff].copy()

    global_atr_mean = df["ATR"].mean()
    global_avg_candle = (df["Close"] - df["Open"]).abs().mean()

    df["SlotKey"] = df["CandleTime"].apply(_slot_key)
    market_slots = _get_market_slots()

    candidates = []
    for slot in market_slots:
        slot_df = df[df["SlotKey"] == slot].copy()
        if len(slot_df) < MIN_DATA_DAYS:
            continue

        stats = _calculate_slot_stats(slot_df, global_atr_mean, global_avg_candle)
        if stats["dominant_direction"] is None or stats["confidence"] < MIN_CONFIDENCE:
            continue

        candidates.append(
            {
                "time": slot,
                "pair": "EURUSD",
                "direction": stats["dominant_direction"],
                "confidence": stats["confidence"],
                "_bullish_win_rate": stats["bullish_win_rate"],
                "_bearish_win_rate": stats["bearish_win_rate"],
                "_occurrences": stats["total_occurrences"],
            }
        )

    candidates.sort(key=lambda x: x["confidence"], reverse=True)
    selected = candidates[:MAX_SIGNALS]

    if len(selected) < MIN_SIGNALS and candidates:
        selected = candidates[:MIN_SIGNALS]

    signals = [
        {
            "time": s["time"],
            "pair": "EURUSD",
            "direction": s["direction"],
            "confidence": s["confidence"],
        }
        for s in selected
    ]

    _save_generated_signals(signals)
    _mark_generated_today()
    print(f"Generated {len(signals)} signals for {_today()}.")
    return signals


def get_signals() -> List[dict]:
    return _load_generated_signals()


if __name__ == "__main__":
    from config_local import TD_API_KEY
    sigs = generate_signals(TD_API_KEY)
    print(json.dumps(sigs, indent=2))
=======
from logger import logger
import os
import json
import time
import pandas as pd
import requests
from datetime import datetime, timedelta
from indicators import add_indicators
from learning_engine import learning_engine

# Configuration
PAIR = "EURUSD"
SYMBOL = "EUR/USD"
INTERVAL = "5min"
SIGNAL_FILE = "generated_signals.json"
STATE_FILE = ".generator_state.json"
DF_CACHE_FILE = ".df_cache.pkl"

# Force daily signal times (IST)
FORCE_DIRECT_TIME = "15:05"
FORCE_MARTINGALE_TIME = "15:10"
FORCE_SIGNAL_CONFIDENCE_THRESHOLD = 55  # Below this → LOW CONFIDENCE warning

# Load TD_API_KEY
if os.getenv("RAILWAY_ENVIRONMENT"):
    from config_prod import TD_API_KEY
else:
    try:
        from config_local import TD_API_KEY
    except ImportError:
        TD_API_KEY = os.getenv("TD_API_KEY")

# ──────────────────────────────────────────────────────────────
# DataFrame cache (fallback when API unavailable)
# ──────────────────────────────────────────────────────────────
_df_memory_cache: pd.DataFrame | None = None


def _save_df_cache(df: pd.DataFrame) -> None:
    """Persist DataFrame to disk so next API failure can use it."""
    global _df_memory_cache
    _df_memory_cache = df
    try:
        df.to_pickle(DF_CACHE_FILE)
    except Exception as e:
        logger.warning(f"DF cache save failed: {e}")


def _load_df_cache() -> pd.DataFrame | None:
    """Load the latest cached DataFrame (memory first, then disk)."""
    global _df_memory_cache
    if _df_memory_cache is not None:
        return _df_memory_cache
    if os.path.exists(DF_CACHE_FILE):
        try:
            df = pd.read_pickle(DF_CACHE_FILE)
            _df_memory_cache = df
            logger.info("Loaded DataFrame from disk cache.")
            return df
        except Exception as e:
            logger.warning(f"DF cache load failed: {e}")
    return None


# ──────────────────────────────────────────────────────────────
# Data fetching
# ──────────────────────────────────────────────────────────────

def get_historical_data(outputsize=4500) -> pd.DataFrame | None:
    """Fetch 10-14 days of 5-minute candles from TwelveData.
    Falls back to the latest cached DataFrame on API failure.
    """
    if not TD_API_KEY:
        logger.error("Error: TD_API_KEY not found.")
        return _load_df_cache()

    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": SYMBOL,
        "interval": INTERVAL,
        "apikey": TD_API_KEY,
        "outputsize": outputsize
    }

    try:
        res = requests.get(url, params=params, timeout=15).json()
        if "values" not in res:
            logger.error(f"API Error: {res}")
            return _load_df_cache()

        df = pd.DataFrame(res["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])

        df.rename(columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close"
        }, inplace=True)

        price_columns = ["Open", "High", "Low", "Close"]
        df[price_columns] = df[price_columns].astype(float)

        df = df.sort_values("datetime").reset_index(drop=True)
        df = add_indicators(df)

        df["TimeOfDay"] = df["datetime"].dt.strftime("%H:%M")

        # Candle Result (Target for win rate)
        df["Result_CALL"] = (df["Close"] > df["Open"]).astype(int)
        df["Result_PUT"] = (df["Close"] < df["Open"]).astype(int)

        # EMA Trend Consistency
        df["EMA_Trend_CALL"] = (df["EMA50"] > df["EMA200"]).astype(int)
        df["EMA_Trend_PUT"] = (df["EMA50"] < df["EMA200"]).astype(int)

        # RSI Continuation
        df["RSI_Cont_CALL"] = (df["RSI"] > 50).astype(int)
        df["RSI_Cont_PUT"] = (df["RSI"] < 50).astype(int)

        # Candle Strength
        df["Body"] = (df["Close"] - df["Open"]).abs()
        df["Range"] = (df["High"] - df["Low"]).replace(0, 0.00001)
        df["Strength"] = df["Body"] / df["Range"]

        # Momentum Continuation
        df["Mom_Cont_CALL"] = (df["Close"] > df.shift(1)["Close"]).astype(int)
        df["Mom_Cont_PUT"] = (df["Close"] < df.shift(1)["Close"]).astype(int)

        _save_df_cache(df)
        return df

    except Exception as e:
        logger.error(f"Fetch Error: {e}")
        cached = _load_df_cache()
        if cached is not None:
            logger.info("Using cached DataFrame due to fetch error.")
        return cached


# ──────────────────────────────────────────────────────────────
# Live direction decision
# ──────────────────────────────────────────────────────────────

def decide_direction_live(df: pd.DataFrame) -> tuple[str, int]:
    """
    Dynamically decide CALL or PUT using the latest candle data.
    Uses EMA trend, RSI momentum, ATR strength, and candle momentum.

    Returns (direction, confidence) where confidence is 0-100.
    Never returns a hardcoded direction – always calculated from live data.
    """
    if df is None or len(df) < 50:
        # Absolute fallback – market data unavailable; default to neutral guess
        logger.warning("Insufficient data for live direction decision; defaulting CALL.")
        return "CALL", 40

    last = df.iloc[-1]
    prev = df.iloc[-2] if len(df) >= 2 else last

    call_score = 0
    put_score = 0

    # --- 1. EMA Trend (weight: 35) ---
    try:
        if not pd.isna(last["EMA50"]) and not pd.isna(last["EMA200"]):
            if last["EMA50"] > last["EMA200"]:
                call_score += 35
            else:
                put_score += 35
    except Exception:
        pass

    # --- 2. RSI Momentum (weight: 25) ---
    try:
        rsi = float(last["RSI"]) if not pd.isna(last["RSI"]) else 50.0
        rsi_prev = float(prev["RSI"]) if not pd.isna(prev["RSI"]) else 50.0
        if rsi > 50 and rsi > rsi_prev:
            call_score += 25
        elif rsi < 50 and rsi < rsi_prev:
            put_score += 25
        elif rsi > 50:
            call_score += 12
        else:
            put_score += 12
    except Exception:
        pass

    # --- 3. ATR Strength – bonus for active market (weight: 15) ---
    try:
        atr = float(last["ATR"]) if not pd.isna(last["ATR"]) else 0
        atr_mean = float(df["ATR"].mean()) if not pd.isna(df["ATR"].mean()) else 0
        if atr > atr_mean:
            # ATR boost goes to whatever side is winning
            if call_score >= put_score:
                call_score += 15
            else:
                put_score += 15
    except Exception:
        pass

    # --- 4. Latest candle momentum (weight: 15) ---
    try:
        close = float(last["Close"])
        open_ = float(last["Open"])
        prev_close = float(prev["Close"])
        if close > open_ and close > prev_close:
            call_score += 15
        elif close < open_ and close < prev_close:
            put_score += 15
        elif close > open_:
            call_score += 7
        else:
            put_score += 7
    except Exception:
        pass

    # --- 5. Price relative to EMA50 (weight: 10) ---
    try:
        ema50 = float(last["EMA50"]) if not pd.isna(last["EMA50"]) else float(last["Close"])
        if float(last["Close"]) > ema50:
            call_score += 10
        else:
            put_score += 10
    except Exception:
        pass

    total_possible = 100
    if call_score >= put_score:
        direction = "CALL"
        confidence = int((call_score / total_possible) * 100)
    else:
        direction = "PUT"
        confidence = int((put_score / total_possible) * 100)

    confidence = max(0, min(confidence, 99))
    logger.info(f"Live direction: {direction} | CALL={call_score} PUT={put_score} | Confidence={confidence}%")
    return direction, confidence


# ──────────────────────────────────────────────────────────────
# Signal generation helpers
# ──────────────────────────────────────────────────────────────

def calculate_recurring_strength(df):
    """Analyze recurring timing strength across historical data."""
    if df is None or len(df) < 500:
        return []

    atr_mean = df["ATR"].mean()
    unique_times = df["TimeOfDay"].unique()
    call_candidates = []
    put_candidates = []

    for t in unique_times:
        slot_data = df[df["TimeOfDay"] == t]
        if len(slot_data) < 7:
            continue

        wr_call = slot_data["Result_CALL"].mean() * 100
        wr_put = slot_data["Result_PUT"].mean() * 100
        ema_call = slot_data["EMA_Trend_CALL"].mean() * 100
        ema_put = slot_data["EMA_Trend_PUT"].mean() * 100
        rsi_call = slot_data["RSI_Cont_CALL"].mean() * 100
        rsi_put = slot_data["RSI_Cont_PUT"].mean() * 100
        atr_avg = slot_data["ATR"].mean()
        atr_score = 100 if atr_avg > atr_mean else 60
        rsi_avg = slot_data["RSI"].mean()
        mom_call = slot_data["Mom_Cont_CALL"].mean() * 100
        mom_put = slot_data["Mom_Cont_PUT"].mean() * 100
        str_avg = slot_data["Strength"].mean() * 100

        conf_call = (wr_call * 0.35) + (ema_call * 0.20) + (rsi_call * 0.15) + (atr_score * 0.10) + (mom_call * 0.10) + (str_avg * 0.10)
        conf_put = (wr_put * 0.35) + (ema_put * 0.20) + (rsi_put * 0.15) + (atr_score * 0.10) + (mom_put * 0.10) + (str_avg * 0.10)

        adj_call = learning_engine.get_adaptive_adjustment(t, "CALL", int(conf_call), atr_avg, rsi_avg, source="generated")
        adj_put = learning_engine.get_adaptive_adjustment(t, "PUT", int(conf_put), atr_avg, rsi_avg, source="generated")

        if adj_call <= -3:
            conf_call = 0
        else:
            conf_call += adj_call

        if adj_put <= -3:
            conf_put = 0
        else:
            conf_put += adj_put

        h, m = map(int, t.split(':'))
        utc_minutes = h * 60 + m
        ist_minutes = (utc_minutes + 330) % 1440

        if not (13 * 60 <= ist_minutes <= 22 * 60):
            continue

        if conf_call >= 70:
            call_candidates.append({
                "time": t,
                "pair": PAIR,
                "direction": "CALL",
                "confidence": int(conf_call)
            })

        if conf_put >= 70:
            put_candidates.append({
                "time": t,
                "pair": PAIR,
                "direction": "PUT",
                "confidence": int(conf_put)
            })

    call_candidates.sort(key=lambda x: x["confidence"], reverse=True)
    put_candidates.sort(key=lambda x: x["confidence"], reverse=True)

    total_target = 12
    max_one_side = 8

    calls = call_candidates[:max_one_side]
    puts = put_candidates[:max_one_side]
    combined = calls + puts
    combined.sort(key=lambda x: x["confidence"], reverse=True)

    final_signals = []
    c_count = 0
    p_count = 0

    for s in combined:
        if len(final_signals) >= total_target:
            break
        if s["direction"] == "CALL":
            if c_count < max_one_side:
                final_signals.append(s)
                c_count += 1
        else:
            if p_count < max_one_side:
                final_signals.append(s)
                p_count += 1

    final_signals.sort(key=lambda x: x["time"])
    return final_signals


def has_run_today():
    """Check if the generator already ran today."""
    if not os.path.exists(STATE_FILE):
        return False
    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
            last_run = state.get("last_run_date")
            return last_run == datetime.now().strftime("%Y-%m-%d")
    except Exception:
        return False


def update_run_state():
    """Update the state file with today's date."""
    with open(STATE_FILE, "w") as f:
        json.dump({"last_run_date": datetime.now().strftime("%Y-%m-%d")}, f)


def generate_daily_signals():
    """Main execution function to generate signals once daily."""
    if has_run_today():
        logger.info("Signals already generated for today. Skipping.")
        return False

    logger.info(f"--- Generating Daily Signals for {PAIR} ---")
    df = get_historical_data()

    if df is None:
        logger.error("Failed to fetch data.")
        return False

    signals = calculate_recurring_strength(df)

    if signals:
        with open(SIGNAL_FILE, "w") as f:
            json.dump(signals, f, indent=2)
        logger.info(f"Successfully generated {len(signals)} strong signals.")
        update_run_state()
        return True
    else:
        logger.info("No signals met the confidence threshold today.")
        return False


# ──────────────────────────────────────────────────────────────
# FORCED DAILY SIGNALS  (15:05 direct + 15:10 martingale)
# ──────────────────────────────────────────────────────────────

def generate_forced_daily_signals(df: pd.DataFrame | None = None) -> list[dict]:
    """
    ALWAYS generate two compulsory signals every day:
      • 15:05 IST  – direct / main signal
      • 15:10 IST  – martingale follow-up

    Direction is NEVER hardcoded. It is decided dynamically using the
    latest candle's EMA trend, RSI momentum, ATR strength, and candle
    momentum from the supplied (or freshly fetched) DataFrame.

    If the DataFrame is unavailable, falls back to the disk cache.

    Signals are written into generated_signals.json (merged with existing
    signals, deduplicating by time).

    Returns the two forced signal dicts.
    """
    logger.info("--- Generating FORCED daily signals (15:05 + 15:10) ---")

    # 1. Ensure we have a DataFrame
    if df is None or len(df) < 50:
        df = _load_df_cache()
    if df is None or len(df) < 50:
        df = get_historical_data(outputsize=500)  # lightweight fetch
    if df is None or len(df) < 50:
        logger.warning("No market data available for forced signals; using minimal default.")
        df = None  # decide_direction_live handles None gracefully

    # 2. Calculate live direction
    direction, confidence = decide_direction_live(df)
    low_confidence = confidence < FORCE_SIGNAL_CONFIDENCE_THRESHOLD

    if low_confidence:
        logger.warning(
            f"Forced signal confidence LOW ({confidence}%) — will tag as LOW CONFIDENCE / RISKY MARKET"
        )

    # 3. Build the two forced signal dicts
    direct_signal = {
        "time": FORCE_DIRECT_TIME,
        "pair": PAIR,
        "direction": direction,
        "confidence": confidence,
        "source": "forced",
        "signal_type": "direct",
        "low_confidence": low_confidence,
    }

    martingale_signal = {
        "time": FORCE_MARTINGALE_TIME,
        "pair": PAIR,
        "direction": direction,
        "confidence": confidence,
        "source": "forced",
        "signal_type": "martingale",
        "low_confidence": low_confidence,
    }

    forced_signals = [direct_signal, martingale_signal]

    # 4. Merge with existing signals in generated_signals.json
    existing: list[dict] = []
    if os.path.exists(SIGNAL_FILE):
        try:
            with open(SIGNAL_FILE, "r") as f:
                existing = json.load(f)
        except Exception as e:
            logger.error(f"Could not read existing signals: {e}")
            existing = []

    # Remove any previous forced signals at the same times so we always
    # regenerate them fresh (never skip due to stale cache)
    forced_times = {FORCE_DIRECT_TIME, FORCE_MARTINGALE_TIME}
    existing = [s for s in existing if s.get("time") not in forced_times]

    merged = existing + forced_signals
    merged.sort(key=lambda x: x.get("time", ""))

    try:
        with open(SIGNAL_FILE, "w") as f:
            json.dump(merged, f, indent=2)
        logger.info(
            f"Forced signals saved: {direction} @ {FORCE_DIRECT_TIME} (direct) "
            f"& {FORCE_MARTINGALE_TIME} (martingale) | confidence={confidence}%"
        )
    except Exception as e:
        logger.error(f"Could not save forced signals: {e}")

    return forced_signals


if __name__ == "__main__":
    generate_daily_signals()
    generate_forced_daily_signals()
>>>>>>> copilot/create-project-structure
