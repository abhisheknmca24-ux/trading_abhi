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
