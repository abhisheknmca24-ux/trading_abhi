from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import json
import os
import re
from typing import Callable, List, Optional

import pandas as pd

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


TIMEZONE_NAME = "Asia/Kolkata"
MARKET_OPEN = time(13, 30)
SIGNAL_WINDOW_SECONDS = 75
PRE_SIGNAL_MIN_SECONDS = 60
PRE_SIGNAL_MAX_SECONDS = 120
MARTINGALE_PREALERT_MIN_SECONDS = 45
MARTINGALE_PREALERT_MAX_SECONDS = 150
MARTINGALE_ENTRY_DELAY = timedelta(minutes=2)
SIGNAL_PATTERN = re.compile(r"^(?P<hour>\d{2}):(?P<minute>\d{2})\s+EURUSD\s+(?P<signal>CALL|PUT)$")


@dataclass(frozen=True)
class SignalEntry:
    signal_time: datetime
    pair: str
    direction: str
    raw_line: str


signal_list = []
processed_signals = set()
last_update_id = None
last_signal_update_time = None
evaluated_signal_count = 0
confirmed_signal_count = 0
ENABLE_MARTINGALE = True
 
# Result tracking
tracked_signals: List[dict] = []
total_trades = 0
wins = 0
losses = 0
last_daily_report_date = None


def _get_timezone():
    if ZoneInfo is None:
        return None
    return ZoneInfo(TIMEZONE_NAME)


def _now() -> datetime:
    tz = _get_timezone()
    if tz is None:
        return datetime.now()
    return datetime.now(tz)


def _signal_key(signal_time: datetime, direction: str) -> str:
    return f"{signal_time:%H:%M}_{direction}"


def _build_signal_state(entries: List[SignalEntry]) -> List[dict]:
    return [
        {
            "time": entry.signal_time,
            "pair": entry.pair,
            "direction": entry.direction,
            "pre_sent": False,
            "confirmed_sent": False,
            "martingale_time": entry.signal_time + MARTINGALE_ENTRY_DELAY,
            "martingale_prealert_sent": False,
            "martingale_confirmed_sent": False,
            "martingale_confidence": 0,
        }
        for entry in entries
    ]


def _parse_line(line: str, current_day: date) -> Optional[SignalEntry]:
    try:
        if not isinstance(line, str):
            return None

        stripped = line.strip()

        if not stripped or stripped.startswith("#"):
            return None

        match = SIGNAL_PATTERN.match(stripped.upper())
        if match is None:
            return None

        signal_hour = int(match.group("hour"))
        signal_minute = int(match.group("minute"))

        if signal_hour < 0 or signal_hour > 23:
            return None
        if signal_minute < 0 or signal_minute > 59:
            return None

        # Critical time conversion is guarded to keep the bot crash-free.
        signal_time = datetime.combine(current_day, time(signal_hour, signal_minute))

        tz = _get_timezone()
        if tz is not None:
            signal_time = signal_time.replace(tzinfo=tz)

        return SignalEntry(
            signal_time=signal_time,
            pair="EURUSD",
            direction=match.group("signal").upper(),
            raw_line=stripped,
        )
    except Exception as e:
        print(f"Parse error: {e}")
        return None


def load_signal_entries(signal_lines: List[str], current_day: Optional[date] = None) -> List[SignalEntry]:
    if current_day is None:
        current_day = _now().date()

    if not signal_lines:
        return []

    entries: List[SignalEntry] = []
    for line in signal_lines:
        entry = _parse_line(line, current_day)
        if entry is not None:
            entries.append(entry)

    return entries


def update_signal_list(signal_lines: Optional[List[str]] = None, now: Optional[datetime] = None) -> List[dict]:
    global signal_list, processed_signals, last_update_id, last_signal_update_time, evaluated_signal_count, confirmed_signal_count

    if now is None:
        now = _now()

    current_day = now.date()
    previous_day = last_signal_update_time.date() if last_signal_update_time is not None else None

    if previous_day != current_day:
        signal_list = []
        processed_signals = set()
        last_update_id = None
        evaluated_signal_count = 0
        confirmed_signal_count = 0

    if signal_lines is None:
        last_signal_update_time = now
        return signal_list

    if isinstance(signal_lines, str):
        signal_lines = signal_lines.splitlines()

    if not isinstance(signal_lines, list):
        signal_lines = []

    if not signal_lines:
        signal_list = []
        processed_signals = set()
        last_update_id = None
        evaluated_signal_count = 0
        confirmed_signal_count = 0
        last_signal_update_time = now
        return signal_list

    update_id = f"mem-{current_day.isoformat()}-{len(signal_lines)}-{'|'.join(signal_lines)}"

    if update_id != last_update_id:
        entries = load_signal_entries(signal_lines, current_day)
        signal_list = _build_signal_state(entries)
        processed_signals = set()
        evaluated_signal_count = 0
        confirmed_signal_count = 0
        last_update_id = update_id
        print("Signal list updated")

    last_signal_update_time = now
    return signal_list


def apply_signal_text(signal_text: str, now: Optional[datetime] = None) -> List[dict]:
    if not isinstance(signal_text, str):
        return update_signal_list([], now)

    lines = [line.strip() for line in signal_text.splitlines() if line.strip()]
    return update_signal_list(lines, now)


def should_force_fast_mode(now: Optional[datetime] = None, window_seconds: int = 180) -> bool:
    if now is None:
        now = _now()

    for signal in signal_list:
        try:
            signal_time = signal.get("time")
            direction = signal.get("direction")
            if signal_time is None:
                continue
            if signal_time.date() != now.date():
                continue
            if signal_time.time() < MARKET_OPEN:
                continue

            if direction is not None and _signal_key(signal_time, direction) in processed_signals:
                continue

            seconds_to_signal = (signal_time - now).total_seconds()
            if 0 <= seconds_to_signal <= window_seconds:
                return True
        except Exception:
            continue

    return False


def validate_sniper_signal(df: pd.DataFrame, direction: str) -> bool:
    if df is None or len(df) < 200:
        return False

    last = df.iloc[-1]
    prev = df.iloc[-2]

    required_values = [
        last["EMA50"],
        last["EMA200"],
        last["RSI"],
        prev["RSI"],
        last["ATR"],
        df["ATR"].mean(),
    ]
    if any(pd.isna(value) for value in required_values):
        return False

    atr = float(last["ATR"])
    atr_mean = float(df["ATR"].mean())
    candle_size = abs(float(last["Close"]) - float(last["Open"]))
    avg_candle = (df["Close"] - df["Open"]).abs().tail(10).mean()
    distance = abs(float(last["Close"]) - float(last["EMA50"]))

    trend_threshold = max(0.0002, atr * 0.15)
    ema_distance_threshold = max(0.0003, atr * 0.50)

    if direction == "CALL":
        trend_ok = last["EMA50"] > last["EMA200"]
        rsi_ok = last["RSI"] > prev["RSI"]
    else:
        trend_ok = last["EMA50"] < last["EMA200"]
        rsi_ok = last["RSI"] < prev["RSI"]

    if not trend_ok:
        return False
    if abs(float(last["EMA50"]) - float(last["EMA200"])) <= trend_threshold:
        return False
    if not rsi_ok:
        return False
    if atr <= atr_mean:
        return False
    if candle_size <= float(avg_candle):
        return False
    if distance > ema_distance_threshold:
        return False

    return True


def validate_martingale_signal(df: pd.DataFrame, direction: str) -> bool:
    if not validate_sniper_signal(df, direction):
        return False

    last = df.iloc[-1]
    prev = df.iloc[-2]

    if direction == "CALL":
        if last["RSI"] <= 60:
            return False
        if last["RSI"] <= prev["RSI"]:
            return False
        if float(last["Close"]) <= float(last["Open"]):
            return False
    else:
        if last["RSI"] >= 40:
            return False
        if last["RSI"] >= prev["RSI"]:
            return False
        if float(last["Close"]) >= float(last["Open"]):
            return False

    return True


def calculate_confidence(df: pd.DataFrame, direction: str) -> int:
    last = df.iloc[-1]
    prev = df.iloc[-2]
    atr = float(last["ATR"]) if not pd.isna(last.get("ATR")) else 0.0
    atr_mean = float(df["ATR"].mean()) if not pd.isna(df["ATR"].mean()) else 0.0
    candle_size = abs(float(last["Close"]) - float(last["Open"]))
    avg_candle = float((df["Close"] - df["Open"]).abs().tail(10).mean())
    distance = abs(float(last["Close"]) - float(last["EMA50"]))

    score = 0
    # EMA Trend: +25
    if direction == "CALL":
        if last["EMA50"] > last["EMA200"]:
            score += 25
    else:
        if last["EMA50"] < last["EMA200"]:
            score += 25

    # RSI Movement: +15
    if direction == "CALL":
        rsi = float(last["RSI"])
        rsi_prev = float(prev["RSI"])
        if rsi > 55 and rsi > rsi_prev:
            score += 15
    else:
        rsi = float(last["RSI"])
        rsi_prev = float(prev["RSI"])
        if rsi < 45 and rsi < rsi_prev:
            score += 15

    # ATR Strength: +15
    if atr > atr_mean:
        score += 15

    # Candle Strength: +10
    if candle_size > avg_candle:
        score += 10

    # Distance: +10 (if close to EMA50)
    if distance <= max(0.0003, atr * 0.50):
        score += 10

    # Score now maxes at 75 based on weights above; scale to percentage and cap at 90%
    confidence = int((score / 75) * 85)
    return min(confidence, 90)


def build_forex_targets(df: pd.DataFrame, direction: str, confidence: int) -> tuple[float, float, float]:
    last = df.iloc[-1]
    atr = float(last["ATR"])
    entry = float(last["Close"])

    if confidence >= 85:
        tp_factor = 2.0
        sl_factor = 0.8
    elif confidence >= 75:
        tp_factor = 1.5
        sl_factor = 1.0
    else:
        tp_factor = 1.0
        sl_factor = 1.2

    if direction == "CALL":
        tp = entry + (tp_factor * atr)
        sl = entry - (sl_factor * atr)
    else:
        tp = entry - (tp_factor * atr)
        sl = entry + (sl_factor * atr)

    return round(entry, 5), round(tp, 5), round(sl, 5)


def _get_ema_trend(df: pd.DataFrame, direction: str) -> str:
    """Determine if EMA trend is Bullish or Bearish."""
    last = df.iloc[-1]
    normalized_direction = str(direction).upper()

    if normalized_direction in {"CALL", "BUY"}:
        if last["EMA50"] > last["EMA200"]:
            return "Bullish 📈"
        else:
            return "Bearish 📉"
    else:
        if last["EMA50"] < last["EMA200"]:
            return "Bearish 📉"
        else:
            return "Bullish 📈"


def _get_rsi_interpretation(rsi: float) -> str:
    """Interpret RSI value."""
    if rsi >= 70:
        return "Overbought"
    elif rsi >= 60:
        return "Moderate Bullish"
    elif rsi > 50:
        return "Slightly Bullish"
    elif rsi == 50:
        return "Neutral"
    elif rsi > 40:
        return "Slightly Bearish"
    elif rsi >= 30:
        return "Moderate Bearish"
    else:
        return "Oversold"


def _get_atr_strength(df: pd.DataFrame) -> str:
    """Determine ATR strength as Low/Medium/High."""
    last = df.iloc[-1]
    atr = float(last["ATR"])
    atr_mean = float(df["ATR"].mean())
    
    if atr < atr_mean:
        return "Low Volatility"
    elif atr < atr_mean * 1.3:
        return "Medium Volatility"
    else:
        return "High Volatility 🔥"


def _get_candle_strength(df: pd.DataFrame) -> str:
    """Determine candle strength as Weak/Strong."""
    last = df.iloc[-1]
    candle_size = abs(float(last["Close"]) - float(last["Open"]))
    avg_candle = float((df["Close"] - df["Open"]).abs().tail(10).mean())
    
    if candle_size < avg_candle:
        return "Weak"
    else:
        return "Strong 💪"


def _build_pre_message(signal: dict, confidence: int, df: pd.DataFrame) -> str:
    """Build pre-signal message with full indicator analysis."""
    last = df.iloc[-1]
    rsi = float(last["RSI"]) if not pd.isna(last["RSI"]) else 0
    ema_trend = _get_ema_trend(df, signal['direction'])
    rsi_interp = _get_rsi_interpretation(rsi)
    atr_strength = _get_atr_strength(df)
    candle_strength = _get_candle_strength(df)
    
    return (
        f"📊 *PRE-SIGNAL*\n\n"
        f"Pair: {signal['pair']}\n"
        f"Direction: {signal['direction']}\n"
        f"Time: {signal['time']:%H:%M}\n\n"
        f"Confidence: {confidence}%\n\n"
        f"*Technical Analysis*\n"
        f"EMA Trend: {ema_trend}\n"
        f"RSI: {rsi:.0f} ({rsi_interp})\n"
        f"ATR: {atr_strength}\n"
        f"Candle Strength: {candle_strength}\n\n"
        f"Status: Preparing for entry ⏳"
    )


def _build_confirm_message(signal: dict, confidence: int, expiry: datetime, tp: float, sl: float, df: pd.DataFrame) -> str:
    """Build confirmed signal message with full indicator analysis."""
    last = df.iloc[-1]
    rsi = float(last["RSI"]) if not pd.isna(last["RSI"]) else 0
    ema_trend = _get_ema_trend(df, signal['direction'])
    rsi_interp = _get_rsi_interpretation(rsi)
    atr_strength = _get_atr_strength(df)
    candle_strength = _get_candle_strength(df)
    
    # Determine signal quality based on confidence
    if confidence >= 85:
        signal_quality = "HIGH PROBABILITY TRADE ✅"
    elif confidence >= 75:
        signal_quality = "STRONG SIGNAL ✅"
    else:
        signal_quality = "VALID SIGNAL ✓"
    
    return (
        f"📊 *SIGNAL CONFIRMED*\n\n"
        f"Pair: {signal['pair']}\n"
        f"Direction: {signal['direction']}\n\n"
        f"Confidence: {confidence}%\n\n"
        f"*Technical Analysis*\n"
        f"EMA Trend: {ema_trend}\n"
        f"RSI: {rsi:.0f} ({rsi_interp})\n"
        f"ATR: {atr_strength}\n"
        f"Candle Strength: {candle_strength}\n\n"
        f"*Trade Details*\n"
        f"Entry Time: {signal['time']:%H:%M}\n"
        f"Expiry: {expiry:%H:%M}\n"
        f"TP: {tp}\n"
        f"SL: {sl}\n\n"
        f"Decision: {signal_quality}"
    )


def _check_safety_rules(df: pd.DataFrame, direction: str, confidence: int) -> tuple[bool, str]:
    if confidence < 70:
        return False, "confidence below 70"

    if df is None or len(df) < 200:
        return False, "insufficient data"

    last = df.iloc[-1]
    prev = df.iloc[-2]

    required_values = [
        last["EMA50"],
        last["EMA200"],
        last["RSI"],
        prev["RSI"],
        last["ATR"],
        df["ATR"].mean(),
    ]
    if any(pd.isna(value) for value in required_values):
        return False, "indicator values unavailable"

    atr = float(last["ATR"])
    atr_mean = float(df["ATR"].mean())
    trend_gap = abs(float(last["EMA50"]) - float(last["EMA200"]))
    trend_threshold = max(0.0002, atr * 0.15)
    rsi_value = float(last["RSI"])

    # Overbought/Oversold protection
    if direction == "CALL" and rsi_value >= 68:
        return False, "RSI overbought, reject CALL"
    if direction == "PUT" and rsi_value <= 32:
        return False, "RSI oversold, reject PUT"

    if direction == "CALL":
        trend_direction_ok = float(last["EMA50"]) > float(last["EMA200"])
        rsi_opposite_extreme = rsi_value < 35
    else:
        trend_direction_ok = float(last["EMA50"]) < float(last["EMA200"])
        rsi_opposite_extreme = rsi_value > 65

    if not trend_direction_ok or trend_gap <= trend_threshold:
        return False, "trend unclear"

    if rsi_opposite_extreme:
        return False, "RSI extreme opposite"

    if direction == "CALL" and rsi_value < 55:
        return False, "RSI weak zone"
    if direction == "PUT" and rsi_value > 45:
        return False, "RSI weak zone"

    if atr <= atr_mean:
        return False, "low volatility"

    return True, "safety passed"


def _is_strong_martingale(df: pd.DataFrame, direction: str) -> bool:
    if not validate_martingale_signal(df, direction):
        return False

    confidence = calculate_confidence(df, direction)
    return confidence >= 75


def _should_take_signal(df: pd.DataFrame, direction: str, confidence: int, is_next_signal: bool) -> tuple[bool, str]:
    safety_ok, safety_reason = _check_safety_rules(df, direction, confidence)
    if not safety_ok:
        return False, safety_reason

    # First signal is strict: only high-confidence entries.
    base_threshold = 70 if is_next_signal else 75
    threshold = get_adaptive_trade_threshold(base_threshold)

    if confidence >= threshold:
        return True, f"confidence >= {threshold}"

    if is_next_signal:
        return False, f"next signal confidence below {threshold}"

    return False, f"first signal confidence below {threshold}"


def _is_trade_direction(direction: str) -> bool:
    return str(direction).upper() in {"CALL", "BUY"}


def _get_resolved_trades() -> List[dict]:
    return [entry for entry in tracked_signals if entry.get("resolved")]


def _get_recent_resolved_trades(limit: int = 10) -> List[dict]:
    resolved = _get_resolved_trades()
    resolved.sort(key=lambda entry: entry.get("signal_time") or datetime.min)
    return resolved[-limit:]


def _confidence_bucket(confidence: float) -> str:
    if confidence >= 80:
        return ">=80"
    if confidence >= 70:
        return "70-79"
    return "<70"


def _win_rate(trades: List[dict]) -> float:
    if not trades:
        return 0.0
    wins_count = sum(1 for trade in trades if trade.get("result") == "WIN")
    return (wins_count / len(trades)) * 100


def get_trade_performance() -> dict:
    resolved = _get_resolved_trades()
    resolved_total = len(resolved)
    resolved_wins = sum(1 for trade in resolved if trade.get("result") == "WIN")
    resolved_losses = sum(1 for trade in resolved if trade.get("result") == "LOSS")
    overall_win_rate = (resolved_wins / resolved_total * 100) if resolved_total else 0.0

    confidence_buckets = {">=80": [], "70-79": [], "<70": []}
    for trade in resolved:
        confidence = trade.get("confidence")
        if confidence is None:
            continue
        confidence_buckets[_confidence_bucket(float(confidence))].append(trade)

    bucket_stats = {}
    for bucket, trades in confidence_buckets.items():
        bucket_total = len(trades)
        bucket_wins = sum(1 for trade in trades if trade.get("result") == "WIN")
        bucket_stats[bucket] = {
            "total": bucket_total,
            "wins": bucket_wins,
            "losses": bucket_total - bucket_wins,
            "win_rate": (bucket_wins / bucket_total * 100) if bucket_total else 0.0,
        }

    recent_10 = _get_recent_resolved_trades(10)

    return {
        "total_trades": resolved_total,
        "wins": resolved_wins,
        "losses": resolved_losses,
        "win_rate": overall_win_rate,
        "recent_10_win_rate": _win_rate(recent_10),
        "confidence_buckets": bucket_stats,
    }


def get_adaptive_trade_threshold(base_threshold: int = 70) -> int:
    recent_10 = _get_recent_resolved_trades(10)
    if not recent_10:
        return base_threshold

    recent_win_rate = _win_rate(recent_10)
    if recent_win_rate < 50:
        return base_threshold + 5
    if recent_win_rate > 70:
        return 70
    return base_threshold


def _save_tracked_signals_to_json() -> None:
    """Save tracked_signals to JSON file for persistence across restarts."""
    try:
        json_data = []
        for signal in tracked_signals:
            data = dict(signal)
            # Convert datetime objects to ISO format strings
            if "signal_time" in data and isinstance(data["signal_time"], datetime):
                data["signal_time"] = data["signal_time"].isoformat()
            if "expiry_time" in data and isinstance(data["expiry_time"], datetime):
                data["expiry_time"] = data["expiry_time"].isoformat()
            json_data.append(data)
        
        with open("tracked_signals.json", "w") as f:
            json.dump(json_data, f, indent=2)
    except Exception as e:
        print(f"Error saving tracked signals to JSON: {e}")


def _load_tracked_signals_from_json() -> None:
    """Load tracked_signals from JSON file on startup."""
    global tracked_signals
    try:
        if os.path.exists("tracked_signals.json"):
            with open("tracked_signals.json", "r") as f:
                json_data = json.load(f)
                for data in json_data:
                    # Convert ISO format strings back to datetime objects
                    if "signal_time" in data and isinstance(data["signal_time"], str):
                        data["signal_time"] = datetime.fromisoformat(data["signal_time"])
                    if "expiry_time" in data and isinstance(data["expiry_time"], str):
                        data["expiry_time"] = datetime.fromisoformat(data["expiry_time"])
                    tracked_signals.append(data)
            print(f"Loaded {len(tracked_signals)} tracked signals from JSON")
    except Exception as e:
        print(f"Error loading tracked signals from JSON: {e}")


def store_tracked_signal(
    signal_time: datetime,
    direction: str,
    entry_price: float,
    expiry_time: datetime,
    signal_type: str,
    pair: str,
    confidence: float,
    df: pd.DataFrame,
) -> None:
    global tracked_signals
    last = df.iloc[-1]
    tracked_signals.append({
        "signal_time": signal_time,
        "direction": direction,
        "entry_price": float(entry_price),
        "expiry_time": expiry_time,
        "signal_type": signal_type,
        "pair": pair,
        "confidence": float(confidence),
        "rsi": float(last["RSI"]) if not pd.isna(last["RSI"]) else None,
        "atr": float(last["ATR"]) if not pd.isna(last["ATR"]) else None,
        "ema_trend": _get_ema_trend(df, direction),
        "resolved": False,
    })

    # Memory management: keep only last 200 trades
    if len(tracked_signals) > 200:
        tracked_signals[:] = tracked_signals[-200:]
    
    # Save to JSON for persistence
    _save_tracked_signals_to_json()



def _build_performance_report() -> str:
    stats = get_trade_performance()
    return (
        f"📊 Performance Report\n"
        f"Total Trades: {stats['total_trades']}\n"
        f"Wins: {stats['wins']}\n"
        f"Losses: {stats['losses']}\n"
        f"Win Rate: {stats['win_rate']:.1f}%\n\n"
        f"Win Rate >=80: {stats['confidence_buckets']['>=80']['win_rate']:.1f}%\n"
        f"Win Rate 70-79: {stats['confidence_buckets']['70-79']['win_rate']:.1f}%\n"
        f"Win Rate <70: {stats['confidence_buckets']['<70']['win_rate']:.1f}%"
    )


def _maybe_build_daily_report(now: datetime) -> Optional[str]:
    global last_daily_report_date

    report_date = now.date() - timedelta(days=1)
    if last_daily_report_date == report_date:
        return None

    resolved_for_day = [
        trade for trade in _get_resolved_trades()
        if trade.get("signal_time") is not None and trade["signal_time"].date() == report_date
    ]

    if not resolved_for_day:
        return None

    day_stats = {
        "total_trades": len(resolved_for_day),
        "wins": sum(1 for trade in resolved_for_day if trade.get("result") == "WIN"),
        "losses": sum(1 for trade in resolved_for_day if trade.get("result") == "LOSS"),
    }
    day_stats["win_rate"] = (day_stats["wins"] / day_stats["total_trades"] * 100) if day_stats["total_trades"] else 0.0
    day_stats["best_trades"] = sum(1 for trade in resolved_for_day if float(trade.get("confidence") or 0) >= 80 and trade.get("result") == "WIN")

    last_daily_report_date = report_date

    return (
        f"📊 Performance Report\n"
        f"Total Trades: {day_stats['total_trades']}\n"
        f"Wins: {day_stats['wins']}\n"
        f"Losses: {day_stats['losses']}\n"
        f"Win Rate: {day_stats['win_rate']:.1f}%\n"
        f"Best Trades: {day_stats['best_trades']}"
    )


def _build_mg_pre_message(signal: dict, confidence: int, df: pd.DataFrame) -> str:
    """Build martingale pre-alert message with indicator analysis."""
    last = df.iloc[-1]
    rsi = float(last["RSI"]) if not pd.isna(last["RSI"]) else 0
    ema_trend = _get_ema_trend(df, signal['direction'])
    rsi_interp = _get_rsi_interpretation(rsi)
    atr_strength = _get_atr_strength(df)
    candle_strength = _get_candle_strength(df)
    
    return (
        f"🎲 *MARTINGALE PRE-ALERT*\n\n"
        f"Pair: {signal['pair']}\n"
        f"Direction: {signal['direction']}\n"
        f"Time: {signal['martingale_time']:%H:%M}\n\n"
        f"Confidence: {confidence}%\n\n"
        f"*Technical Analysis*\n"
        f"EMA Trend: {ema_trend}\n"
        f"RSI: {rsi:.0f} ({rsi_interp})\n"
        f"ATR: {atr_strength}\n"
        f"Candle Strength: {candle_strength}\n\n"
        f"Status: Ready for martingale entry"
    )


def _build_mg_confirm_message(signal: dict, confidence: int, expiry: datetime, tp: float, sl: float, df: pd.DataFrame) -> str:
    """Build martingale confirmed message with indicator analysis."""
    last = df.iloc[-1]
    rsi = float(last["RSI"]) if not pd.isna(last["RSI"]) else 0
    ema_trend = _get_ema_trend(df, signal['direction'])
    rsi_interp = _get_rsi_interpretation(rsi)
    atr_strength = _get_atr_strength(df)
    candle_strength = _get_candle_strength(df)
    
    return (
        f"🎲 *MARTINGALE CONFIRMED*\n\n"
        f"Pair: {signal['pair']}\n"
        f"Direction: {signal['direction']}\n\n"
        f"Confidence: {confidence}%\n\n"
        f"*Technical Analysis*\n"
        f"EMA Trend: {ema_trend}\n"
        f"RSI: {rsi:.0f} ({rsi_interp})\n"
        f"ATR: {atr_strength}\n"
        f"Candle Strength: {candle_strength}\n\n"
        f"*Trade Details*\n"
        f"Entry Time: {signal['martingale_time']:%H:%M}\n"
        f"Expiry: {expiry:%H:%M}\n"
        f"TP: {tp}\n"
        f"SL: {sl}\n\n"
        f"Decision: ENTERING MARTINGALE ✅"
    )


def _format_result_message(entry: dict) -> str:
    """Format result message for a finished trade."""
    pair = entry.get("pair", "EURUSD")
    direction = entry.get("direction")
    entry_price = entry.get("entry_price")
    final_price = entry.get("final_price")
    result = entry.get("result")

    outcome = "✅ WIN" if result == "WIN" else "❌ LOSS"

    return (
        f"📊 RESULT\n\n"
        f"Pair: {pair}\n"
        f"Direction: {direction}\n\n"
        f"Entry: {entry_price:.5f}\n"
        f"Exit: {final_price:.5f}\n\n"
        f"Result: {outcome}"
    )


def _update_stats(entry: dict):
    """Update global stats based on a finished trade."""
    global total_trades, wins, losses
    total_trades += 1
    if entry.get("result") == "WIN":
        wins += 1
    else:
        losses += 1


def process_signal_list(
    df: pd.DataFrame,
    minute_data_fetcher: Optional[Callable[[], Optional[pd.DataFrame]]] = None,
) -> List[str]:
    global signal_list, processed_signals, evaluated_signal_count, confirmed_signal_count
    global tracked_signals, total_trades, wins, losses

    # Load tracked signals from JSON on first call
    if not hasattr(process_signal_list, "_initialized"):
        _load_tracked_signals_from_json()
        process_signal_list._initialized = True

    if df is None or len(df) < 200:
        return []

    now = _now()
    messages: List[str] = []

    daily_report = _maybe_build_daily_report(now)
    if daily_report:
        messages.append(daily_report)

    # Fetch 1-minute data ONCE and reuse for all operations
    minute_df = None
    if minute_data_fetcher is not None:
        try:
            minute_df = minute_data_fetcher()
        except Exception:
            minute_df = None

    # 1) Check for any tracked signals that have expired and report results
    try:
        for entry in list(tracked_signals):
            if entry.get("resolved"):
                continue
            expiry = entry.get("expiry_time")
            if expiry is None:
                continue
            if expiry <= now:
                # Use cached 1min data to determine final price; fallback to provided `df`.
                final_data = None
                if minute_df is not None and len(minute_df) >= 1:
                    final_data = minute_df
                elif df is not None and len(df) >= 1:
                    final_data = df

                # Ensure a fallback to `df` so result will always be calculated
                if final_data is None or len(final_data) < 1:
                    final_data = df

                try:
                    final_price = float(final_data.iloc[-1]["Close"])
                except:
                    final_price = float(df.iloc[-1]["Close"])
                entry_price = float(entry.get("entry_price"))
                direction = entry.get("direction")

                if _is_trade_direction(direction):
                    is_win = final_price > entry_price
                else:
                    is_win = final_price < entry_price

                entry["final_price"] = final_price
                entry["resolved"] = True
                entry["result"] = "WIN" if is_win else "LOSS"

                # update stats and prepare message
                _update_stats(entry)
                messages.append(_format_result_message(entry))
                
                # Save updated tracked signals to JSON
                _save_tracked_signals_to_json()
    except Exception as e:
        print(f"Result tracking error: {e}")

    for signal in signal_list:
        try:
            signal_time = signal["time"]
            direction = signal["direction"]

            if signal_time.date() != now.date():
                continue
            if signal_time.time() < MARKET_OPEN:
                continue

            base_key = _signal_key(signal_time, direction)

            # Use cached 1-minute data for PRE-SIGNAL confidence when available.
            # Determine whether this is a 'next' signal for thresholding.
            is_next_signal = evaluated_signal_count > 0
            base_threshold = 70 if is_next_signal else 75
            threshold = get_adaptive_trade_threshold(base_threshold)

            # Prefer minute-level data for pre-signal calculation; fallback to provided df.
            pre_df = minute_df if (minute_df is not None and len(minute_df) >= 200) else df
            pre_confidence = calculate_confidence(pre_df, direction)

            seconds_to_entry = (signal_time - now).total_seconds()
            if PRE_SIGNAL_MIN_SECONDS <= seconds_to_entry <= PRE_SIGNAL_MAX_SECONDS and not signal.get("pre_sent"):
                # Only send pre-signal when the confidence meets the same adaptive threshold
                if pre_confidence >= threshold:
                    messages.append(_build_pre_message(signal, pre_confidence, pre_df))
                    signal["pre_sent"] = True
                    # Persist the pre-calculated confidence so confirmation uses the same value
                    signal["pre_confidence"] = pre_confidence
                    print(f"FINAL CONFIDENCE USED: {pre_confidence}%")

            if abs((now - signal_time).total_seconds()) <= SIGNAL_WINDOW_SECONDS:
                if base_key in processed_signals:
                    continue

                signal_df = minute_df

                if signal_df is None or len(signal_df) < 200:
                    signal_df = df

                # Use any pre-calculated confidence if present so pre-signal and confirmation match.
                pre_conf = signal.get("pre_confidence")
                is_next_signal = evaluated_signal_count > 0
                if pre_conf is not None:
                    confidence = pre_conf
                else:
                    confidence = calculate_confidence(signal_df, direction)

                # Debug log final confidence used for decision
                print(f"FINAL CONFIDENCE USED: {confidence}%")

                # Re-check safety rules at confirmation time - if market weakens, skip confirmation
                should_take, skip_reason = _should_take_signal(signal_df, direction, confidence, is_next_signal)

                if should_take:
                    _, tp, sl = build_forex_targets(signal_df, direction, confidence)
                    expiry = signal_time + timedelta(minutes=5)
                    messages.append(_build_confirm_message(signal, confidence, expiry, tp, sl, signal_df))
                    # Store confirmed signal for result tracking
                    try:
                        entry_price = float(signal_df.iloc[-1]["Close"])
                        store_tracked_signal(
                            signal_time=signal_time,
                            direction=direction,
                            entry_price=entry_price,
                            expiry_time=expiry,
                            signal_type="direct",
                            pair=signal.get("pair", "EURUSD"),
                            confidence=confidence,
                            df=signal_df,
                        )
                    except Exception as e:
                        print(f"store_tracked_signal error: {e}")
                    signal["confirmed_sent"] = True
                    signal["martingale_confidence"] = confidence
                    confirmed_signal_count += 1
                    print(f"Signal confirmed: {signal_time:%H:%M} {direction}")
                else:
                    # Market weakened between pre-signal and confirmation - skip safely
                    print(f"Skipping confirmation for {signal_time:%H:%M} {direction}: {skip_reason}")

                processed_signals.add(base_key)
                evaluated_signal_count += 1

            if not ENABLE_MARTINGALE:
                continue

            mg_time = signal["martingale_time"]
            mg_key = _signal_key(mg_time, direction)
            seconds_to_mg = (mg_time - now).total_seconds()

            if (
                MARTINGALE_PREALERT_MIN_SECONDS <= seconds_to_mg <= MARTINGALE_PREALERT_MAX_SECONDS
                and not signal.get("martingale_prealert_sent")
            ):
                # Prefer 1-minute data for martingale checks; fallback to main df
                mg_df = minute_df if (minute_df is not None and len(minute_df) >= 200) else df
                if _is_strong_martingale(mg_df, direction):
                    mg_confidence = calculate_confidence(mg_df, direction)
                    signal["martingale_confidence"] = mg_confidence
                    messages.append(_build_mg_pre_message(signal, mg_confidence, mg_df))
                    print(f"FINAL CONFIDENCE USED: {mg_confidence}%")
                    signal["martingale_prealert_sent"] = True

            if abs((now - mg_time).total_seconds()) <= SIGNAL_WINDOW_SECONDS:
                # Prefer 1-minute data for martingale confirmation; fallback to main df
                mg_df = minute_df if (minute_df is not None and len(minute_df) >= 200) else df
                if mg_key not in processed_signals and _is_strong_martingale(mg_df, direction):
                    mg_confidence = calculate_confidence(mg_df, direction)
                    signal["martingale_confidence"] = mg_confidence
                    print(f"FINAL CONFIDENCE USED: {mg_confidence}%")
                    _, tp, sl = build_forex_targets(mg_df, direction, mg_confidence)
                    expiry = mg_time + timedelta(minutes=5)
                    messages.append(
                        _build_mg_confirm_message(
                            signal,
                            mg_confidence,
                            expiry,
                            tp,
                            sl,
                            mg_df,
                        )
                    )
                    # Store martingale confirmed signal
                    try:
                        entry_price = float(mg_df.iloc[-1]["Close"])
                        store_tracked_signal(
                            signal_time=mg_time,
                            direction=direction,
                            entry_price=entry_price,
                            expiry_time=expiry,
                            signal_type="martingale",
                            pair=signal.get("pair", "EURUSD"),
                            confidence=mg_confidence,
                            df=mg_df,
                        )
                    except Exception as e:
                        print(f"store_tracked_signal error: {e}")
                    processed_signals.add(mg_key)
                    signal["martingale_confirmed_sent"] = True
                    print(f"Signal confirmed: {mg_time:%H:%M} {direction}")
        except Exception as e:
            print(f"Processing error: {e}")
            continue

    return messages
