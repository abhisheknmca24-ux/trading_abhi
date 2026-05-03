from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import re
from typing import List, Optional

import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


TIMEZONE_NAME = "Asia/Kolkata"
MARKET_OPEN = time(13, 30)
SIGNAL_WINDOW_SECONDS = 30
MARTINGALE_PREALERT_MIN_SECONDS = 30
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
            "martingale_scheduled": False,
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
    global signal_list, processed_signals, last_update_id, last_signal_update_time

    if now is None:
        now = _now()

    current_day = now.date()
    previous_day = last_signal_update_time.date() if last_signal_update_time is not None else None

    if previous_day != current_day:
        signal_list = []
        processed_signals = set()
        last_update_id = None

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
        last_signal_update_time = now
        return signal_list

    update_id = f"mem-{current_day.isoformat()}-{len(signal_lines)}-{'|'.join(signal_lines)}"

    if update_id != last_update_id:
        entries = load_signal_entries(signal_lines, current_day)
        signal_list = _build_signal_state(entries)
        processed_signals = set()
        last_update_id = update_id
        print("Signal list updated")

    last_signal_update_time = now
    return signal_list


def apply_signal_text(signal_text: str, now: Optional[datetime] = None) -> List[dict]:
    if not isinstance(signal_text, str):
        return update_signal_list([], now)

    lines = [line.strip() for line in signal_text.splitlines() if line.strip()]
    return update_signal_list(lines, now)


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    working_df = df.copy()

    required = {"EMA50", "EMA200", "RSI", "ATR", "TrendStrength"}
    if required.issubset(set(working_df.columns)):
        return working_df

    working_df["EMA50"] = EMAIndicator(working_df["Close"], 50).ema_indicator()
    working_df["EMA200"] = EMAIndicator(working_df["Close"], 200).ema_indicator()
    working_df["RSI"] = RSIIndicator(working_df["Close"], 14).rsi()
    working_df["ATR"] = AverageTrueRange(
        working_df["High"],
        working_df["Low"],
        working_df["Close"],
        14,
    ).average_true_range()
    working_df["TrendStrength"] = (working_df["EMA50"] - working_df["EMA200"]).abs()
    return working_df


def validate_sniper_signal(df: pd.DataFrame, direction: str) -> bool:
    if df is None or len(df) < 200:
        return False

    df = add_indicators(df)
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

    df = add_indicators(df)
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
    df = add_indicators(df)
    last = df.iloc[-1]
    prev = df.iloc[-2]

    atr = float(last["ATR"])
    atr_mean = float(df["ATR"].mean()) if not pd.isna(df["ATR"].mean()) else 0.0
    candle_size = abs(float(last["Close"]) - float(last["Open"]))
    avg_candle = float((df["Close"] - df["Open"]).abs().tail(10).mean())
    distance = abs(float(last["Close"]) - float(last["EMA50"]))

    score = 0
    if direction == "CALL":
        if last["EMA50"] > last["EMA200"]:
            score += 30
        if last["RSI"] > prev["RSI"]:
            score += 20
    else:
        if last["EMA50"] < last["EMA200"]:
            score += 30
        if last["RSI"] < prev["RSI"]:
            score += 20

    if atr > atr_mean:
        score += 20
    if candle_size > avg_candle:
        score += 15
    if distance <= max(0.0003, atr * 0.50):
        score += 15

    return min(score, 100)


def build_forex_targets(df: pd.DataFrame, direction: str, confidence: int) -> tuple[float, float, float]:
    df = add_indicators(df)
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


def _build_pre_message(signal: dict, confidence: int) -> str:
    return (
        f"*PRE-SIGNAL*\n\n"
        f"Pair: {signal['pair']}\n"
        f"Direction: {signal['direction']}\n"
        f"Time: {signal['time']:%H:%M}\n"
        f"Confidence: {confidence}%\n\n"
        f"FIRST TRADE"
    )


def _build_confirm_message(signal: dict, confidence: int, expiry: datetime, tp: float, sl: float) -> str:
    return (
        f"*CONFIRMED SIGNAL*\n\n"
        f"Pair: {signal['pair']}\n"
        f"Direction: {signal['direction']}\n"
        f"Entry time: {signal['time']:%H:%M}\n"
        f"Expiry: {expiry:%H:%M}\n"
        f"Confidence: {confidence}%\n"
        f"Forex TP: {tp}\n"
        f"Forex SL: {sl}"
    )


def _build_mg_pre_message(signal: dict, confidence: int) -> str:
    return (
        f"*MARTINGALE PRE-ALERT*\n\n"
        f"Pair: {signal['pair']}\n"
        f"Direction: {signal['direction']}\n"
        f"Time: {signal['martingale_time']:%H:%M}\n"
        f"Confidence: {confidence}%"
    )


def _build_mg_confirm_message(signal: dict, confidence: int, expiry: datetime, tp: float, sl: float) -> str:
    return (
        f"*MARTINGALE CONFIRMED*\n\n"
        f"Pair: {signal['pair']}\n"
        f"Direction: {signal['direction']}\n"
        f"Entry time: {signal['martingale_time']:%H:%M}\n"
        f"Expiry: {expiry:%H:%M}\n"
        f"Confidence: {confidence}%\n"
        f"Forex TP: {tp}\n"
        f"Forex SL: {sl}"
    )


def process_signal_list(df: pd.DataFrame) -> List[str]:
    global signal_list, processed_signals

    if df is None or len(df) < 200:
        return []

    now = _now()
    messages: List[str] = []

    for signal in signal_list:
        try:
            signal_time = signal["time"]
            direction = signal["direction"]

            if signal_time.date() != now.date():
                continue
            if signal_time.time() < MARKET_OPEN:
                continue

            base_key = _signal_key(signal_time, direction)
            confidence = calculate_confidence(df, direction)

            seconds_to_entry = (signal_time - now).total_seconds()
            if 30 <= seconds_to_entry <= 150 and not signal.get("pre_sent"):
                if validate_sniper_signal(df, direction):
                    messages.append(_build_pre_message(signal, confidence))
                    signal["pre_sent"] = True

            if abs((now - signal_time).total_seconds()) <= SIGNAL_WINDOW_SECONDS:
                if base_key not in processed_signals and validate_sniper_signal(df, direction):
                    _, tp, sl = build_forex_targets(df, direction, confidence)
                    expiry = signal_time + timedelta(minutes=5)
                    messages.append(_build_confirm_message(signal, confidence, expiry, tp, sl))
                    processed_signals.add(base_key)
                    signal["confirmed_sent"] = True
                    signal["martingale_scheduled"] = True
                    signal["martingale_confidence"] = confidence
                    print(f"Signal confirmed: {signal_time:%H:%M} {direction}")

            if not signal.get("martingale_scheduled"):
                continue

            mg_time = signal["martingale_time"]
            mg_key = _signal_key(mg_time, direction)
            seconds_to_mg = (mg_time - now).total_seconds()

            if (
                MARTINGALE_PREALERT_MIN_SECONDS <= seconds_to_mg <= MARTINGALE_PREALERT_MAX_SECONDS
                and not signal.get("martingale_prealert_sent")
            ):
                if validate_martingale_signal(df, direction):
                    mg_confidence = calculate_confidence(df, direction)
                    signal["martingale_confidence"] = mg_confidence
                    messages.append(_build_mg_pre_message(signal, mg_confidence))
                    signal["martingale_prealert_sent"] = True

            if abs((now - mg_time).total_seconds()) <= SIGNAL_WINDOW_SECONDS:
                if mg_key not in processed_signals and validate_martingale_signal(df, direction):
                    mg_confidence = calculate_confidence(df, direction)
                    signal["martingale_confidence"] = mg_confidence
                    _, tp, sl = build_forex_targets(df, direction, mg_confidence)
                    expiry = mg_time + timedelta(minutes=5)
                    messages.append(
                        _build_mg_confirm_message(
                            signal,
                            mg_confidence,
                            expiry,
                            tp,
                            sl,
                        )
                    )
                    processed_signals.add(mg_key)
                    signal["martingale_confirmed_sent"] = True
                    print(f"Signal confirmed: {mg_time:%H:%M} {direction}")
        except Exception as e:
            print(f"Processing error: {e}")
            continue

    return messages
