from __future__ import annotations
from logger import logger

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import json
import os
import re
from typing import Callable, List, Optional

import pandas as pd

from indicators import add_indicators

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


TIMEZONE_NAME = "Asia/Kolkata"
MARKET_OPEN = time(13, 0)
MARKET_CLOSE = time(22, 0)
SIGNAL_WINDOW_SECONDS = 75
PRE_SIGNAL_MIN_SECONDS = 60
PRE_SIGNAL_MAX_SECONDS = 120
MARTINGALE_PREALERT_MIN_SECONDS = 45
MARTINGALE_PREALERT_MAX_SECONDS = 150
MARTINGALE_ENTRY_DELAY = timedelta(minutes=2)
GENERATED_SIGNALS_FILE = "generated_signals.json"
SIGNAL_PATTERN = re.compile(r"^(?P<hour>\d{2}):(?P<minute>\d{2})\s+EURUSD\s+(?P<signal>CALL|PUT)$")

# Forced daily signal times (IST) – ALWAYS sent, never skipped
FORCED_DIRECT_TIME = "15:05"
FORCED_MARTINGALE_TIME = "15:10"
FORCED_SIGNAL_LOW_CONF_THRESHOLD = 55  # Below this → LOW CONFIDENCE warning
FORCED_MARTINGALE_MIN_CONFIDENCE = 70  # Below this → HIGH RISK MARTINGALE warning


@dataclass(frozen=True)
class SignalEntry:
    signal_time: datetime
    pair: str
    direction: str
    raw_line: str


class SmartSignalManager:
    def __init__(self):
        self.active_signals = []
        self.processed_signals = set()
        self.last_update_id = None
        self.last_signal_update_time = None
        self.evaluated_signal_count = 0
        self.confirmed_signal_count = 0
        self.enable_martingale = True
        self.tracked_trades = []
        self.total_trades = 0
        self.wins = 0
        self.losses = 0
        self.last_daily_report_date = None
        self.storage_file = "smart_signals.json"
        self.last_signal_lines = []
        self._initialized = False

    def load(self):
        if not os.path.exists(self.storage_file):
            return
        try:
            with open(self.storage_file, "r") as f:
                data = json.load(f)
                self.tracked_trades = data.get("tracked_trades", [])
                for t in self.tracked_trades:
                    if "signal_time" in t and isinstance(t["signal_time"], str):
                        t["signal_time"] = datetime.fromisoformat(t["signal_time"])
                    if "expiry_time" in t and isinstance(t["expiry_time"], str):
                        t["expiry_time"] = datetime.fromisoformat(t["expiry_time"])
                
                self.last_daily_report_date = data.get("last_daily_report_date")
                if self.last_daily_report_date:
                    self.last_daily_report_date = date.fromisoformat(self.last_daily_report_date)
                
                self.recalculate_stats()
                logger.info(f"Loaded {len(self.tracked_trades)} trades from {self.storage_file}")
        except Exception as e:
            logger.error(f"Error loading {self.storage_file}: {e}")

    def save(self):
        try:
            data = {
                "tracked_trades": [],
                "last_daily_report_date": self.last_daily_report_date.isoformat() if self.last_daily_report_date else None
            }
            for t in self.tracked_trades:
                entry = dict(t)
                if "signal_time" in entry and isinstance(entry["signal_time"], datetime):
                    entry["signal_time"] = entry["signal_time"].isoformat()
                if "expiry_time" in entry and isinstance(entry["expiry_time"], datetime):
                    entry["expiry_time"] = entry["expiry_time"].isoformat()
                data["tracked_trades"].append(entry)
            
            with open(self.storage_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving {self.storage_file}: {e}")

    def recalculate_stats(self):
        resolved = [t for t in self.tracked_trades if t.get("resolved")]
        self.total_trades = len(resolved)
        self.wins = sum(1 for t in resolved if t.get("result") == "WIN")
        self.losses = self.total_trades - self.wins

manager = SmartSignalManager()
manager.load()


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


def _build_signal_state(entries: List[SignalEntry], source: str = "telegram") -> List[dict]:
    return [
        {
            "time": entry.signal_time,
            "pair": entry.pair,
            "direction": entry.direction,
            "source": source,
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
        logger.error(f"Parse error: {e}")
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


def load_generated_signals() -> List[SignalEntry]:
    """Load generated_signals.json and convert to SignalEntry list."""
    try:
        if not os.path.exists(GENERATED_SIGNALS_FILE):
            return []
        with open(GENERATED_SIGNALS_FILE) as f:
            content = f.read().strip()
            if not content:
                return []
            generated = json.loads(content)
        if not isinstance(generated, list):
            return []
    except Exception as e:
        print(f"Error loading generated signals: {e}")
        return []

    tz = _get_timezone()
    today = _now().date()
    entries = []
    for sig in generated:
        try:
            time_str = str(sig.get("time", ""))
            pair = str(sig.get("pair", "EURUSD")).upper()
            direction = str(sig.get("direction", "")).upper()

            if not time_str or direction not in ("CALL", "PUT"):
                continue

            parts = time_str.split(":")
            hour, minute = int(parts[0]), int(parts[1])
            sig_time = datetime.combine(today, time(hour, minute))
            if tz is not None:
                sig_time = sig_time.replace(tzinfo=tz)

            if sig_time.time() < MARKET_OPEN:
                continue

            raw_line = f"{time_str} {pair} {direction}"
            entries.append(SignalEntry(
                signal_time=sig_time,
                pair=pair,
                direction=direction,
                raw_line=raw_line,
            ))
        except (KeyError, ValueError, IndexError, TypeError) as e:
            print(f"Error parsing generated signal {sig}: {e}")
            continue

    return entries


def _merge_generated_into_signal_list() -> None:
    """Merge generated signal entries into signal_list, avoiding duplicates."""
    global signal_list

    generated_entries = load_generated_signals()
    if not generated_entries:
        return

    existing_keys = set()
    for sig in signal_list:
        try:
            key = _signal_key(sig["time"], sig["direction"])
            existing_keys.add(key)
        except Exception:
            continue

    new_entries = []
    for entry in generated_entries:
        key = _signal_key(entry.signal_time, entry.direction)
        if key not in existing_keys:
            existing_keys.add(key)
            new_entries.append(entry)

    if new_entries:
        signal_list.extend(_build_signal_state(new_entries))


def load_generated_signals(current_day: Optional[date] = None) -> List[dict]:
    """Load signals from generated_signals.json and convert to signal states."""
    if current_day is None:
        current_day = _now().date()

    file_path = "generated_signals.json"
    if not os.path.exists(file_path):
        return []

    try:
        with open(file_path, "r") as f:
            generated = json.load(f)

        states = []
        tz = _get_timezone()

        for item in generated:
            try:
                t_str = item.get("time", "")
                if ":" not in t_str:
                    continue

                hour, minute = map(int, t_str.split(":"))
                signal_time = datetime.combine(current_day, time(hour, minute))

                if tz is not None:
                    signal_time = signal_time.replace(tzinfo=tz)

                is_forced = item.get("source") == "forced"
                sig_type = item.get("signal_type", "direct")
                low_conf = item.get("low_confidence", False)

                entry = SignalEntry(
                    signal_time=signal_time,
                    pair=item.get("pair", "EURUSD"),
                    direction=item.get("direction", "CALL").upper(),
                    raw_line=(
                        f"{t_str} {item.get('pair')} {item.get('direction')} "
                        f"({'FORCED' if is_forced else 'GENERATED'})"
                    )
                )
                # Build base state and enrich with forced-signal metadata
                state = {
                    "time": entry.signal_time,
                    "pair": entry.pair,
                    "direction": entry.direction,
                    "source": item.get("source", "generated"),
                    "pre_sent": False,
                    "confirmed_sent": False,
                    "martingale_time": entry.signal_time + MARTINGALE_ENTRY_DELAY,
                    "martingale_prealert_sent": False,
                    "martingale_confirmed_sent": False,
                    "martingale_confidence": 0,
                    "raw_line": entry.raw_line,
                    # Forced-signal extras
                    "is_forced": is_forced,
                    "forced_type": sig_type if is_forced else None,
                    "low_confidence": low_conf,
                    "stored_confidence": item.get("confidence", 0),
                }
                states.append(state)
            except Exception as e:
                logger.error(f"Error parsing generated signal item: {e}")
                continue

        return states
    except Exception as e:
        logger.error(f"Error loading generated signals: {e}")
        return []


def _merge_states(primary: List[dict], secondary: List[dict]) -> List[dict]:
    """Merge two lists of signal states, avoiding duplicates based on time and direction."""
    seen = set()
    merged = []
    
    # Primary (Telegram) takes precedence
    for s in primary:
        key = _signal_key(s["time"], s["direction"])
        if key not in seen:
            merged.append(s)
            seen.add(key)
            
    # Secondary (Generated) added if not already present
    for s in secondary:
        key = _signal_key(s["time"], s["direction"])
        if key not in seen:
            merged.append(s)
            seen.add(key)
            
    return merged


def update_signal_list(signal_lines: Optional[List[str]] = None, now: Optional[datetime] = None) -> List[dict]:
    if now is None:
        now = _now()

    current_day = now.date()
    previous_day = manager.last_signal_update_time.date() if manager.last_signal_update_time is not None else None

    if previous_day != current_day:
        manager.active_signals = []
        manager.processed_signals = set()
        manager.last_update_id = None
        manager.evaluated_signal_count = 0
        manager.confirmed_signal_count = 0

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

    for signal in manager.active_signals:
        try:
            signal_time = signal.get("time")
            direction = signal.get("direction")
            if signal_time is None:
                continue
            if signal_time.date() != now.date():
                continue
            if signal_time.time() < MARKET_OPEN or signal_time.time() > MARKET_CLOSE:
                continue

            if direction is not None and _signal_key(signal_time, direction) in manager.processed_signals:
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
    source = signal.get("source", "telegram")
    source_badges = {"telegram": "📨 Telegram", "generated": "🤖 Generated", "auto": "⚡ Auto"}
    source_label = source_badges.get(source, source.title())
    
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
        f"Direction: {signal['direction']}\n"
        f"Source: {source_label}\n\n"
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


def _check_safety_rules(df: pd.DataFrame, direction: str, confidence: int) -> tuple[bool, str, int]:
    """
    Strictly re-check technical indicators before confirmation.
    Re-checks: ATR, RSI, EMA Trend, and Momentum.
    """
    if df is None or len(df) < 200:
        return False, "insufficient data", confidence

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
        return False, "indicator values unavailable", confidence

    atr = float(last["ATR"])
    atr_mean = float(df["ATR"].mean())
    rsi_value = float(last["RSI"])
    rsi_prev = float(prev["RSI"])
    
    # ==========================================
    # 1. ATR / VOLATILITY CHECK (Weak Market)
    # ==========================================
    # Absolute minimum volatility floor
    if atr < 0.00008:
        return False, "Market too dead (Flat ATR)", confidence
        
    # Relative weakness
    if atr < (atr_mean * 0.65):
        return False, "Volatility too weak for reliable signal", confidence

    # ==========================================
    # 2. RSI & MOMENTUM CHECK
    # ==========================================
    # Overbought/Oversold protection (Stricter)
    if direction == "CALL" and rsi_value >= 65:
        return False, "RSI overbought (>65)", confidence
    if direction == "PUT" and rsi_value <= 35:
        return False, "RSI oversold (<35)", confidence

    # Momentum Direction check (Must be moving WITH the trade)
    if direction == "CALL":
        if rsi_value < 52:
            return False, "Weak bullish momentum (RSI < 52)", confidence
        if rsi_value <= rsi_prev:
            return False, "Momentum stalled (RSI not increasing)", confidence
    else:
        if rsi_value > 48:
            return False, "Weak bearish momentum (RSI > 48)", confidence
        if rsi_value >= rsi_prev:
            return False, "Momentum stalled (RSI not decreasing)", confidence

    # ==========================================
    # 3. EMA TREND & ALIGNMENT
    # ==========================================
    # Directional Alignment
    ema50 = float(last["EMA50"])
    ema200 = float(last["EMA200"])
    ema50_prev = float(df.iloc[-3]["EMA50"]) # Check 2 candles back for slope
    
    if direction == "CALL":
        trend_direction_ok = ema50 > ema200
        price_above_ema = float(last["Close"]) > ema50
        ema_slope_ok = ema50 > ema50_prev # Must be rising
    else:
        trend_direction_ok = ema50 < ema200
        price_above_ema = float(last["Close"]) < ema50
        ema_slope_ok = ema50 < ema50_prev # Must be falling

    if not trend_direction_ok:
        return False, f"Opposite EMA trend ({'Bullish' if ema50 > ema200 else 'Bearish'})", confidence
        
    if not price_above_ema:
        return False, "Price on wrong side of EMA50", confidence
        
    if not ema_slope_ok:
        return False, f"EMA50 slope not {direction.lower()}-aligned", confidence

    # ==========================================
    # 4. PRICE ACTION MOMENTUM
    # ==========================================
    candle_size = abs(float(last["Close"]) - float(last["Open"]))
    avg_candle = float((df["Close"] - df["Open"]).abs().tail(10).mean())
    
    # Require current candle body to be at least 60% of average candle (slightly more relaxed but still strict)
    if candle_size < (avg_candle * 0.6):
        return False, "Weak price action (Doji or small body)", confidence

    # --- Apply Penalties for Minor Weaknesses ---
    penalties = 0
    if atr < atr_mean: penalties += 5
    if candle_size < avg_candle: penalties += 5
    
    # If RSI is near boundaries, apply penalty
    if direction == "CALL" and rsi_value > 60: penalties += 10
    if direction == "PUT" and rsi_value < 40: penalties += 10
    
    confidence -= penalties
    if confidence < 70:
        return False, f"Confidence dropped below 70 after penalties (-{penalties})", confidence

    return True, "Safety passed", confidence


def _is_strong_martingale(df: pd.DataFrame, direction: str) -> bool:
    if not validate_martingale_signal(df, direction):
        return False

    confidence = calculate_confidence(df, direction)
    return confidence >= 75


def _should_take_signal(df: pd.DataFrame, direction: str, confidence: int, is_next_signal: bool) -> tuple[bool, str]:
    safety_ok, safety_reason = _check_safety_rules(df, direction, confidence)
    if not safety_ok:
        return False, safety_reason, confidence

    # 2. Market Safety Engine (Session, Volatility, Wicks, Sideways)
    market_ok, market_msg, penalty = run_market_safety(df, direction)
    confidence -= penalty
    if not market_ok:
        return False, f"Market Safety: {market_msg}", confidence

    # 3. Live Confirmation Engine (Sudden Reversals, Momentum, Spikes)
    live_ok, live_msg = validate_live_signal(df, direction)
    if not live_ok:
        return False, f"Live Confirmation: {live_msg}", confidence

    # 4. Signal Confirmation (Confidence Threshold)
    # First signal is strict: only high-confidence entries.
    base_threshold = 70 if is_next_signal else 75
    threshold = get_adaptive_trade_threshold(base_threshold)

    if confidence >= threshold:
        return True, f"confidence >= {threshold}", confidence

    if is_next_signal:
        return False, f"next signal confidence below {threshold}", confidence

    return False, f"first signal confidence below {threshold}", confidence


def _is_trade_direction(direction: str) -> bool:
    return str(direction).upper() in {"CALL", "BUY"}


def _get_resolved_trades() -> List[dict]:
    return [entry for entry in manager.tracked_trades if entry.get("resolved")]


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

    confidence_buckets = {">= 80": [], "70-79": [], "<70": []}
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

    # Per-source breakdown
    source_stats: dict = {}
    for trade in resolved:
        src = trade.get("source", "telegram")
        if src not in source_stats:
            source_stats[src] = {"total": 0, "wins": 0}
        source_stats[src]["total"] += 1
        if trade.get("result") == "WIN":
            source_stats[src]["wins"] += 1
    for src, s in source_stats.items():
        s["win_rate"] = (s["wins"] / s["total"] * 100) if s["total"] else 0.0

    recent_10 = _get_recent_resolved_trades(10)

    return {
        "total_trades": resolved_total,
        "wins": resolved_wins,
        "losses": resolved_losses,
        "win_rate": overall_win_rate,
        "recent_10_win_rate": _win_rate(recent_10),
        "confidence_buckets": bucket_stats,
        "source_stats": source_stats,
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


def store_tracked_signal(
    signal_time: datetime,
    direction: str,
    entry_price: float,
    expiry_time: datetime,
    signal_type: str,
    pair: str,
    confidence: float,
    df: pd.DataFrame,
    source: str = "telegram",
) -> None:
    last = df.iloc[-1]
    manager.tracked_trades.append({
        "signal_time": signal_time,
        "direction": direction,
        "entry_price": float(entry_price),
        "expiry_time": expiry_time,
        "signal_type": signal_type,
        "pair": pair,
        "confidence": float(confidence),
        "source": source,
        "rsi": float(last["RSI"]) if not pd.isna(last["RSI"]) else None,
        "atr": float(last["ATR"]) if not pd.isna(last["ATR"]) else None,
        "ema_trend": _get_ema_trend(df, direction),
        "resolved": False,
    })

    # Memory management: keep only last 200 trades
    if len(manager.tracked_trades) > 200:
        manager.tracked_trades[:] = manager.tracked_trades[-200:]
    
    # Save to manager JSON for persistence
    manager.save()



def _build_performance_report() -> str:
    stats = get_trade_performance()
    lines = [
        f"📊 Performance Report",
        f"Total Trades: {stats['total_trades']}",
        f"Wins: {stats['wins']}",
        f"Losses: {stats['losses']}",
        f"Win Rate: {stats['win_rate']:.1f}%",
        f"",
        f"Win Rate >=80: {stats['confidence_buckets'].get('>= 80', {}).get('win_rate', 0):.1f}%",
        f"Win Rate 70-79: {stats['confidence_buckets'].get('70-79', {}).get('win_rate', 0):.1f}%",
        f"Win Rate <70: {stats['confidence_buckets'].get('<70', {}).get('win_rate', 0):.1f}%",
    ]
    source_stats = stats.get("source_stats", {})
    if source_stats:
        lines.append("")
        lines.append("*By Source*")
        badges = {"telegram": "📨", "generated": "🤖", "auto": "⚡", "forced": "🔒"}
        for src, s in source_stats.items():
            badge = badges.get(src, "•")
            lines.append(f"{badge} {src.title()}: {s['wins']}W/{s['total'] - s['wins']}L ({s['win_rate']:.0f}%)")
    return "\n".join(lines)


def _maybe_build_daily_report(now: datetime) -> Optional[str]:
    report_date = now.date() - timedelta(days=1)
    if manager.last_daily_report_date == report_date:
        return None

    resolved_for_day = [
        trade for trade in _get_resolved_trades()
        if trade.get("signal_time") is not None and trade["signal_time"].date() == report_date
    ]

    if not resolved_for_day:
        return None

    total = len(resolved_for_day)
    wins = sum(1 for t in resolved_for_day if t.get("result") == "WIN")
    losses = total - wins
    win_rate = (wins / total * 100) if total else 0.0
    best_trades = sum(1 for t in resolved_for_day if float(t.get("confidence") or 0) >= 80 and t.get("result") == "WIN")

    # Per-source breakdown
    source_lines = []
    source_map: dict = {}
    for t in resolved_for_day:
        src = t.get("source", "telegram")
        if src not in source_map:
            source_map[src] = {"w": 0, "l": 0}
        if t.get("result") == "WIN":
            source_map[src]["w"] += 1
        else:
            source_map[src]["l"] += 1
    badges = {"telegram": "📨", "generated": "🤖", "auto": "⚡"}
    for src, s in source_map.items():
        src_total = s["w"] + s["l"]
        src_rate = (s["w"] / src_total * 100) if src_total else 0.0
        badge = badges.get(src, "•")
        source_lines.append(f"{badge} {src.title()}: {s['w']}W/{s['l']}L ({src_rate:.0f}%)")

    manager.last_daily_report_date = report_date
    manager.save()

    report = (
        f"📊 Performance Report\n"
        f"Total Trades: {total}\n"
        f"Wins: {wins}\n"
        f"Losses: {losses}\n"
        f"Win Rate: {win_rate:.1f}%\n"
        f"Best Trades: {best_trades}"
    )
    if source_lines:
        report += "\n\n*By Source*\n" + "\n".join(source_lines)
    return report


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
    manager.total_trades += 1
    if entry.get("result") == "WIN":
        manager.wins += 1
    else:
        manager.losses += 1



# ─────────────────────────────────────────────────────────────────────────────
# Forced signal message builders
# ─────────────────────────────────────────────────────────────────────────────

def _build_forced_pre_message(signal_time: datetime, direction: str, confidence: int,
                              df: Optional[pd.DataFrame], low_confidence: bool,
                              is_martingale: bool) -> str:
    """PRE-SIGNAL message for forced 15:05 / 15:10 signals."""
    label = "🎲 MARTINGALE PRE-ALERT" if is_martingale else "📊 PRE-SIGNAL"
    risk_tag = "\n⚠️ *LOW CONFIDENCE / RISKY MARKET*" if low_confidence else ""
    analysis = ""
    if df is not None and len(df) >= 2:
        last = df.iloc[-1]
        rsi = float(last["RSI"]) if not pd.isna(last.get("RSI", float("nan"))) else 0
        analysis = (
            f"\n*Technical Analysis*\n"
            f"EMA Trend: {_get_ema_trend(df, direction)}\n"
            f"RSI: {rsi:.0f} ({_get_rsi_interpretation(rsi)})\n"
            f"ATR: {_get_atr_strength(df)}\n"
            f"Candle: {_get_candle_strength(df)}\n"
        )
    return (
        f"{label} ⚡ *FORCED*\n\n"
        f"Pair: EURUSD\n"
        f"Direction: {direction}\n"
        f"Time: {signal_time:%H:%M}\n\n"
        f"Confidence: {confidence}%{risk_tag}\n"
        f"{analysis}\n"
        f"Status: Preparing for entry ⏳"
    )


def _build_forced_confirm_message(signal_time: datetime, direction: str, confidence: int,
                                   expiry: datetime, tp: float, sl: float,
                                   df: Optional[pd.DataFrame], low_confidence: bool,
                                   is_martingale: bool, direction_flipped: bool) -> str:
    """CONFIRMATION message for forced 15:05 / 15:10 signals."""
    label = "🎲 MARTINGALE CONFIRMED" if is_martingale else "📊 SIGNAL CONFIRMED"
    risk_lines = []
    if low_confidence:
        risk_lines.append("⚠️ *LOW CONFIDENCE / RISKY MARKET*")
    if is_martingale and confidence < FORCED_MARTINGALE_MIN_CONFIDENCE:
        risk_lines.append("🔴 *High Risk Martingale*")
    if direction_flipped:
        risk_lines.append("🔄 Direction updated by live confirmation")
    risk_tag = "\n" + "\n".join(risk_lines) if risk_lines else ""
    analysis = ""
    if df is not None and len(df) >= 2:
        last = df.iloc[-1]
        rsi = float(last["RSI"]) if not pd.isna(last.get("RSI", float("nan"))) else 0
        analysis = (
            f"\n*Technical Analysis*\n"
            f"EMA Trend: {_get_ema_trend(df, direction)}\n"
            f"RSI: {rsi:.0f} ({_get_rsi_interpretation(rsi)})\n"
            f"ATR: {_get_atr_strength(df)}\n"
            f"Candle: {_get_candle_strength(df)}\n"
        )
    return (
        f"{label} ⚡ *FORCED*\n\n"
        f"Pair: EURUSD\n"
        f"Direction: {direction}\n"
        f"Source: ⚡ Forced Daily\n"
        f"{risk_tag}\n"
        f"Confidence: {confidence}%\n"
        f"{analysis}\n"
        f"*Trade Details*\n"
        f"Entry Time: {signal_time:%H:%M}\n"
        f"Expiry: {expiry:%H:%M}\n"
        f"TP: {tp}\n"
        f"SL: {sl}"
    )


def _process_forced_signals(
    messages: List[str],
    active_signals: List[dict],
    minute_df: Optional[pd.DataFrame],
    df: pd.DataFrame,
    now: datetime,
) -> None:
    """
    Process compulsory 15:05 (direct) and 15:10 (martingale) signals.

    Rules:
    • Always sends PRE-SIGNAL 60-120 s before entry.
    • Always sends CONFIRMATION near entry time.
    • Re-checks direction live at confirmation; may flip if original is invalidated.
    • Bypasses processed_signals dedup using 'forced_' prefixed keys.
    • Adds LOW CONFIDENCE / RISKY MARKET or High Risk Martingale warnings as needed.
    • Never skips due to confidence thresholds – only warns.
    """
    work_df = minute_df if (minute_df is not None and len(minute_df) >= 50) else df

    for signal in active_signals:
        try:
            if not signal.get("is_forced"):
                continue

            signal_time: datetime = signal["time"]
            if signal_time.date() != now.date():
                continue

            is_martingale = signal.get("forced_type") == "martingale"
            # Use a dedup key that cannot collide with regular signals
            forced_key = f"forced_{signal_time:%H:%M}_{'MG' if is_martingale else 'DIR'}"

            if manager.processed_signals.issuperset({forced_key}):
                continue  # Already confirmed today

            # ── Direction: start with stored, re-decide at confirmation ───
            stored_direction: str = signal["direction"]
            stored_confidence: int = int(signal.get("stored_confidence", 0))
            low_confidence: bool = signal.get("low_confidence", bool(stored_confidence < FORCED_SIGNAL_LOW_CONF_THRESHOLD))

            # ── PRE-SIGNAL (60-120 s before entry) ───────────────────────
            seconds_to_entry = (signal_time - now).total_seconds()
            if PRE_SIGNAL_MIN_SECONDS <= seconds_to_entry <= PRE_SIGNAL_MAX_SECONDS and not signal.get("pre_sent"):
                # Re-calculate live direction for the pre-signal message
                live_dir, live_conf = decide_direction_live(work_df)
                signal["direction"] = live_dir
                signal["stored_confidence"] = live_conf
                low_confidence = live_conf < FORCED_SIGNAL_LOW_CONF_THRESHOLD
                signal["low_confidence"] = low_confidence

                msg = _build_forced_pre_message(signal_time, live_dir, live_conf, work_df, low_confidence, is_martingale)
                messages.append(msg)
                signal["pre_sent"] = True
                signal["pre_confidence"] = live_conf
                logger.info(f"[FORCED] PRE sent: {'MG' if is_martingale else 'DIR'} {signal_time:%H:%M} {live_dir} conf={live_conf}%")

            # ── CONFIRMATION (within SIGNAL_WINDOW_SECONDS of entry) ──────
            if abs(seconds_to_entry) <= SIGNAL_WINDOW_SECONDS and not signal.get("confirmed_sent"):
                # Re-check direction live – may flip from original
                live_dir, live_conf = decide_direction_live(work_df)
                direction_flipped = live_dir != signal["direction"]
                if direction_flipped:
                    logger.info(f"[FORCED] Direction flipped {signal['direction']} → {live_dir} at confirmation")
                    signal["direction"] = live_dir

                low_confidence = live_conf < FORCED_SIGNAL_LOW_CONF_THRESHOLD
                signal["low_confidence"] = low_confidence

                # Use work_df for TP/SL even if data is thin
                conf_df = work_df if (work_df is not None and len(work_df) >= 2) else df
                try:
                    _, tp, sl = build_forex_targets(conf_df, live_dir, live_conf)
                except Exception:
                    atr_val = float(conf_df.iloc[-1]["ATR"]) if conf_df is not None and len(conf_df) >= 1 else 0.0005
                    close_val = float(conf_df.iloc[-1]["Close"]) if conf_df is not None and len(conf_df) >= 1 else 1.0
                    tp = round(close_val + atr_val, 5) if live_dir == "CALL" else round(close_val - atr_val, 5)
                    sl = round(close_val - atr_val, 5) if live_dir == "CALL" else round(close_val + atr_val, 5)

                expiry = signal_time + timedelta(minutes=5)
                msg = _build_forced_confirm_message(
                    signal_time, live_dir, live_conf, expiry, tp, sl,
                    conf_df, low_confidence, is_martingale, direction_flipped
                )
                messages.append(msg)
                signal["confirmed_sent"] = True
                signal["martingale_confidence"] = live_conf
                manager.processed_signals.add(forced_key)

                # Track trade for performance analytics
                try:
                    entry_price = float(conf_df.iloc[-1]["Close"]) if conf_df is not None and len(conf_df) >= 1 else 0.0
                    store_tracked_signal(
                        signal_time=signal_time,
                        direction=live_dir,
                        entry_price=entry_price,
                        expiry_time=expiry,
                        signal_type="martingale" if is_martingale else "direct",
                        pair="EURUSD",
                        confidence=live_conf,
                        df=conf_df,
                        source="forced",
                    )
                except Exception as te:
                    logger.error(f"[FORCED] store_tracked_signal error: {te}")

                logger.info(
                    f"[FORCED] CONFIRMED: {'MG' if is_martingale else 'DIR'} {signal_time:%H:%M} "
                    f"{live_dir} conf={live_conf}% low={low_confidence}"
                )
        except Exception as e:
            logger.error(f"[FORCED] Processing error: {e}")


def process_signal_list(
    df: pd.DataFrame,
    minute_data_fetcher: Optional[Callable[[], Optional[pd.DataFrame]]] = None,
) -> List[str]:
    # Load tracked signals from JSON on first call
    if not manager._initialized:
        manager.load()
        manager._initialized = True

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
            if minute_df is not None and len(minute_df) >= 200:
                minute_df = add_indicators(minute_df)
        except Exception:
            minute_df = None

    # 1) Check for any tracked signals that have expired and report results
    try:
        for entry in list(manager.tracked_trades):
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

                # Feed learning engine (per source)
                try:
                    learning_engine.record_trade(
                        time_of_day=entry["signal_time"].strftime("%H:%M") if isinstance(entry["signal_time"], datetime) else str(entry["signal_time"]),
                        direction=entry.get("direction", "CALL"),
                        confidence=int(entry.get("confidence") or 0),
                        atr=float(entry.get("atr") or 0),
                        rsi=float(entry.get("rsi") or 50),
                        result=entry["result"],
                        source=entry.get("source", "telegram"),
                    )
                except Exception as le:
                    logger.error(f"LearningEngine record error: {le}")

                # Save updated tracked signals to JSON
                manager.save()
    except Exception as e:
        logger.error(f"Result tracking error: {e}")

    # ── FORCED DAILY SIGNALS (15:05 direct + 15:10 martingale) ──────────────
    # These ALWAYS send PRE + CONFIRMATION regardless of confidence or safety
    # thresholds. Low confidence signals carry a warning, not a skip.
    _process_forced_signals(messages, manager.active_signals, minute_df, df, now)
    # ── END FORCED DAILY SIGNALS ─────────────────────────────────────────────

    for signal in manager.active_signals:
        try:
            signal_time = signal["time"]
            direction = signal["direction"]

            if signal_time.date() != now.date():
                continue
            if signal_time.time() < MARKET_OPEN or signal_time.time() > MARKET_CLOSE:
                continue

            base_key = _signal_key(signal_time, direction)

            # Use cached 1-minute data for PRE-SIGNAL confidence when available.
            # Determine whether this is a 'next' signal for thresholding.
            is_next_signal = manager.evaluated_signal_count > 0
            base_threshold = 70 if is_next_signal else 75
            threshold = get_adaptive_trade_threshold(base_threshold)

            # Prefer minute-level data for pre-signal calculation; fallback to provided df.
            pre_df = minute_df if (minute_df is not None and len(minute_df) >= 200) else df
            pre_confidence = calculate_confidence(pre_df, direction)

            seconds_to_entry = (signal_time - now).total_seconds()
            if PRE_SIGNAL_MIN_SECONDS <= seconds_to_entry <= PRE_SIGNAL_MAX_SECONDS and not signal.get("pre_sent"):
                # Only send pre-signal when the confidence meets the same adaptive threshold
                # AND it passes technical safety (ATR, RSI, Trend)
                safety_ok, safety_reason, pre_confidence = _check_safety_rules(pre_df, direction, pre_confidence)
                if pre_confidence >= threshold and safety_ok:
                    messages.append(_build_pre_message(signal, pre_confidence, pre_df))
                    signal["pre_sent"] = True
                elif not safety_ok and "(GENERATED)" in signal.get("raw_line", ""):
                    # Log why a generated signal was skipped at pre-signal stage
                    logger.info(f"Skipping generated pre-signal at {signal['time']:%H:%M} - {safety_reason}")
                    signal["pre_sent"] = True # Mark as "sent" to avoid repeat checks

                if signal.get("pre_sent"):
                    # Persist the pre-calculated confidence so confirmation uses the same value
                    signal["pre_confidence"] = pre_confidence
                    logger.debug(f"FINAL CONFIDENCE USED: {pre_confidence}%")

            if abs((now - signal_time).total_seconds()) <= SIGNAL_WINDOW_SECONDS:
                if base_key in manager.processed_signals:
                    continue

                signal_df = minute_df

                if signal_df is None or len(signal_df) < 200:
                    signal_df = df

                # ALWAYS re-calculate confidence and run full re-checks at confirmation.
                # Do NOT blindly trust pre-calculated values or timings.
                confidence = calculate_confidence(signal_df, direction)
                is_next_signal = manager.evaluated_signal_count > 0
                
                # Run full suite of checks: Indicators, Market Safety, and Live Confirmation
                should_take, skip_reason, confidence = _should_take_signal(signal_df, direction, confidence, is_next_signal)

                # Debug log final confidence used for decision
                logger.debug(f"FINAL CONFIDENCE USED: {confidence}%")

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
                            source=signal.get("source", "telegram"),
                        )
                    except Exception as e:
                        logger.error(f"store_tracked_signal error: {e}")
                    signal["confirmed_sent"] = True
                    signal["martingale_confidence"] = confidence
                    manager.confirmed_signal_count += 1
                    logger.info(f"Signal confirmed: {signal_time:%H:%M} {direction}")
                else:
                    # Market weakened between pre-signal and confirmation - skip safely
                    logger.info(f"Skipping confirmation for {signal_time:%H:%M} {direction}: {skip_reason}")
                    if signal.get("pre_sent"):
                        skip_msg = (
                            f"⚠️ *SIGNAL SKIPPED*\n\n"
                            f"Pair: {signal.get('pair', 'EURUSD')}\n"
                            f"Time: {signal_time:%H:%M}\n\n"
                            f"Signal skipped due to weak market:\n"
                            f"{skip_reason}"
                        )
                        # DO NOT append skip_msg to prevent Telegram spam
                        # messages.append(skip_msg)

                manager.processed_signals.add(base_key)
                manager.evaluated_signal_count += 1

            if not manager.enable_martingale:
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
                    logger.debug(f"FINAL CONFIDENCE USED: {mg_confidence}%")
                    signal["martingale_prealert_sent"] = True

            if abs((now - mg_time).total_seconds()) <= SIGNAL_WINDOW_SECONDS:
                # Prefer 1-minute data for martingale confirmation; fallback to main df
                mg_df = minute_df if (minute_df is not None and len(minute_df) >= 200) else df
                if mg_key not in manager.processed_signals and _is_strong_martingale(mg_df, direction):
                    mg_confidence = calculate_confidence(mg_df, direction)
                    signal["martingale_confidence"] = mg_confidence
                    logger.debug(f"FINAL CONFIDENCE USED: {mg_confidence}%")
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
                            source=signal.get("source", "telegram"),
                        )
                    except Exception as e:
                        logger.error(f"store_tracked_signal error: {e}")
                    manager.processed_signals.add(mg_key)
                    signal["martingale_confirmed_sent"] = True
                    logger.info(f"Signal confirmed: {mg_time:%H:%M} {direction}")
        except Exception as e:
            logger.error(f"Processing error: {e}")
            continue

    return messages
