from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import json
import os
import re
import time
from typing import Callable, List, Optional
import logging

import pandas as pd

# Simple logging setup - reduce console spam
LOG_LEVEL = os.getenv("LOG_LEVEL", "WARNING")  # Default to WARNING in production
logging.basicConfig(level=LOG_LEVEL, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

_last_log_time = {}  # Track last log time to reduce spam

def _log_throttle(key: str, min_interval: int = 60) -> bool:
    """Throttle logging to reduce spam. Returns True if should log."""
    global _last_log_time
    now = time.time()
    if key not in _last_log_time or now - _last_log_time[key] > min_interval:
        _last_log_time[key] = now
        return True
    return False

from indicators import add_indicators
from market_cache import get_processed_df
from signal_sources import global_signal_manager
from confidence_engine import calculate_real_confidence
from confirmation_engine import validate_live_signal
import learning_engine
from martingale_safety import (
    can_enter_martingale,
    record_martingale_result,
    get_martingale_safety_engine,
    get_martingale_status,
)
from market_safety import (
    check_market_safety,
    is_optimal_session,
    get_current_session,
)
from news_filter import is_news_blackout, get_news_warning_message, cleanup_old_events
from market_cache import is_api_safety_mode_active
from smart_signal_manager import (
    get_signal_manager,
    create_signal,
)
from telegram_queue import (
    send_telegram_queued,
    TelegramFormatter,
    MessageType,
)

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
SIGNAL_PATTERN = re.compile(r"^(?P<hour>\d{2}):(?P<minute>\d{2})\s+EURUSD\s+(?P<signal>CALL|PUT|SKIP)$")


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
 
last_daily_report_date = None

def reset_daily_state() -> None:
    """
    Reset daily state for day change.
    Called by day_reset module at market open.
    """
    global signal_list, processed_signals, last_update_id, last_signal_update_time
    global evaluated_signal_count, confirmed_signal_count
    
    # Clear signals and processing state
    signal_list.clear()
    processed_signals.clear()
    last_update_id = None
    last_signal_update_time = None
    evaluated_signal_count = 0
    confirmed_signal_count = 0
    
    print("  ✓ Signal list daily state reset")


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


def _build_signal_state(entries: List[SignalEntry], boost: int, skip: bool) -> dict:
    return {
        "time": entries[0].signal_time,
        "pair": entries[0].pair,
        "direction": entries[0].direction,
        "pre_sent": False,
        "confirmed_sent": False,
        "martingale_time": entries[0].signal_time + MARTINGALE_ENTRY_DELAY,
        "martingale_prealert_sent": False,
        "martingale_confirmed_sent": False,
        "martingale_confidence": 0,
        "boost": boost,
        "skip": skip
    }


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


def sync_with_signal_manager(now: Optional[datetime] = None) -> List[dict]:
    global signal_list, processed_signals, last_signal_update_time, evaluated_signal_count, confirmed_signal_count

    if now is None:
        now = _now()

    current_day = now.date()
    previous_day = last_signal_update_time.date() if last_signal_update_time is not None else None

    if previous_day != current_day:
        signal_list = []
        processed_signals = set()
        evaluated_signal_count = 0
        confirmed_signal_count = 0

    merged_signals = global_signal_manager.get_merged_signals()
    
    # Build a dictionary of current signals for easy lookup
    current_state_map = { _signal_key(sig["time"], sig["direction"]): sig for sig in signal_list }
    
    new_signal_list = []
    
    for ms in merged_signals:
        entry = _parse_line(ms["line"], current_day)
        if entry is None:
            continue
            
        key = _signal_key(entry.signal_time, entry.direction)
        if key in current_state_map:
            sig = current_state_map[key]
            sig["boost"] = ms["boost"]
            sig["skip"] = ms["skip"]
            new_signal_list.append(sig)
        else:
            new_signal_list.append(_build_signal_state([entry], ms["boost"], ms["skip"]))
            
    signal_list = new_signal_list
    last_signal_update_time = now
    return signal_list


def apply_signal_text(signal_text: str, now: Optional[datetime] = None) -> List[dict]:
    from signal_sources import SignalSource
    if not isinstance(signal_text, str):
        return sync_with_signal_manager(now)

    lines = [line.strip() for line in signal_text.splitlines() if line.strip()]
    if lines:
        global_signal_manager.add_signals(lines, SignalSource.EXTERNAL)
    return sync_with_signal_manager(now)

def update_signal_list(signal_lines: Optional[List[str]] = None, now: Optional[datetime] = None) -> List[dict]:
    from signal_sources import SignalSource
    if signal_lines:
        global_signal_manager.add_signals(signal_lines, SignalSource.INTERNAL)
    return sync_with_signal_manager(now)


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


# DEPRECATED: This validation logic is now handled in _check_safety_rules()
# Kept for backward compatibility - returns True always
def validate_sniper_signal(df: pd.DataFrame, direction: str) -> bool:
    return True


# DEPRECATED: Martingale validation now handled by martingale_safety.py
def validate_martingale_signal(df: pd.DataFrame, direction: str) -> bool:
    return True



def build_forex_targets(df: pd.DataFrame, direction: str, confidence: int) -> tuple[float, float, float]:
    df = get_processed_df()
    if df is None or len(df) < 200:
        return 0.0, 0.0, 0.0
    
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


def _build_pre_message(signal: dict, confidence: int, df: pd.DataFrame, source: str = "direct") -> str:
    """Build pre-signal message using new TelegramFormatter."""
    return TelegramFormatter.format_pre_signal(
        pair=signal['pair'],
        direction=signal['direction'],
        confidence=confidence,
        entry="Pending",
        time=f"{signal['time']:%H:%M}",
        source=source,
    )


def _build_confirm_message(signal: dict, confidence: int, expiry: datetime, tp: float, sl: float, df: pd.DataFrame, source: str = "direct") -> str:
    """Build confirmed signal message using new TelegramFormatter."""
    return TelegramFormatter.format_confirmed_signal(
        pair=signal['pair'],
        direction=signal['direction'],
        confidence=confidence,
        entry=f"{signal['time']:%H:%M}",
        expiry=f"{expiry:%H:%M}",
        tp=f"{tp}",
        sl=f"{sl}",
        source=source,
    )


def _check_safety_rules(df: pd.DataFrame, direction: str, confidence: int) -> tuple[bool, str]:
    # ── Global safety gates (always enforced, cannot be bypassed) ──
    # 1. API failure safety mode
    if is_api_safety_mode_active():
        return False, "API safety mode active — stale data"

    # 2. News blackout window
    blackout, blackout_reason = is_news_blackout()
    if blackout:
        return False, blackout_reason

    if confidence < 70:
        return False, "confidence below 70"

    if df is None or len(df) < 200:
        return False, "insufficient data"

    df = get_processed_df()
    if df is None or len(df) < 200:
        return False, "insufficient data"

    last = df.iloc[-1]
    prev = df.iloc[-2]
    prev2 = df.iloc[-3]
    lookback_5 = df.iloc[-5]

    required_values = [
        last["EMA50"], last["EMA200"], last["RSI"], prev["RSI"], last["ATR"]
    ]
    if any(pd.isna(value) for value in required_values):
        return False, "indicator values unavailable"

    atr = float(last["ATR"])
    atr_mean = float(df["ATR"].mean())
    rsi_value = float(last["RSI"])
    rsi_prev = float(prev["RSI"])

    # ── High-Confidence Fast Mode: bypass soft secondary filters at ≥82 ──
    fast_mode = confidence >= 82

    # 1. HARD REJECTION ZONES (always enforced)
    if direction == "CALL" and rsi_value > 78:
        return False, "RSI > 78 (Hard Rejection)"
    if direction == "PUT" and rsi_value < 22:
        return False, "RSI < 22 (Hard Rejection)"

    # 2. RSI MOMENTUM & STRONG ZONES — relaxed further
    if direction == "CALL":
        if rsi_value <= rsi_prev and not fast_mode:
            return False, "RSI momentum not increasing"
        if rsi_value < 52 and not fast_mode:  # was 55 — relaxed further to 52
            return False, f"RSI {rsi_value:.1f} < 52 (Weak Zone)"
    else:
        if rsi_value >= rsi_prev and not fast_mode:
            return False, "RSI momentum not decreasing"
        if rsi_value > 48 and not fast_mode:  # was 45 — relaxed further to 48
            return False, f"RSI {rsi_value:.1f} > 45 (Weak Zone)"

    # 3. EMA SLOPE DETECTION
    ema50 = float(last["EMA50"])
    ema50_prev = float(prev["EMA50"])
    ema50_prev2 = float(prev2["EMA50"])

    if direction == "CALL":
        if not (ema50 > ema50_prev) and not fast_mode:
            return False, "EMA50 not rising"
    else:
        if not (ema50 < ema50_prev) and not fast_mode:
            return False, "EMA50 not falling"

    # 4. DIVERGENCE REJECTION - relaxed significantly
    close_now = float(last["Close"])
    close_5 = float(lookback_5["Close"])
    rsi_5 = float(lookback_5["RSI"])
    
    # Only reject strong divergence in non-fast mode
    if direction == "CALL":
        if close_now > close_5 * 1.005 and rsi_value < rsi_5 - 10 and not fast_mode:  # was close_now > close_5
            return False, "Strong Bearish Divergence"
    else:
        if close_now < close_5 * 0.995 and rsi_value > rsi_5 + 10 and not fast_mode:  # was close_now < close_5
            return False, "Strong Bullish Divergence"

    # Distance to EMA - relaxed significantly
    distance_to_ema = abs(close_now - ema50)
    if distance_to_ema > (atr * 5.0) and not fast_mode:  # was 3.5
        return False, "Price far from EMA50"

    # Candle exhaustion - relaxed
    candle_size = abs(close_now - float(last["Open"]))
    avg_candle = float((df["Close"] - df["Open"]).abs().tail(10).mean())
    if candle_size > (avg_candle * 4.5) and not fast_mode:  # was 3.0
        return False, "Large blow-off candle"

    # 6. BASIC TREND CHECK vs EMA200 - relaxed
    trend_gap = abs(float(last["EMA50"]) - float(last["EMA200"]))
    trend_threshold = max(0.00010, atr * 0.08)  # was 0.00015 / 0.12
    
    if direction == "CALL":
        trend_direction_ok = float(last["EMA50"]) > float(last["EMA200"])
    else:
        trend_direction_ok = float(last["EMA50"]) < float(last["EMA200"])

    # Only reject completely reversed trend in non-fast mode
    if not trend_direction_ok and not fast_mode:
        return False, "trend reversed vs EMA200"
    
    # Remove flat trend rejection - allow more setups
    
    # Weak ATR — relaxed significantly
    if atr <= atr_mean * 0.35 and not fast_mode:  # was 0.5
        return False, "very weak volatility"

    mode_tag = " [FastMode]" if fast_mode else ""
    return True, f"safety passed{mode_tag}"


def _is_strong_martingale(df: pd.DataFrame, direction: str, confidence: int, signal_key: str, now: datetime) -> bool:
    """
    Check if martingale is safe using the comprehensive safety engine.
    """
    allowed, reason = can_enter_martingale(df, direction, confidence, signal_key, now)
    if not allowed:
        # Only log occasionally to reduce spam
        return False
    return True


def _should_take_signal(df: pd.DataFrame, direction: str, confidence: int, is_next_signal: bool) -> tuple[bool, str]:
    safety_ok, safety_reason = _check_safety_rules(df, direction, confidence)
    if not safety_ok:
        return False, safety_reason

    # First signal is strict: only high-confidence entries - relaxed slightly
    base_threshold = 68 if is_next_signal else 73
    threshold = get_adaptive_trade_threshold(base_threshold)

    if confidence >= threshold:
        return True, f"confidence >= {threshold}"

    if is_next_signal:
        return False, f"next signal confidence below {threshold}"

    return False, f"first signal confidence below {threshold}"


def _is_trade_direction(direction: str) -> bool:
    return str(direction).upper() in {"CALL", "BUY"}


def _confidence_bucket(confidence: float) -> str:
    if confidence >= 80:
        return ">=80"
    if confidence >= 70:
        return "70-79"
    return "<70"


def get_trade_performance() -> dict:
    """Wrapper for SmartSignalManager stats to maintain compatibility."""
    from smart_signal_manager import get_signal_stats
    return get_signal_stats()


def get_adaptive_trade_threshold(base_threshold: int = 70) -> int:
    return learning_engine.adapt_confidence_threshold(base_threshold)






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

    from smart_signal_manager import get_signal_manager
    mgr = get_signal_manager()
    resolved_for_day = [
        sig for sig in mgr.signals.values()
        if sig.resolved and sig.signal_time.date() == report_date
    ]

    if not resolved_for_day:
        return None

    day_stats = {
        "total_trades": len(resolved_for_day),
        "wins": sum(1 for sig in resolved_for_day if sig.result == "WIN"),
        "losses": sum(1 for sig in resolved_for_day if sig.result == "LOSS"),
    }
    day_stats["win_rate"] = (day_stats["wins"] / day_stats["total_trades"] * 100) if day_stats["total_trades"] else 0.0
    day_stats["best_trades"] = sum(1 for sig in resolved_for_day if sig.confidence >= 80 and sig.result == "WIN")

    last_daily_report_date = report_date

    # Add safety engine status to report
    safety_status = get_martingale_status()
    
    # Use new TelegramFormatter for daily report
    return TelegramFormatter.format_daily_report(
        date=f"{report_date:%Y-%m-%d}",
        total=day_stats['total_trades'],
        wins=day_stats['wins'],
        losses=day_stats['losses'],
        win_rate=day_stats['win_rate'],
        safety_status=safety_status,
    )


def _build_mg_pre_message(signal: dict, confidence: int, df: pd.DataFrame) -> str:
    """Build martingale pre-alert message using TelegramFormatter."""
    return TelegramFormatter.format_pre_signal(
        pair=signal['pair'],
        direction=signal['direction'],
        confidence=confidence,
        entry="Pending",
        time=f"{signal['martingale_time']:%H:%M}",
        source="martingale",
    )


def _build_mg_confirm_message(signal: dict, confidence: int, expiry: datetime, tp: float, sl: float, df: pd.DataFrame) -> str:
    """Build martingale confirmed message using TelegramFormatter."""
    return TelegramFormatter.format_confirmed_signal(
        pair=signal['pair'],
        direction=signal['direction'],
        confidence=confidence,
        entry=f"{signal['martingale_time']:%H:%M}",
        expiry=f"{expiry:%H:%M}",
        tp=f"{tp}",
        sl=f"{sl}",
        source="martingale",
    )




def process_signal_list(
    df: pd.DataFrame,
    minute_data_fetcher: Optional[Callable[[], Optional[pd.DataFrame]]] = None,
) -> List[str]:
    global signal_list, processed_signals, evaluated_signal_count, confirmed_signal_count

    if df is None or len(df) < 200:
        return []

    now = _now()
    messages: List[str] = []

    daily_report = _maybe_build_daily_report(now)
    if daily_report:
        messages.append(daily_report)

    # Get trade stats once for the loop
    trade_stats = get_trade_performance()

    # Fetch 1-minute data ONCE and reuse for all operations
    minute_df = None
    if minute_data_fetcher is not None:
        try:
            minute_df = minute_data_fetcher()
            if minute_df is not None and len(minute_df) >= 200:
                minute_df = add_indicators(minute_df)
        except Exception:
            minute_df = None

    # 1) Result reporting now handled by bot.py calling smart_signal_manager.process_signals()

    for signal in signal_list:
        try:
            signal_time = signal["time"]
            direction = signal["direction"]

            if signal_time.date() != now.date():
                continue
            if signal_time.time() < MARKET_OPEN:
                continue

            base_key = _signal_key(signal_time, direction)
            
            # Use Learning Engine for confidence penalty instead of hard blocking
            learning_penalty = 0
            if not signal.get("skip"):
                learning_penalty = learning_engine.get_confidence_penalty(signal_time, direction)
                if learning_penalty >= 25:  # Only skip for extreme penalty
                    signal["skip"] = True
                    if _log_throttle(f"learn_skip_{signal_time:%H:%M}", 300):
                        logger.info(f"Learning penalty: {learning_penalty}% for {signal_time:%H:%M}")

            if signal.get("skip"):
                if base_key not in processed_signals:
                    processed_signals.add(base_key)
                    processed_signals.add(_signal_key(signal["martingale_time"], direction))
                continue

            # Determine whether this is a 'next' signal for thresholding.
            is_next_signal = evaluated_signal_count > 0
            base_threshold = 68 if is_next_signal else 73
            threshold = get_adaptive_trade_threshold(base_threshold)

            boost = signal.get("boost", 0)

            # ALWAYS use 5m data (df) for indicator-based calculations to avoid fake/high con from 1m noise.
            pre_confidence = calculate_real_confidence(df, direction, boost, trade_stats)
            
            # Apply learning engine penalty to reduce confidence instead of blocking
            if learning_penalty > 0:
                pre_confidence = max(0, pre_confidence - learning_penalty)

            seconds_to_entry = (signal_time - now).total_seconds()
            # PRE-SIGNALS DISABLED - reduce Telegram noise, only send confirmed signals
            # if PRE_SIGNAL_MIN_SECONDS <= seconds_to_entry <= PRE_SIGNAL_MAX_SECONDS and not signal.get("pre_sent"):
            #     if pre_confidence >= threshold:
            #         messages.append(_build_pre_message(signal, pre_confidence, df))
            #         signal["pre_sent"] = True
            #         signal["pre_confidence"] = pre_confidence
# PRE-SIGNALS DISABLED - reduce Telegram noise, only send confirmed signals
            # Always store pre_confidence for use in confirmation
            signal["pre_confidence"] = pre_confidence

            if abs((now - signal_time).total_seconds()) <= SIGNAL_WINDOW_SECONDS:
                if base_key in processed_signals:
                    continue

                # Use any pre-calculated confidence if present so pre-signal and confirmation match.
                pre_conf = signal.get("pre_confidence")
                is_next_signal = evaluated_signal_count > 0
                if pre_conf is not None:
                    confidence = pre_conf
                else:
                    confidence = calculate_real_confidence(df, direction, boost, trade_stats)

                # Debug log final confidence used for decision - only throttled
                if confidence >= 82 and _log_throttle(f"conf_{signal_time:%H:%M}", 180):
                    logger.info(f"Confidence: {confidence}% for {signal_time:%H:%M} {direction}")

                # Re-check safety rules at confirmation time - if market weakens, skip confirmation
                should_take, skip_reason = _should_take_signal(df, direction, confidence, is_next_signal)

                # Market Safety Checks - session and market conditions
                session_ok, session_reason = is_optimal_session(now)
                if not session_ok:
                    should_take = False
                    skip_reason = f"Session: {session_reason}"
                else:
                    market_ok, market_reason = check_market_safety(df, direction, now)
                    if not market_ok:
                        should_take = False
                        skip_reason = f"Market safety: {market_reason}"

                live_ok = True
                live_reason = ""
                if should_take and minute_df is not None:
                    live_ok, live_reason = validate_live_signal(minute_df, direction, confidence)

                if should_take and live_ok:
                    _, tp, sl = build_forex_targets(df, direction, confidence)
                    expiry = signal_time + timedelta(minutes=5)
                    messages.append(_build_confirm_message(signal, confidence, expiry, tp, sl, df))
                    
                    # Smart signal with lifecycle management
                    create_signal(
                        pair=signal.get("pair", "EURUSD"),
                        direction=direction,
                        signal_time=signal_time,
                        expiry_time=expiry,
                        entry_price=float(df.iloc[-1]["Close"]),
                        stop_loss=sl,
                        take_profit=tp,
                        confidence=int(confidence),
                        auto_trade=True # Allow monitoring/closing for external signals too
                    )
                    
                    signal["confirmed_sent"] = True
                    signal["martingale_confidence"] = confidence
                    confirmed_signal_count += 1
                    if _log_throttle(f"confirm_{signal_time:%H:%M}", 120):
                        logger.info(f"Signal confirmed: {signal_time:%H:%M} {direction}")
                else:
                    # Market weakened between pre-signal and confirmation - skip safely
                    reason = skip_reason if not should_take else live_reason
                    if _log_throttle(f"skip_{signal_time:%H:%M}", 180):
                        logger.info(f"Skipped: {signal_time:%H:%M} {direction} - {reason[:50]}")
                    if signal.get("pre_sent") and not signal.get("skip_sent"):
                        # Send skip message using new formatter with clear reason
                        skip_msg = TelegramFormatter.format_skipped_signal(
                            pair=signal.get("pair", "EURUSD"),
                            direction=direction,
                            time=f"{signal_time:%H:%M}",
                            reason=reason,
                            confidence=confidence,
                        )
                        messages.append(skip_msg)
                        signal["skip_sent"] = True

                processed_signals.add(base_key)
                evaluated_signal_count += 1

            if not ENABLE_MARTINGALE:
                continue

            mg_time = signal["martingale_time"]
            mg_key = _signal_key(mg_time, direction)
            seconds_to_mg = (mg_time - now).total_seconds()

            # MARTINGALE PRE-ALERTS DISABLED - reduce Telegram noise
            # if (
            #     MARTINGALE_PREALERT_MIN_SECONDS <= seconds_to_mg <= MARTINGALE_PREALERT_MAX_SECONDS
            #     and not signal.get("martingale_prealert_sent")
            # ):
            #     mg_confidence = calculate_real_confidence(df, direction, boost, trade_stats)
            #     signal["martingale_confidence_pre"] = mg_confidence
            #     
            #     if _is_strong_martingale(df, direction, mg_confidence, mg_key, now):
            #         messages.append(_build_mg_pre_message(signal, mg_confidence, df))
            #         print(f"FINAL CONFIDENCE USED: {mg_confidence}%")
            #         signal["martingale_prealert_sent"] = True
            #     else:
            #         print(f"Martingale prealert blocked for {mg_time:%H:%M} {direction}")
            
            # Always calculate martingale confidence for the confirmation check
            mg_confidence = calculate_real_confidence(df, direction, boost, trade_stats)
            signal["martingale_confidence_pre"] = mg_confidence

            if abs((now - mg_time).total_seconds()) <= SIGNAL_WINDOW_SECONDS:
                if mg_key not in processed_signals:
                    # Get or calculate confidence for safety check
                    pre_mg_conf = signal.get("martingale_confidence_pre")
                    if pre_mg_conf is not None:
                        mg_confidence = pre_mg_conf
                    else:
                        mg_confidence = calculate_real_confidence(df, direction, boost, trade_stats)
                    
                    # Check comprehensive safety rules with confidence
                    should_mg = _is_strong_martingale(df, direction, mg_confidence, mg_key, now)
                    
                    mg_live_ok = True
                    mg_live_reason = ""
                    if should_mg and minute_df is not None:
                        mg_live_ok, mg_live_reason = validate_live_signal(minute_df, direction, mg_confidence)

                    if should_mg and mg_live_ok:
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
                                df,
                             )
                        )
                        # Smart signal for martingale
                        create_signal(
                            pair=signal.get("pair", "EURUSD"),
                            direction=direction,
                            signal_time=mg_time,
                            expiry_time=expiry,
                            entry_price=float(df.iloc[-1]["Close"]),
                            stop_loss=sl,
                            take_profit=tp,
                            confidence=int(mg_confidence),
                            auto_trade=True,
                            is_martingale=True
                        )
                        
                        signal["martingale_confirmed_sent"] = True
                        if _log_throttle(f"mg_confirm_{mg_time:%H:%M}", 180):
                            logger.info(f"Martingale confirmed: {mg_time:%H:%M} {direction}")
                    elif signal.get("martingale_prealert_sent") and not signal.get("martingale_skip_sent"):
                        reason = "Weakened martingale structure" if not should_mg else mg_live_reason
                        if _log_throttle(f"mg_skip_{mg_time:%H:%M}", 300):
                            logger.info(f"MG skipped: {mg_time:%H:%M} - {reason[:40]}")
                        
                        # Send skip message using new formatter
                        skip_msg = TelegramFormatter.format_skipped_signal(
                            pair=signal.get("pair", "EURUSD"),
                            direction=direction,
                            time=f"{mg_time:%H:%M}",
                            reason=reason,
                        )
                        messages.append(skip_msg)
                        signal["martingale_skip_sent"] = True

                    processed_signals.add(mg_key)
        except Exception as e:
            if _log_throttle("proc_error", 60):
                logger.error(f"Processing error: {e}")
            continue

    return messages
