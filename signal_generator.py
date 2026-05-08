"""
Signal Generator — Data-Driven Daily Signal Engine
Generates 3-5 strong candidate signals per day using multi-factor validation:
  - Historical win rate (14-day rolling)
  - EMA trend alignment (EMA50 vs EMA200)
  - EMA slope direction (rising/falling consistently)
  - RSI momentum
  - ATR above rolling average
  - Market safety score
  - Session quality
  - Sideways market avoidance
"""
import os
import json
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, List, Dict

from market_cache import get_5m_data
from signal_list import update_signal_list
from market_safety import check_market_safety, get_current_session

if os.getenv("RAILWAY_ENVIRONMENT"):
    from config_prod import BOT_TOKEN, CHAT_ID
else:
    from config_local import BOT_TOKEN, CHAT_ID

DAILY_SIGNALS_STATE_FILE = "daily_signals_state.json"
MAX_DAILY_SIGNALS = 5
MIN_DAILY_SIGNALS = 3

_last_generation_date: Optional[datetime] = None
_generated_signals_cache: List[Dict] = []


def _get_now() -> datetime:
    """Get current time in Asia/Kolkata timezone."""
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Kolkata"))
    except ImportError:
        return datetime.now()


def _load_signals_state() -> Dict:
    try:
        if os.path.exists(DAILY_SIGNALS_STATE_FILE):
            with open(DAILY_SIGNALS_STATE_FILE, 'r') as f:
                return json.load(f)
    except Exception as e:
        print(f"Error loading daily signals state: {e}")
    return {}


def _save_signals_state(signals: List[Dict], generation_date) -> None:
    try:
        state = {
            "generation_date": generation_date.isoformat(),
            "signals": signals,
            "saved_at": _get_now().isoformat()
        }
        with open(DAILY_SIGNALS_STATE_FILE, 'w') as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"Error saving daily signals state: {e}")


def get_cached_daily_signals() -> Optional[List[Dict]]:
    global _generated_signals_cache, _last_generation_date
    today = _get_now().date()
    if _last_generation_date == today and _generated_signals_cache:
        return _generated_signals_cache
    state = _load_signals_state()
    if state.get("generation_date") == today.isoformat():
        signals = state.get("signals", [])
        if signals:
            _generated_signals_cache = signals
            _last_generation_date = today
            return signals
    return None


def reset_daily_signals_cache() -> None:
    global _generated_signals_cache, _last_generation_date
    _generated_signals_cache = []
    _last_generation_date = None
    print("  ✓ Daily signals cache reset")


# ─────────────────────────────────────────────
# Multi-factor signal scoring
# ─────────────────────────────────────────────

def _add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Add EMA50, EMA200, RSI, ATR to df in-place copy."""
    df = df.copy()
    df["EMA50"] = df["Close"].ewm(span=50, adjust=False).mean()
    df["EMA200"] = df["Close"].ewm(span=200, adjust=False).mean()

    # RSI (14)
    delta = df["Close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(span=14, adjust=False).mean()
    avg_loss = loss.ewm(span=14, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["RSI"] = 100 - (100 / (1 + rs))

    # ATR (14)
    high_low = df["High"] - df["Low"]
    high_close = (df["High"] - df["Close"].shift()).abs()
    low_close = (df["Low"] - df["Close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["ATR"] = tr.ewm(span=14, adjust=False).mean()

    return df


def _compute_signal_quality_score(
    direction: str,
    group: pd.DataFrame,
    full_df: pd.DataFrame,
    overall_atr_mean: float,
    now: datetime
) -> float:
    """
    Score a candidate time-slot signal (0-100).
    Combines win rate, EMA alignment, EMA slope, RSI momentum, ATR strength,
    and global market safety filters.
    Returns the composite score. Returns -1 if hard-rejected.
    """
    score = 0.0

    # ── 0. Global Market Safety (Hard Gate) ──
    # This uses market_safety.py which handles sideways, spikes, etc.
    safety_ok, safety_reason = check_market_safety(full_df, direction, now)
    if not safety_ok:
        return -1.0

    # ── 1. Historical win rate (0-35 pts) ──
    count = len(group)
    if count < 5:
        return -1.0

    if direction == "CALL":
        win_rate = group["Is_Bullish"].sum() / count
    else:
        win_rate = group["Is_Bearish"].sum() / count

    if win_rate < 0.62: # Slightly tightened from 0.60
        return -1.0

    score += win_rate * 35  # max 35 pts

    # ── 2. Volatility vs overall (0-10 pts) ──
    avg_size = group["CandleSize"].mean()
    if avg_size < overall_atr_mean * 0.7:
        return -1.0
    vol_ratio = min(avg_size / max(overall_atr_mean, 1e-9), 2.0)
    score += min(vol_ratio * 5, 10) # max 10 pts

    # ── 3. EMA trend alignment (0-15 pts) ──
    last = full_df.iloc[-1]
    prev = full_df.iloc[-2] if len(full_df) >= 2 else last
    prev2 = full_df.iloc[-3] if len(full_df) >= 3 else prev

    ema50 = float(last["EMA50"]) if not pd.isna(last.get("EMA50")) else None
    ema200 = float(last["EMA200"]) if not pd.isna(last.get("EMA200")) else None

    if ema50 is None or ema200 is None:
        return -1.0

    if direction == "CALL" and ema50 > ema200:
        score += 15
    elif direction == "PUT" and ema50 < ema200:
        score += 15
    else:
        return -1.0  # EMA trend misaligned — hard reject

    # ── 4. EMA slope direction (consistent last 3 candles) (0-10 pts) ──
    ema50_p1 = float(prev["EMA50"]) if not pd.isna(prev.get("EMA50")) else ema50
    ema50_p2 = float(prev2["EMA50"]) if not pd.isna(prev2.get("EMA50")) else ema50_p1

    if direction == "CALL":
        # Consistent rising slope
        slope_ok = (ema50 > ema50_p1) and (ema50_p1 >= ema50_p2)
    else:
        # Consistent falling slope
        slope_ok = (ema50 < ema50_p1) and (ema50_p1 <= ema50_p2)

    if slope_ok:
        score += 10
    else:
        score += 3 # Partial credit for general alignment but not perfect slope

    # ── 5. RSI momentum (0-10 pts) ──
    rsi = float(last["RSI"]) if not pd.isna(last.get("RSI")) else 50.0
    rsi_prev = float(prev["RSI"]) if not pd.isna(prev.get("RSI")) else rsi

    if direction == "CALL":
        if rsi > 80 or rsi < 50: # Enforce strong but not overbought zone
            return -1.0
        if rsi > rsi_prev:
            score += 10
    else:
        if rsi < 20 or rsi > 50:
            return -1.0
        if rsi < rsi_prev:
            score += 10

    # ── 6. Session Quality (0-10 pts) ──
    session = get_current_session(now)
    if session == "london_ny_overlap":
        score += 10
    elif session in ["london", "new_york"]:
        score += 6
    elif session == "dead":
        return -1.0
    else:
        score -= 5 # Sydney/Tokyo penalty

    # ── 7. ATR strength (0-10 pts) ──
    atr = float(last["ATR"]) if not pd.isna(last.get("ATR")) else 0.0
    if atr < overall_atr_mean * 0.75:
        return -1.0 # Volatility too low for reliable binary signals

    if atr >= overall_atr_mean:
        score += 10
    else:
        score += 5

    return min(score, 100.0)


def _session_quality_score(time_slot: str) -> float:
    """
    Return a multiplier 0.0-1.0 based on session quality.
    London/NY overlap (18:00-22:30 IST) = 1.0
    London (13:30-18:00 IST) = 0.85
    Others = 0.5
    """
    try:
        h, m = int(time_slot[:2]), int(time_slot[3:])
        t = h * 60 + m
        london_ny_start = 18 * 60
        london_ny_end = 22 * 60 + 30
        london_start = 13 * 60 + 30

        if london_ny_start <= t <= london_ny_end:
            return 1.0
        elif london_start <= t < london_ny_start:
            return 0.85
        else:
            return 0.5
    except Exception:
        return 0.5


def generate_daily_signals(force: bool = False) -> bool:
    """
    Generate 3-5 strongest daily candidate signals using multi-factor analysis.
    Returns True if signals were generated.
    """
    global _last_generation_date, _generated_signals_cache

    today = _get_now().date()

    if not force:
        cached = get_cached_daily_signals()
        if cached is not None:
            print("Daily signals already generated for today (cached).")
            return False

    print("Generating daily signals (multi-factor engine)...")
    _last_generation_date = today
    _generated_signals_cache = []

    # Fetch raw data
    raw_df = get_5m_data()
    if raw_df is None or raw_df.empty:
        print("No data available for signal generation.")
        return False

    # Add indicators to full dataset
    full_df = _add_indicators(raw_df)

    # Need at least 200 candles for reliable indicators
    if len(full_df) < 200:
        print(f"Insufficient data: {len(full_df)} candles (need 200+).")
        return False

    df = full_df.copy()
    df["CandleTime"] = pd.to_datetime(df["CandleTime"])

    # Rolling 14-day window for historical stats
    cutoff_date = df["CandleTime"].max() - timedelta(days=14)
    recent_df = df[df["CandleTime"] >= cutoff_date].copy()

    recent_df["Is_Bullish"] = recent_df["Close"] > recent_df["Open"]
    recent_df["Is_Bearish"] = recent_df["Close"] < recent_df["Open"]
    recent_df["CandleSize"] = (recent_df["Close"] - recent_df["Open"]).abs()
    recent_df["TimeSlot"] = recent_df["CandleTime"].dt.strftime("%H:%M")

    overall_atr_mean = float(recent_df["CandleSize"].mean())

    candidates = []

    for time_slot, group in recent_df.groupby("TimeSlot"):
        # Only active market hours (13:30 - 21:30 IST)
        if time_slot < "13:30" or time_slot > "21:30":
            continue

        # Session quality multiplier
        session_q = _session_quality_score(time_slot)
        if session_q < 0.7:
            continue  # Skip low-quality sessions

        for direction in ["CALL", "PUT"]:
            quality_score = _compute_signal_quality_score(
                direction=direction,
                group=group,
                full_df=full_df,
                overall_atr_mean=overall_atr_mean,
                now=_get_now()
            )

            if quality_score < 0:
                continue  # Hard rejected

            # Apply session quality multiplier
            final_score = quality_score * session_q

            count = len(group)
            if direction == "CALL":
                win_rate = group["Is_Bullish"].sum() / count
            else:
                win_rate = group["Is_Bearish"].sum() / count

            candidates.append({
                "time_slot": time_slot,
                "direction": direction,
                "quality_score": round(final_score, 2),
                "win_rate": round(win_rate, 4),
                "count": count,
                "wins": int(group["Is_Bullish"].sum() if direction == "CALL" else group["Is_Bearish"].sum()),
                "volatility": round(float(group["CandleSize"].mean()), 6),
                "session_quality": round(session_q, 2),
            })

    if not candidates:
        print("No high-quality candidate signals found for today.")
        return False

    # Sort by quality score (descending)
    candidates.sort(key=lambda x: x["quality_score"], reverse=True)

    # Take top 3-5 signals
    top_signals = candidates[:MAX_DAILY_SIGNALS]

    # Ensure we don't force too many if quality drops significantly
    if len(top_signals) > MIN_DAILY_SIGNALS:
        max_score = top_signals[0]["quality_score"]
        # Drop signals with score < 60% of the best
        top_signals = [s for s in top_signals if s["quality_score"] >= max_score * 0.6]
        top_signals = top_signals[:MAX_DAILY_SIGNALS]

    _generated_signals_cache = top_signals
    _save_signals_state(top_signals, today)

    # Build signal lines for signal_list
    signal_lines = []
    for sig in top_signals:
        line = f"{sig['time_slot']} EURUSD {sig['direction']}"
        signal_lines.append(line)

    update_signal_list(signal_lines)

    # Build Telegram message
    msg = "🚀 *Signal Engine — Daily Candidates*\n\n"
    msg += f"📅 {today.strftime('%d %b %Y')} | Signals: {len(top_signals)}\n"
    msg += "─────────────────────────\n\n"

    for sig in top_signals:
        wr_pct = int(sig['win_rate'] * 100)
        emoji = "🟢" if sig['direction'] == "CALL" else "🔴"
        session_tag = "🔥 LDN/NY" if sig['session_quality'] >= 1.0 else "📍 LDN"
        msg += f"{emoji} {sig['time_slot']} EURUSD *{sig['direction']}*\n"
        msg += f"   WinRate: {wr_pct}% | Score: {sig['quality_score']:.0f} | {session_tag}\n\n"

    msg += "⚠️ _These are CANDIDATE signals only._\n"
    msg += "_Live confirmation still required before entering._"

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": msg,
        "parse_mode": "Markdown"
    }

    try:
        requests.post(url, json=payload, timeout=10)
        print(f"Generated {len(top_signals)} daily signal candidates → Telegram.")
    except Exception as e:
        print(f"Error sending generated signals to Telegram: {e}")

    return True


if __name__ == "__main__":
    generate_daily_signals()
