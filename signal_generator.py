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
        # Absolute fallback – market data unavailable; default to None
        logger.warning("Insufficient data for live direction decision.")
        return None, 0

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
    if call_score > put_score:
        direction = "CALL"
        confidence = int((call_score / total_possible) * 100)
    elif put_score > call_score:
        direction = "PUT"
        confidence = int((put_score / total_possible) * 100)
    else:
        # Tie or 0-0 failure
        direction = None
        confidence = 0

    confidence = max(0, min(confidence, 99))
    if direction:
        logger.info(f"Live direction: {direction} | CALL={call_score} PUT={put_score} | Confidence={confidence}%")
    else:
        logger.warning(f"No clear direction found: CALL={call_score} PUT={put_score}")
    
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

        ist_time_str = f"{ist_minutes // 60:02d}:{ist_minutes % 60:02d}"

        if conf_call >= 70:
            call_candidates.append({
                "time": ist_time_str,
                "pair": PAIR,
                "direction": "CALL",
                "confidence": int(conf_call)
            })

        if conf_put >= 70:
            put_candidates.append({
                "time": ist_time_str,
                "pair": PAIR,
                "direction": "PUT",
                "confidence": int(conf_put)
            })

    call_candidates.sort(key=lambda x: x["confidence"], reverse=True)
    put_candidates.sort(key=lambda x: x["confidence"], reverse=True)

    combined = call_candidates + put_candidates
    combined.sort(key=lambda x: x["confidence"], reverse=True)

    total_target = 12
    max_dominance = 0.70
    # Minimum confidence required for opposite-direction balancing signals
    BALANCE_MIN_CONFIDENCE = 65

    final_signals = []
    c_count = 0
    p_count = 0

    available_c = len(call_candidates)
    available_p = len(put_candidates)

    # ── Pass 1: standard selection at ≥70 confidence ─────────────────────────
    for s in combined:
        if len(final_signals) >= total_target:
            break

        if s["direction"] == "CALL":
            max_possible_p = min(available_p, total_target - (c_count + 1))
            # Allow at least 2 signals to avoid empty outputs on low-volatility days
            max_allowed = max(2, (c_count + 1 + max_possible_p) * max_dominance)
            if (c_count + 1) > max_allowed:
                continue
            final_signals.append(s)
            c_count += 1
        else:
            max_possible_c = min(available_c, total_target - (p_count + 1))
            max_allowed = max(2, (p_count + 1 + max_possible_c) * max_dominance)
            if (p_count + 1) > max_allowed:
                continue
            final_signals.append(s)
            p_count += 1

    # ── Pass 2: rebalancing – soften dominance using ≥65-confidence opposites ─
    # Only triggers when one direction exceeds the 70% cap after Pass 1.
    total_selected = len(final_signals)
    if total_selected >= 2:
        dominant = "CALL" if c_count > p_count else "PUT"
        minority = "PUT" if dominant == "CALL" else "CALL"
        dominant_count = c_count if dominant == "CALL" else p_count
        minority_count = p_count if dominant == "CALL" else c_count

        dominance_ratio = dominant_count / total_selected if total_selected > 0 else 0

        if dominance_ratio > max_dominance:
            # Collect minority candidates that meet the lower threshold but were
            # excluded from Pass 1 (conf < 70 or simply not yet included).
            selected_times = {s["time"] for s in final_signals}
            minority_pool = [
                s for s in (put_candidates if minority == "PUT" else call_candidates)
                if s["confidence"] >= BALANCE_MIN_CONFIDENCE
                and s["time"] not in selected_times
            ]
            minority_pool.sort(key=lambda x: x["confidence"], reverse=True)

            # How many swaps do we need to bring dominance to ≤70%?
            # dominance_ratio_after = (dominant_count - swaps) / total_selected
            # We want (dominant_count - swaps) / total_selected ≤ max_dominance
            max_dominant_allowed = int(total_selected * max_dominance)
            swaps_needed = dominant_count - max_dominant_allowed

            for extra in minority_pool:
                if swaps_needed <= 0:
                    break
                # Remove the weakest dominant-direction signal to make room
                dominant_signals = [
                    s for s in final_signals if s["direction"] == dominant
                ]
                if not dominant_signals:
                    break
                weakest = min(dominant_signals, key=lambda x: x["confidence"])
                if weakest["confidence"] > extra["confidence"]:
                    # The weakest dominant signal is still stronger; stop swapping
                    logger.info(
                        f"[Balance] Stopping swap: weakest {dominant} conf={weakest['confidence']}% "
                        f"> opposite {extra['confidence']}%"
                    )
                    break
                final_signals.remove(weakest)
                final_signals.append(extra)
                if dominant == "CALL":
                    c_count -= 1
                    p_count += 1
                else:
                    p_count -= 1
                    c_count += 1
                swaps_needed -= 1
                logger.info(
                    f"[Balance] Swapped out {dominant} conf={weakest['confidence']}% "
                    f"for {minority} conf={extra['confidence']}% at {extra['time']}"
                )

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
    
    if direction is None:
        logger.warning("Skipping forced signal generation due to missing live direction (API/data failure).")
        return []

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

    # 5. Filter by session (13:00 - 22:00 IST)
    all_signals = existing + forced_signals
    filtered = []
    for s in all_signals:
        t_str = s.get("time")
        if not t_str: continue
        try:
            h, m = map(int, t_str.split(":"))
            total_m = h * 60 + m
            if 13 * 60 <= total_m <= 22 * 60:
                filtered.append(s)
        except Exception:
            continue

    merged = filtered
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
