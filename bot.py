import os
import time
from datetime import time as clock_time

import pandas as pd
import requests
import market_cache
import signal_generator

# ONLY detect Railway (not BOT_TOKEN)
if os.getenv("RAILWAY_ENVIRONMENT"):
    from config_prod import BOT_TOKEN, CHAT_ID, TD_API_KEY
else:
    from config_local import BOT_TOKEN, CHAT_ID, TD_API_KEY

from fixed_trade import get_fixed_signal
from forex_trade import get_forex_signal
from indicators import add_indicators, calculate_score, ensure_indicators
from signal_list import (
    apply_signal_text,
    get_adaptive_trade_threshold,
    process_signal_list,
    should_force_fast_mode,
    store_tracked_signal,
    update_signal_list,
    _martingale_safety_manager,  # PHASE 5: Import safety manager
)

PAIR = "EUR/USD"
SLEEP_TIME = 120   # 2 minutes
IDLE_SLEEP_TIME = 900   # 15 minutes during weekends/off-market hours
MARKET_OPEN_TIME = clock_time(13, 30)
MARKET_CLOSE_TIME = clock_time(21, 30)
NEWS_BLOCK_MINUTES = 15
TRADE_COOLDOWN_MINUTES = 20
HIGH_IMPACT_NEWS_EVENTS = [
    # Add high-impact event times in Asia/Kolkata timezone.
    # Example: "2026-05-03 18:00"
]
MAX_SIGNAL_MESSAGES_PER_CYCLE = 10

LAST_SIGNAL_INPUT_UPDATE_ID = None
LAST_GENERATED_CANDIDATE_REPORT_DATE = None




if not TD_API_KEY:
    raise ValueError("TD_API_KEY is missing or empty")


def is_market_open():
    market_open, _ = get_market_status()
    return market_open


def get_market_status(now=None):
    if now is None:
        now = pd.Timestamp.now(tz="Asia/Kolkata")

    if now.weekday() >= 5:
        return False, "weekend"

    current_time = now.time()

    if current_time < MARKET_OPEN_TIME or current_time > MARKET_CLOSE_TIME:
        return False, "closed"

    return True, "open"


def get_next_market_open(now=None):
    if now is None:
        now = pd.Timestamp.now(tz="Asia/Kolkata")

    candidate = now.normalize() + pd.Timedelta(
        hours=MARKET_OPEN_TIME.hour,
        minutes=MARKET_OPEN_TIME.minute
    )

    if now.weekday() < 5 and now < candidate:
        return candidate

    candidate += pd.Timedelta(days=1)

    while candidate.weekday() >= 5:
        candidate += pd.Timedelta(days=1)

    return candidate


def get_idle_sleep_seconds(now=None):
    if now is None:
        now = pd.Timestamp.now(tz="Asia/Kolkata")

    seconds_until_open = (get_next_market_open(now) - now).total_seconds()
    return max(1, int(min(IDLE_SLEEP_TIME, seconds_until_open)))


def is_near_candle_close():
    now = pd.Timestamp.now(tz="Asia/Kolkata")
    return now.second >= 45


def get_current_candle_key(interval):
    now = pd.Timestamp.now(tz="Asia/Kolkata")

    if interval == "1min":
        return now.floor("min")

    if interval == "5min":
        return now.floor("5min")

    return now.floor("min")


def is_high_impact_news_window(now=None):
    if now is None:
        now = pd.Timestamp.now(tz="Asia/Kolkata")

    for event_time in HIGH_IMPACT_NEWS_EVENTS:
        event = pd.Timestamp(event_time)

        if event.tzinfo is None:
            event = event.tz_localize("Asia/Kolkata")
        else:
            event = event.tz_convert("Asia/Kolkata")

        minutes_from_event = abs((now - event).total_seconds()) / 60

        if minutes_from_event <= NEWS_BLOCK_MINUTES:
            return True, event

    return False, None


# ==============================
# TELEGRAM
# ==============================
import messaging


def send_telegram(msg):
    try:
        # route through messaging queue as DAILY_REPORT fallback
        messaging.send("DAILY_REPORT", msg)
    except Exception as e:
        print("Telegram error:", e)


def fetch_signal_text_from_telegram():
    global LAST_SIGNAL_INPUT_UPDATE_ID

    try:
        params = {"timeout": 5}
        if LAST_SIGNAL_INPUT_UPDATE_ID is not None:
            params["offset"] = LAST_SIGNAL_INPUT_UPDATE_ID + 1

        url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
        res = requests.get(url, params=params, timeout=20).json()

        if not res.get("ok"):
            return None

        updates = res.get("result", [])
        if not updates:
            return None

        latest_text = None
        max_update_id = LAST_SIGNAL_INPUT_UPDATE_ID

        for update in updates:
            update_id = update.get("update_id")
            if not isinstance(update_id, int):
                continue

            if max_update_id is None or update_id > max_update_id:
                max_update_id = update_id

            message = update.get("message") or update.get("edited_message") or {}
            chat_id = str(message.get("chat", {}).get("id", ""))

            if chat_id and str(CHAT_ID) != chat_id:
                continue

            text = (message.get("text") or "").strip()
            if text:
                latest_text = text

        LAST_SIGNAL_INPUT_UPDATE_ID = max_update_id
        return latest_text

    except Exception as e:
        print("Telegram input error:", e)
        return None


# ==============================
# DATA FETCH
# ==============================
# Data fetches are centralized in market_cache.py


def run_external_signal_engine(df, cached_minute_df=None):
    """Run external signal processing, optionally using cached minute data."""
    # Ensure we always pass a usable `df` into process_signal_list so external
    # signal processing runs even when the auto-bot skipped or df is small.
    # Prefer provided `df`, then cached_minute_df, then attempt a fresh 5min fetch.
    if df is None or (hasattr(df, "__len__") and len(df) < 200):
        fallback_df = None
        if cached_minute_df is not None and len(cached_minute_df) >= 200:
            fallback_df = cached_minute_df
        else:
            try:
                fallback_df = market_cache.get_5m_df(PAIR)
            except Exception:
                fallback_df = None

        if fallback_df is not None and len(fallback_df) >= 200:
            df = fallback_df

    if cached_minute_df is not None:
        minute_data_fetcher = lambda: cached_minute_df
    else:
        minute_data_fetcher = lambda: market_cache.get_1m_df(PAIR)

    signal_messages = process_signal_list(df, minute_data_fetcher=minute_data_fetcher)
    # signal_messages are tuples (type, text)
    sent = 0
    for item in signal_messages:
        if sent >= MAX_SIGNAL_MESSAGES_PER_CYCLE:
            break
        if isinstance(item, tuple) and len(item) == 2:
            typ, text = item
            try:
                messaging.send(typ, text)
                sent += 1
            except Exception as e:
                print(f"messaging error: {e}")
        elif isinstance(item, str):
            # legacy: send as DAILY_REPORT
            try:
                messaging.send("DAILY_REPORT", item)
                sent += 1
            except Exception as e:
                print(f"messaging error: {e}")


def maybe_publish_generated_candidates(df, cached_minute_df=None):
    """Publish generated signals as candidate-only daily reports."""
    global LAST_GENERATED_CANDIDATE_REPORT_DATE

    now = pd.Timestamp.now(tz="Asia/Kolkata")
    today = now.floor("D")
    if LAST_GENERATED_CANDIDATE_REPORT_DATE == today:
        return

    live_df = cached_minute_df if cached_minute_df is not None and len(cached_minute_df) >= 200 else df
    candidates = signal_generator.generate_if_needed(now=now, live_df=live_df)
    if not candidates:
        return

    report = signal_generator.format_candidate_report(candidates)
    messaging.send("DAILY_REPORT", report)
    LAST_GENERATED_CANDIDATE_REPORT_DATE = today


# ==============================
# MAIN LOOP
# ==============================
def run():
    print("BOT RUNNING")
    # Startup health message
    try:
        from learning_engine import learning_summary
        import signal_generator
        import market_cache as _mc

        cache_status = "OK" if getattr(_mc, "last_successful_5m", None) is not None else "No cached 5m"
        learning = learning_summary()
        ls = learning.get("total_trades") if isinstance(learning, dict) else None
        health_msg = (
            "*Bot Online*\n\n"
            f"Cache: {cache_status}\n"
            "Signal engine: active\n"
            "Learning engine: active\n"
            f"Learning trades: {ls}\n"
        )
        messaging.send("DAILY_REPORT", health_msg)
    except Exception:
        messaging.send("DAILY_REPORT", "*Bot Started* - health check unavailable")

    last_signal_time = None
    last_trade_time = None
    last_safety_report_time = None  # PHASE 5: Track martingale status reporting
    cached_interval = None
    cached_candle_key = None
    cached_df = None
    cached_minute_df = None
    cached_minute_time = None

    while True:
        try:
            # 1) Update external signal list first.
            message_text = fetch_signal_text_from_telegram()
            if message_text:
                apply_signal_text(message_text)
            else:
                update_signal_list()

            # 2) Check market status.
            market_open, _ = get_market_status()

            if not market_open:
                print("Market closed — idle mode")
                print(f"Next market open: {get_next_market_open():%Y-%m-%d %H:%M %Z}")
                time.sleep(get_idle_sleep_seconds())
                continue

            print("Market Open — Running")

            force_fast_mode = should_force_fast_mode()

            if force_fast_mode:
                interval = "1min"
                sleep_time = 20
            else:
                interval = "5min"
                sleep_time = SLEEP_TIME

            current_candle_key = get_current_candle_key(interval)

            if (
                cached_df is not None
                and cached_interval == interval
                and cached_candle_key == current_candle_key
            ):
                df = cached_df.copy()
                print(f"Using cached {interval} data")
            else:
                if interval == "5min":
                    df = market_cache.get_5m_df(PAIR)
                else:
                    df = market_cache.get_1m_df(PAIR)

                if df is not None:
                    cached_interval = interval
                    cached_candle_key = current_candle_key
                    cached_df = df.copy()

            # 3) Fetch df is complete above.
            if df is None:
                time.sleep(sleep_time)
                continue

            # Cache 1-minute data once per minute for all signal processing
            now_timestamp = pd.Timestamp.now(tz="Asia/Kolkata")
            now_minute_key = now_timestamp.floor("min")
            
            if cached_minute_df is None or cached_minute_time != now_minute_key:
                cm = market_cache.get_1m_df(PAIR)
                if cm is not None:
                    cached_minute_df = cm
                    cached_minute_time = now_minute_key

            # 4) Run auto bot logic.
            df = ensure_indicators(df)
            maybe_publish_generated_candidates(df, cached_minute_df)
            confidence, grade = calculate_score(df)
            # Use adaptive trade threshold with base 68 per updated settings
            trade_threshold = get_adaptive_trade_threshold(68)

            # Debug: print final confidence used by bot decision path
            print(f"FINAL CONFIDENCE USED: {confidence}%")

            if confidence < trade_threshold:
                print(f"Signal rejected - confidence too low: {confidence}% (threshold {trade_threshold}%)")
                # 5) Run external signal processing after auto bot.
                run_external_signal_engine(df, cached_minute_df)
                time.sleep(sleep_time)
                continue

            fixed = get_fixed_signal(df)

            if fixed:
                now = pd.Timestamp.now(tz="Asia/Kolkata")

                if last_trade_time is not None:
                    cooldown_minutes = (now - last_trade_time).total_seconds() / 60

                    if cooldown_minutes < TRADE_COOLDOWN_MINUTES:
                        remaining = TRADE_COOLDOWN_MINUTES - cooldown_minutes
                        print(f"Trade cooldown active - {remaining:.1f} min remaining")
                        run_external_signal_engine(df, cached_minute_df)
                        time.sleep(sleep_time)
                        continue

                news_blocked, news_time = is_high_impact_news_window()

                if news_blocked:
                    print(f"Trade blocked due to high-impact news at {news_time:%H:%M}")
                    run_external_signal_engine(df, cached_minute_df)
                    time.sleep(sleep_time)
                    continue

                current_candle_time = df.iloc[-1].get("CandleTime", df.index[-1])

                if current_candle_time == last_signal_time:
                    print("Duplicate signal skipped")
                    run_external_signal_engine(df, cached_minute_df)
                    time.sleep(sleep_time)
                    continue

                print("Score:", confidence, grade)

                send_telegram(f"""
*PRE-SIGNAL*

*Market:* EUR/USD
*Signal:* {fixed['signal']}

*Score*
Grade: {grade}
Confidence: {confidence}%

*Fixed Trade*
Entry: {fixed['entry']}
Starts In: {fixed['seconds_left']//60} min

Waiting for confirmation.
""")

                forex = get_forex_signal(df, fixed["signal"], confidence)

                confirm = fixed

                msg = f"""
*CONFIRMED SIGNAL*

*Market:* EUR/USD
*Direction:* {forex['direction']}

*Score*
Grade: {grade}
Confidence: {confidence}%

*Fixed Trade*
Entry: {confirm['entry']}
Expiry: {confirm['expiry']}

*Forex Trade*
Entry: {forex['entry']}
Entry Window: {forex['entry_window']}
Hold: {forex['hold']}

*Risk Targets*
TP: {forex['tp']}
SL: {forex['sl']}
Multiplier: {forex['multiplier']}
Auto Close: {forex['auto_close']}
"""

                print(msg)
                send_telegram(msg)
                try:
                    entry_price = float(df.iloc[-1]["Close"])
                    expiry_time = pd.Timestamp.now(tz="Asia/Kolkata") + pd.Timedelta(minutes=5)

                    store_tracked_signal(
                        signal_time=pd.Timestamp.now(tz="Asia/Kolkata"),
                        direction=forex["direction"],
                        entry_price=entry_price,
                        expiry_time=expiry_time,
                        signal_type="auto_trade",
                        pair="EURUSD",
                        confidence=confidence,
                        df=df,
                        source="Regular",  # PHASE 5: Mark as regular auto trade
                    )
                except Exception as e:
                    print("Tracking error:", e)

                last_signal_time = current_candle_time
                last_trade_time = pd.Timestamp.now(tz="Asia/Kolkata")

            else:
                print("No signal")

            # PHASE 5: Periodic martingale safety status report (every 30 minutes)
            now = pd.Timestamp.now(tz="Asia/Kolkata")
            if last_safety_report_time is None or (now - last_safety_report_time).total_seconds() > 1800:
                try:
                    safety_status = _martingale_safety_manager.get_status_report()
                    send_telegram(safety_status)
                    last_safety_report_time = now
                    print("Martingale safety status sent")
                except Exception as e:
                    print(f"Error sending safety status: {e}")

            # 5) Process external signal list with the same df.
            run_external_signal_engine(df, cached_minute_df)
            time.sleep(sleep_time)

        except Exception as e:
            print(f"Error in main loop: {e}")
            # Fail-safe: provide status and recover gracefully
            try:
                messaging.send("DAILY_REPORT", f"⚠️ *Bot Error*\n\nBot recovered from error: {str(e)[:50]}\n\nRestarting...")
            except Exception:
                pass
            time.sleep(60)


if __name__ == "__main__":
    run()
