from logger import logger
import os
import time
from datetime import time as clock_time

import pandas as pd
import requests

# ONLY detect Railway (not BOT_TOKEN)
if os.getenv("RAILWAY_ENVIRONMENT"):
    from config_prod import BOT_TOKEN, CHAT_ID, TD_API_KEY
else:
    from config_local import BOT_TOKEN, CHAT_ID, TD_API_KEY

from fixed_trade import get_fixed_signal
from forex_trade import get_forex_signal
from indicators import add_indicators, calculate_score
from signal_list import (
    apply_signal_text,
    get_adaptive_trade_threshold,
    process_signal_list,
    should_force_fast_mode,
    store_tracked_signal,
    update_signal_list,
)
from market_safety import run_market_safety
from cache_manager import cache

PAIR = "EUR/USD"
SLEEP_TIME = 120   # 2 minutes
IDLE_SLEEP_TIME = 900   # 15 minutes during weekends/off-market hours
MARKET_OPEN_TIME = clock_time(13, 0)
MARKET_CLOSE_TIME = clock_time(22, 0)  # London/NY Prime ONLY (Aligned with generator)
NEWS_BLOCK_MINUTES = 15
TRADE_COOLDOWN_MINUTES = 20
HIGH_IMPACT_NEWS_EVENTS = [
    # Add high-impact event times in Asia/Kolkata timezone.
    # Example: "2026-05-03 18:00"
]
MAX_SIGNAL_MESSAGES_PER_CYCLE = 10

LAST_SIGNAL_INPUT_UPDATE_ID = None

# API rate limiting
API_CALL_TIMES = []
API_CALLS_PER_MINUTE_LIMIT = 5
_rate_limit_pause_until = 0.0  # epoch seconds; set on 429 for 90-second cooldown


def track_api_call():
    """Track API call timestamp for rate limiting."""
    now = time.time()
    API_CALL_TIMES.append(now)
    # Remove calls older than 60 seconds
    API_CALL_TIMES[:] = [t for t in API_CALL_TIMES if now - t < 60]


def is_api_call_allowed():
    """Check if we can make another API call within rate limit."""
    now = time.time()
    if now < _rate_limit_pause_until:
        remaining = int(_rate_limit_pause_until - now)
        logger.info(f"Emergency cooldown active — skipping API call ({remaining}s remaining)")
        return False
    # Remove calls older than 60 seconds
    recent_calls = [t for t in API_CALL_TIMES if now - t < 60]
    return len(recent_calls) < API_CALLS_PER_MINUTE_LIMIT


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


# get_current_candle_key moved to cache_manager.py


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
def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        res = requests.post(url, data={
            "chat_id": CHAT_ID,
            "text": msg,
            "parse_mode": "Markdown"
        })
        logger.info(f"Telegram: {res.text}")

    except Exception as e:
        logger.error(f"Telegram error: {e}")


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
        logger.error(f"Telegram input error: {e}")
        return None


# ==============================
# DATA FETCH
# ==============================
def get_data(interval):
    if not is_api_call_allowed():
        logger.info(f"API rate limit reached. Skipping {interval} data fetch.")
        return None

    url = "https://api.twelvedata.com/time_series"

    params = {
        "symbol": PAIR,
        "interval": interval,
        "apikey": TD_API_KEY,
        "outputsize": 200
    }

    res = requests.get(url, params=params).json()
    track_api_call()

    if "values" not in res:
        logger.error(f"API ERROR: {res}")

        if res.get("code") == 429:
            global _rate_limit_pause_until
            _rate_limit_pause_until = time.time() + 90
            logger.warning("429 received — emergency cooldown: all fetches paused for 90 seconds.")
            return None

        logger.error(f"*API Error* {res}")
        return None

    df = pd.DataFrame(res["values"])

    if "datetime" in df.columns:
        df["CandleTime"] = pd.to_datetime(df["datetime"])

    price_columns = ["open", "high", "low", "close"]
    df[price_columns] = df[price_columns].astype(float)
    df = df.iloc[::-1]

    df.rename(columns={
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close"
    }, inplace=True)

    return df


def run_external_signal_engine(df, cached_minute_df=None):
    """Run external signal processing, optionally using cached minute data."""
    # Ensure we always pass a usable `df` into process_signal_list so external
    # signal processing runs even when the auto-bot skipped or df is small.
    # Prefer provided `df`, then cached_minute_df, then attempt a fresh 5min fetch.
    if df is None or (hasattr(df, "__len__") and len(df) < 200):
        fallback_df = None
        if cached_minute_df is not None and len(cached_minute_df) >= 200:
            fallback_df = cached_minute_df
        elif is_api_call_allowed():
            try:
                fallback_df = get_data("5min")
            except Exception:
                fallback_df = None

        if fallback_df is not None and len(fallback_df) >= 200:
            df = fallback_df

    if cached_minute_df is not None:
        minute_data_fetcher = lambda: cached_minute_df
    else:
        minute_data_fetcher = lambda: get_data("1min") if is_api_call_allowed() else None

    signal_messages = process_signal_list(df, minute_data_fetcher=minute_data_fetcher)
    for signal_message in signal_messages[:MAX_SIGNAL_MESSAGES_PER_CYCLE]:
        send_telegram(signal_message)


# ==============================
# MAIN LOOP
# ==============================
def run():
    logger.info("BOT RUNNING")

    last_signal_time = None
    last_trade_time = None

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
                logger.info("Market closed — idle mode")
                logger.info(f"Next market open: {get_next_market_open():%Y-%m-%d %H:%M %Z}")
                time.sleep(get_idle_sleep_seconds())
                continue

            logger.info("Market Open — Running")

            force_fast_mode = should_force_fast_mode()

            if force_fast_mode:
                interval = "1min"
                sleep_time = 20
            else:
                interval = "5min"
                sleep_time = SLEEP_TIME

            df = cache.get_processed_dataframe(interval, get_data, add_indicators)

            # 3) Fetch df is complete above.
            if df is None:
                time.sleep(sleep_time)
                continue

            # Fetch 1-minute data using cache manager
            cached_minute_df = cache.get_dataframe("1min", get_data)

            # 4) Run auto bot logic.
            # df is already processed with indicators
            confidence, grade = calculate_score(df)
            trade_threshold = get_adaptive_trade_threshold(75)

            # Debug: print final confidence used by bot decision path
            logger.debug(f"FINAL CONFIDENCE USED: {confidence}%")

            if confidence < trade_threshold:
                logger.info(f"Signal rejected - confidence too low: {confidence}% (threshold {trade_threshold}%)")
                # 5) Run external signal processing after auto bot.
                run_external_signal_engine(df, cached_minute_df)
                time.sleep(sleep_time)
                continue

            fixed = get_fixed_signal(df)
            
            if fixed:
                # NEW: Market Safety Check for Auto-Bot
                safety_ok, safety_msg, penalty = run_market_safety(df, fixed['signal'])
                
                # Apply penalty to confidence
                confidence -= penalty
                
                if not safety_ok or confidence < trade_threshold:
                    reason = safety_msg if not safety_ok else f"Confidence dropped below threshold after safety penalty ({confidence}% < {trade_threshold}%)"
                    logger.info(f"Auto-trade rejected: {reason}")
                    run_external_signal_engine(df, cached_minute_df)
                    time.sleep(sleep_time)
                    continue
                now = pd.Timestamp.now(tz="Asia/Kolkata")

                if last_trade_time is not None:
                    cooldown_minutes = (now - last_trade_time).total_seconds() / 60

                    if cooldown_minutes < TRADE_COOLDOWN_MINUTES:
                        remaining = TRADE_COOLDOWN_MINUTES - cooldown_minutes
                        logger.info(f"Trade cooldown active - {remaining:.1f} min remaining")
                        run_external_signal_engine(df, cached_minute_df)
                        time.sleep(sleep_time)
                        continue

                news_blocked, news_time = is_high_impact_news_window()

                if news_blocked:
                    logger.info(f"Trade blocked due to high-impact news at {news_time:%H:%M}")
                    run_external_signal_engine(df, cached_minute_df)
                    time.sleep(sleep_time)
                    continue

                current_candle_time = df.iloc[-1].get("CandleTime", df.index[-1])

                if current_candle_time == last_signal_time:
                    logger.debug("Duplicate signal skipped")
                    run_external_signal_engine(df, cached_minute_df)
                    time.sleep(sleep_time)
                    continue

                logger.info(f"Score: {confidence} {grade}")

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

                logger.info(msg)
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
                        source="auto",
                    )
                except Exception as e:
                    logger.error(f"Tracking error: {e}")

                last_trade_time = pd.Timestamp.now(tz="Asia/Kolkata")

            else:
                logger.debug(f"Candle {current_candle_time}: No automated signal found.")

            # 5) Process external signal list with the same df.
            run_external_signal_engine(df, cached_minute_df)

            # 6) Final update of last_signal_time to prevent re-processing same candle
            last_signal_time = current_candle_time
            logger.debug(f"Candle {current_candle_time} processing completed.")
            
            time.sleep(sleep_time)

        except Exception as e:
            logger.error(f"Error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    run()
