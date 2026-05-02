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

PAIR = "EUR/USD"
SLEEP_TIME = 120   # 2 minutes
NEWS_BLOCK_MINUTES = 15
TRADE_COOLDOWN_MINUTES = 20
HIGH_IMPACT_NEWS_EVENTS = [
    # Add high-impact event times in Asia/Kolkata timezone.
    # Example: "2026-05-03 18:00"
]


if not TD_API_KEY:
    raise ValueError("TD_API_KEY is missing or empty")


def is_market_open():
    now = pd.Timestamp.now(tz="Asia/Kolkata")

    if now.weekday() >= 5:
        return False

    current_time = now.time()

    if current_time < clock_time(13, 30) or current_time > clock_time(21, 30):
        return False

    return True


def get_market_status():
    now = pd.Timestamp.now(tz="Asia/Kolkata")

    if now.weekday() >= 5:
        return False, "weekend"

    current_time = now.time()

    if current_time < clock_time(13, 30) or current_time > clock_time(21, 30):
        return False, "closed"

    return True, "open"


def is_near_candle_close():
    now = pd.Timestamp.now(tz="Asia/Kolkata")
    return now.second >= 45


def get_current_candle_key(interval):
    now = pd.Timestamp.now(tz="Asia/Kolkata")

    if interval == "1min":
        return now.floor("min")

    if interval == "2min":
        return now.floor("2min")

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
def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        res = requests.post(url, data={
            "chat_id": CHAT_ID,
            "text": msg,
            "parse_mode": "Markdown"
        })
        print("Telegram:", res.text)

    except Exception as e:
        print("Telegram error:", e)


# ==============================
# DATA FETCH
# ==============================
def get_data(interval):
    url = "https://api.twelvedata.com/time_series"

    params = {
        "symbol": PAIR,
        "interval": interval,
        "apikey": TD_API_KEY,
        "outputsize": 200
    }

    res = requests.get(url, params=params).json()

    if "values" not in res:
        print("API ERROR:", res)

        if res.get("code") == 429:
            send_telegram("*API LIMIT HIT*\n\nBot paused for 1 hour.")
            time.sleep(3600)
            return None

        send_telegram(f"*API Error*\n\n{res}")
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


# ==============================
# MAIN LOOP
# ==============================
def run():
    print("BOT RUNNING")
    send_telegram("*Bot Started*\n\nSmart Mode Active.")

    mode = "normal"
    last_signal_time = None
    last_trade_time = None
    cached_interval = None
    cached_candle_key = None
    cached_df = None

    while True:
        try:
            market_open, market_status = get_market_status()

            if not market_open:
                if market_status == "weekend":
                    print("Weekend — Bot Paused")
                else:
                    print("Market Closed — Sleeping")
                time.sleep(600)
                continue

            print("Market Open — Running")

            if mode == "fast":
                interval = "1min"
                sleep_time = 10
            else:
                interval = "2min"
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
                df = get_data(interval)

                if df is not None:
                    cached_interval = interval
                    cached_candle_key = current_candle_key
                    cached_df = df.copy()

            if df is None:
                time.sleep(sleep_time)
                continue

            df = add_indicators(df)
            confidence, grade = calculate_score(df)

            if confidence >= 80:
                mode = "fast"

            if confidence < 85:
                print(f"Signal rejected - confidence too low: {confidence}%")
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
                        time.sleep(sleep_time)
                        continue

                news_blocked, news_time = is_high_impact_news_window()

                if news_blocked:
                    print(f"Trade blocked due to high-impact news at {news_time:%H:%M}")
                    time.sleep(sleep_time)
                    continue

                current_candle_time = df.iloc[-1].get("CandleTime", df.index[-1])

                if current_candle_time == last_signal_time:
                    print("Duplicate signal skipped")
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
                last_signal_time = current_candle_time
                last_trade_time = pd.Timestamp.now(tz="Asia/Kolkata")
                mode = "normal"

            else:
                print("No signal")

            time.sleep(sleep_time)

        except Exception as e:
            print("Error:", e)
            time.sleep(60)


if __name__ == "__main__":
    run()
