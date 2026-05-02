import requests
import time
import pandas as pd
import os

# ONLY detect Railway (not BOT_TOKEN)
if os.getenv("RAILWAY_ENVIRONMENT"):
    from config_prod import BOT_TOKEN, CHAT_ID, TD_API_KEY
else:
    from config_local import BOT_TOKEN, CHAT_ID, TD_API_KEY

from indicators import add_indicators, calculate_score
from fixed_trade import get_fixed_signal
from forex_trade import get_forex_signal

PAIR = "EUR/USD"
SLEEP_TIME = 120   # 2 minutes


if not TD_API_KEY:
    raise ValueError("TD_API_KEY is missing or empty")


def is_market_open():
    now = pd.Timestamp.now(tz="Asia/Kolkata")

    start = now.replace(hour=13, minute=30, second=0)
    end = now.replace(hour=21, minute=30, second=0)

    if now.weekday() >= 5:
        return False

    return start <= now <= end


def is_near_candle_close():
    now = pd.Timestamp.now(tz="Asia/Kolkata")
    return now.second >= 40


# ==============================
# TELEGRAM
# ==============================
def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        res = requests.post(url, data={
            "chat_id": CHAT_ID,
            "text": msg
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
            send_telegram("🚨 API LIMIT HIT — Bot paused for 1 hour")
            time.sleep(3600)
            return None

        send_telegram(f"⚠️ API Error: {res}")
        return None

    df = pd.DataFrame(res["values"])
    df = df.astype(float)
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
    print("🚀 BOT RUNNING")
    send_telegram("🚀 Bot Started — Smart Mode Active")

    while True:
        try:
            if not is_market_open():
                print("⏸ Market closed")
                time.sleep(600)
                continue

            if is_near_candle_close():
                sleep_time = 10   # fast mode
            else:
                sleep_time = 120  # normal mode

            if not is_near_candle_close():
                time.sleep(20)
                continue

            df = get_data("1min")

            if df is None:
                time.sleep(sleep_time)
                continue

            df = add_indicators(df)
            confidence, grade = calculate_score(df)

            fixed = get_fixed_signal(df)

            if fixed:
                print("Score:", confidence, grade)

                send_telegram(f"""
⚠️ PRE-SIGNAL

📊 EURUSD {fixed['signal']}

🏆 Grade: {grade}
📊 Confidence: {confidence}%

⏳ Entry: {fixed['entry']} (in {fixed['seconds_left']//60} min)

⚠️ Wait for confirmation...
""")

                forex = get_forex_signal(df, fixed["signal"])

                confirm = fixed

                msg = f"""
━━━━━━━━━━━━━━
✅ CONFIRMED SIGNAL

📊 EURUSD {forex['direction']}

🏆 Grade: {grade}
📊 Confidence: {confidence}%

⏳ FIXED TRADE
Entry: {confirm['entry']}
Expiry: {confirm['expiry']}

📈 FOREX TRADE
Entry: {forex['entry']}
Window: {forex['entry_window']}
Hold: {forex['hold']}

🎯 TP: {forex['tp']}
🛑 SL: {forex['sl']}

⚙️ Multiplier: {forex['multiplier']}
🛑 Auto Close: {forex['auto_close']}
━━━━━━━━━━━━━━
"""

                print(msg)
                send_telegram(msg)

            else:
                print("❌ No signal")

            time.sleep(sleep_time)

        except Exception as e:
            print("Error:", e)
            time.sleep(60)


if __name__ == "__main__":
    run()