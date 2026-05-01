import requests
import time
import pandas as pd

USE_LOCAL = True

if USE_LOCAL:
    from config_local import BOT_TOKEN, CHAT_ID, TD_API_KEY
else:
    from config_prod import BOT_TOKEN, CHAT_ID, TD_API_KEY

from indicators import add_indicators
from fixed_trade import get_fixed_signal
from forex_trade import get_forex_signal

PAIR = "EUR/USD"
SLEEP_TIME = 600  # 10 min


if not TD_API_KEY:
    raise ValueError("TD_API_KEY is missing or empty")


# ==============================
# TELEGRAM
# ==============================
def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg})
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

    while True:
        try:
            df = get_data("1min")

            if df is None:
                time.sleep(SLEEP_TIME)
                continue

            df = add_indicators(df)

            fixed = get_fixed_signal(df)

            if fixed:
                forex = get_forex_signal(df, fixed["signal"])

                msg = f"""
━━━━━━━━━━━━━━
📊 EURUSD {forex['direction']}

⏳ FIXED TRADE
Entry: {fixed['entry']}
Expiry: {fixed['expiry']}

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

            time.sleep(SLEEP_TIME)

        except Exception as e:
            print("Error:", e)
            time.sleep(60)


if __name__ == "__main__":
    run()