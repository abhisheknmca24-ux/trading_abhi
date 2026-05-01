import os
import time
import json
import requests
import pandas as pd

from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange

# ==============================
# CONFIG SWITCH
# ==============================
USE_LOCAL = os.getenv("USE_LOCAL", "false").lower() == "true"

if USE_LOCAL:
    from config_local import BOT_TOKEN, CHAT_ID, TD_API_KEY
else:
    from config_prod import BOT_TOKEN, CHAT_ID, TD_API_KEY

# ==============================
# CONFIG
# ==============================
PAIR = "EUR/USD"
SLEEP_TIME = 600  # 10 minutes
API_LIMIT_SLEEP = 3600  # 1 hour
STATS_FILE = "trade_stats.json"
api_blocked_until = 0

# ==============================
# TELEGRAM
# ==============================
def send_telegram(msg):
    if not BOT_TOKEN or not CHAT_ID:
        print("Telegram config missing")
        return

    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=10)
    except Exception as e:
        print("Telegram error:", e)

# ==============================
# TIME
# ==============================
def get_ist():
    return pd.Timestamp.now(tz="UTC") + pd.Timedelta(hours=5, minutes=30)

def is_trading_time():
    now = get_ist()
    return 12 <= now.hour <= 21  # 12:00–21:59 IST

# ==============================
# DATA (SAFE)
# ==============================
def get_data(pair, interval):
    global api_blocked_until

    if time.time() < api_blocked_until:
        wait_minutes = round((api_blocked_until - time.time()) / 60)
        print(f"API limit cooldown active. Trying again in {wait_minutes} minutes.")
        return None

    symbols = [pair, pair.replace("/", "")]

    for sym in symbols:
        try:
            time.sleep(1.5)

            url = "https://api.twelvedata.com/time_series"
            params = {
                "symbol": sym,
                "interval": interval,
                "outputsize": 100,
                "apikey": TD_API_KEY
            }

            res = requests.get(url, params=params).json()

            if "values" not in res:
                print("API ERROR:", res)
                if res.get("code") == 429:
                    api_blocked_until = time.time() + API_LIMIT_SLEEP
                    print("Daily API credits exhausted. Pausing API requests for 1 hour.")
                    return None
                continue

            df = pd.DataFrame(res["values"]).iloc[::-1]

            df["Open"] = pd.to_numeric(df["open"])
            df["High"] = pd.to_numeric(df["high"])
            df["Low"] = pd.to_numeric(df["low"])
            df["Close"] = pd.to_numeric(df["close"])

            df = df[["Open", "High", "Low", "Close"]]
            df.dropna(inplace=True)

            if len(df) < 30:
                continue

            return df

        except:
            continue

    print("❌ Data fetch failed")
    return None

# ==============================
# INDICATORS
# ==============================
def add_indicators(df):
    df = df.copy()
    df["EMA50"] = EMAIndicator(df["Close"], 50).ema_indicator()
    df["EMA200"] = EMAIndicator(df["Close"], 200).ema_indicator()
    df["RSI"] = RSIIndicator(df["Close"], 14).rsi()
    df["ATR"] = AverageTrueRange(df["High"], df["Low"], df["Close"], 14).average_true_range()
    return df

# ==============================
# STATS
# ==============================
def load_stats():
    if not os.path.exists(STATS_FILE):
        return {"wins": 0, "losses": 0}
    return json.load(open(STATS_FILE))

def save_stats(s):
    json.dump(s, open(STATS_FILE, "w"))

def update_stats(result):
    s = load_stats()
    if result == "WIN":
        s["wins"] += 1
    else:
        s["losses"] += 1
    save_stats(s)
    return s

def accuracy():
    s = load_stats()
    total = s["wins"] + s["losses"]
    return 0 if total == 0 else round((s["wins"]/total)*100, 2)

# ==============================
# SIGNAL LOGIC
# ==============================
def generate(df1, df5):
    last = df1.iloc[-1]
    prev = df1.iloc[-2]
    m5 = df5.iloc[-1]

    confidence = 0
    reasons = []
    signal = None

    up = last["EMA50"] > last["EMA200"] and m5["EMA50"] > m5["EMA200"]
    down = last["EMA50"] < last["EMA200"] and m5["EMA50"] < m5["EMA200"]

    if up:
        signal = "BUY"
        confidence += 30
        reasons.append("Uptrend")

        if last["RSI"] > prev["RSI"]:
            confidence += 20
            reasons.append("Momentum")

        if last["ATR"] > df1["ATR"].mean():
            confidence += 20
            reasons.append("Volatility")

    elif down:
        signal = "SELL"
        confidence += 30
        reasons.append("Downtrend")

        if last["RSI"] < prev["RSI"]:
            confidence += 20
            reasons.append("Momentum")

        if last["ATR"] > df1["ATR"].mean():
            confidence += 20
            reasons.append("Volatility")

    grade = None
    if confidence >= 85:
        grade = "S-TIER"
    elif confidence >= 75:
        grade = "A+"

    return signal, confidence, grade, reasons

# ==============================
# RESULT CHECK
# ==============================
def check_result(pair, signal, entry_price):
    df = get_data(pair, "1min")
    if df is None:
        return None

    close = df.iloc[-1]["Close"]

    if signal == "BUY":
        return "WIN" if close > entry_price else "LOSS"
    else:
        return "WIN" if close < entry_price else "LOSS"

# ==============================
# MAIN BOT
# ==============================
def run():
    print("🚀 FINAL BOT RUNNING")
    send_telegram("🚀 Bot Started (10min Strategy Active)")

    while True:
        try:
            if time.time() < api_blocked_until:
                print("API cooldown active")
                time.sleep(300)
                continue

            if not is_trading_time():
                print("⏸ Market closed")
                time.sleep(600)
                continue

            df1_raw = get_data(PAIR, "1min")
            if df1_raw is None:
                time.sleep(SLEEP_TIME)
                continue

            df5_raw = get_data(PAIR, "5min")
            if df5_raw is None:
                time.sleep(SLEEP_TIME)
                continue

            df1 = add_indicators(df1_raw)
            df5 = add_indicators(df5_raw)

            signal, confidence, grade, reasons = generate(df1, df5)

            if signal and (grade in ["A+", "S-TIER"] or confidence >= 70):
                now = get_ist()

                entry_time = now + pd.Timedelta(minutes=1)
                entry_time = entry_time.floor("min")
                expiry_time = entry_time + pd.Timedelta(minutes=5)

                direction = "CALL" if signal == "BUY" else "PUT"
                pair_clean = "EURUSD"

                send_telegram(f"""
⏳ PRE SIGNAL

{pair_clean} {direction}

Entry: {entry_time.strftime('%H:%M')}
Expiry: {expiry_time.strftime('%H:%M')}

Grade: {grade}
Confidence: {confidence}%
""")

                while get_ist() < entry_time:
                    time.sleep(1)

                entry_df = get_data(PAIR, "1min")
                if entry_df is None:
                    continue

                entry_price = entry_df.iloc[-1]["Close"]

                send_telegram(f"""
🚀 ENTER NOW

{pair_clean} {direction}

Entry: {entry_time.strftime('%H:%M')}
Expiry: {expiry_time.strftime('%H:%M')}

Duration: 5 min

Reasons: {', '.join(reasons)}
""")

                time.sleep(300)

                result = check_result(PAIR, signal, entry_price)

                if result:
                    stats = update_stats(result)

                    send_telegram(f"""
📊 RESULT

{result}

Wins: {stats['wins']}
Losses: {stats['losses']}
Accuracy: {accuracy()}%
""")

            else:
                print("❌ No signal")

            time.sleep(SLEEP_TIME)

        except Exception as e:
            print("Error:", e)
            time.sleep(60)

# ==============================
# START
# ==============================
if __name__ == "__main__":
    run()
