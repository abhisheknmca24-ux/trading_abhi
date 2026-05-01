import os
import time
import json
import requests
import pandas as pd

from dotenv import load_dotenv
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange

# ==============================
# LOAD ENV
# ==============================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
TD_API_KEY = os.getenv("TD_API_KEY")

if not TD_API_KEY:
    raise ValueError("❌ TD_API_KEY missing in .env")

# ==============================
# CONFIG
# ==============================
PAIRS = ["EUR/USD", "GBP/USD", "USD/JPY"]
SLEEP_TIME = 60
COOLDOWN = 300

STATS_FILE = "trade_stats.json"

# ==============================
# TELEGRAM (SAFE)
# ==============================
def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        r = requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=10)
        if r.status_code != 200:
            print("Telegram failed:", r.text)
    except Exception as e:
        print("Telegram error:", e)

# ==============================
# TIME
# ==============================
def get_ist():
    return pd.Timestamp.utcnow() + pd.Timedelta(hours=5, minutes=30)

# ==============================
# DATA (DUAL FORMAT FIX)
# ==============================
def get_data(pair, interval):
    formats = [pair, pair.replace("/", "")]

    for symbol in formats:
        try:
            time.sleep(1)

            url = "https://api.twelvedata.com/time_series"
            params = {
                "symbol": symbol,
                "interval": interval,
                "outputsize": 100,
                "apikey": TD_API_KEY
            }

            res = requests.get(url, params=params).json()

            if "values" not in res:
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

    print(f"❌ Data failed: {pair}")
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

def save_stats(stats):
    json.dump(stats, open(STATS_FILE, "w"))

def update_stats(result):
    stats = load_stats()
    if result == "WIN":
        stats["wins"] += 1
    else:
        stats["losses"] += 1
    save_stats(stats)
    return stats

def accuracy():
    s = load_stats()
    total = s["wins"] + s["losses"]
    return 0 if total == 0 else round((s["wins"] / total) * 100, 2)

# ==============================
# FILTER
# ==============================
def adaptive_filter(df):
    last = df.iloc[-1]
    score = 0

    if 40 < last["RSI"] < 65:
        score += 30

    trend = abs(last["EMA50"] - last["EMA200"])
    if trend > 0.0005:
        score += 30

    if last["ATR"] > df["ATR"].mean():
        score += 40

    return score >= 70

# ==============================
# SIGNAL
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
        reasons.append("MTF Uptrend")

        if last["RSI"] > prev["RSI"]:
            confidence += 20
            reasons.append("RSI Momentum")

        if last["ATR"] > df1["ATR"].mean():
            confidence += 20
            reasons.append("High Volatility")

    elif down:
        signal = "SELL"
        confidence += 30
        reasons.append("MTF Downtrend")

        if last["RSI"] < prev["RSI"]:
            confidence += 20
            reasons.append("RSI Momentum")

        if last["ATR"] > df1["ATR"].mean():
            confidence += 20
            reasons.append("High Volatility")

    grade = None
    if confidence >= 85:
        grade = "S-TIER"
    elif confidence >= 75:
        grade = "A+"
    elif confidence >= 65:
        grade = "A"

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
    print("🚀 PRO BOT RUNNING")
    send_telegram("🚀 Trading Bot Started")

    last_trade = 0

    while True:
        try:
            if time.time() - last_trade < COOLDOWN:
                time.sleep(5)
                continue

            best = None
            best_conf = 0

            for pair in PAIRS:
                df1_raw = get_data(pair, "1min")
                df5_raw = get_data(pair, "5min")

                if df1_raw is None or df5_raw is None:
                    continue

                df1 = add_indicators(df1_raw)
                df5 = add_indicators(df5_raw)

                signal, confidence, grade, reasons = generate(df1, df5)

                if signal and grade in ["A+", "S-TIER"] and adaptive_filter(df1):
                    if confidence > best_conf:
                        best = (pair, signal, confidence, grade, reasons)
                        best_conf = confidence

            if best:
                pair, signal, confidence, grade, reasons = best

                now = get_ist()
                entry_time = (now + pd.Timedelta(minutes=2)).replace(second=0)
                expiry_time = entry_time + pd.Timedelta(minutes=5)

                direction = "CALL" if signal == "BUY" else "PUT"
                pair_clean = pair.replace("/", "")

                send_telegram(f"""
⏳ PRE SIGNAL

📊 Pair: {pair_clean}
📈 Direction: {direction}

🕒 Entry Time: {entry_time.strftime('%H:%M')}
⏳ Expiry Time: {expiry_time.strftime('%H:%M')}

🏆 Grade: {grade}
📊 Confidence: {confidence}%
""")

                while get_ist() < entry_time:
                    time.sleep(1)

                entry_df = get_data(pair, "1min")
                if entry_df is None:
                    continue

                entry_price = entry_df.iloc[-1]["Close"]

                send_telegram(f"""
🚀 ENTER NOW

📊 Pair: {pair_clean}
📈 Direction: {direction}

🕒 Entry: {entry_time.strftime('%H:%M')}
⏳ Expiry: {expiry_time.strftime('%H:%M')}

⏱ Duration: 5 min

🏆 Grade: {grade}
📊 Confidence: {confidence}%

📌 Reasons:
{', '.join(reasons)}
""")

                time.sleep(300)

                result = check_result(pair, signal, entry_price)

                if result:
                    stats = update_stats(result)

                    send_telegram(f"""
📊 TRADE RESULT

Result: {result}

Wins: {stats['wins']}
Losses: {stats['losses']}

🎯 Accuracy: {accuracy()}%
""")

                last_trade = time.time()

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