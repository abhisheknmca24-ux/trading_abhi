import os
import time
import requests
import pandas as pd
import yfinance as yf

from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator
from ta.volatility import BollingerBands, AverageTrueRange

# ==============================
# ENV
# ==============================
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

PAIRS = {
    "EUR/USD": "EURUSD=X",
    "GBP/USD": "GBPUSD=X",
    "USD/JPY": "USDJPY=X"
}

SLEEP_TIME = 60
COOLDOWN = 300

# ==============================
# TELEGRAM
# ==============================
def send_telegram(message):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": message})
    except Exception as e:
        print("Telegram Error:", e)

# ==============================
# DATA FIX
# ==============================
def get_data(symbol, interval):
    df = yf.download(symbol, interval=interval, period="1d")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df = df.apply(lambda x: x.squeeze())
    df.dropna(inplace=True)

    return df

# ==============================
# INDICATORS
# ==============================
def add_indicators(df):
    df = df.copy()

    df['Close'] = pd.to_numeric(df['Close'], errors='coerce')
    df['High'] = pd.to_numeric(df['High'], errors='coerce')
    df['Low'] = pd.to_numeric(df['Low'], errors='coerce')

    df['EMA50'] = EMAIndicator(df['Close'], 50).ema_indicator()
    df['EMA200'] = EMAIndicator(df['Close'], 200).ema_indicator()
    df['RSI'] = RSIIndicator(df['Close'], 14).rsi()

    bb = BollingerBands(df['Close'], 20, 2)
    df['BB_H'] = bb.bollinger_hband()
    df['BB_L'] = bb.bollinger_lband()

    df['ATR'] = AverageTrueRange(
        df['High'], df['Low'], df['Close'], 14
    ).average_true_range()

    return df

# ==============================
# FILTERS
# ==============================
def is_active_market():
    hour = pd.Timestamp.utcnow().hour
    return 6 <= hour <= 18

def is_fake_breakout(df):
    last = df.iloc[-1]
    body = abs(last['Close'] - last['Open'])
    wick = last['High'] - max(last['Close'], last['Open'])
    return wick > body * 2

def strong_candle(df):
    last = df.iloc[-1]
    body = abs(last['Close'] - last['Open'])
    range_ = last['High'] - last['Low']
    return body > (range_ * 0.6)

# ==============================
# SIGNAL
# ==============================
def generate_signal(df_m1, df_m5):

    last = df_m1.iloc[-1]
    prev = df_m1.iloc[-2]
    m5_last = df_m5.iloc[-1]

    signal = None
    confidence = 0
    reasons = []

    uptrend = last['EMA50'] > last['EMA200'] and m5_last['EMA50'] > m5_last['EMA200']
    downtrend = last['EMA50'] < last['EMA200'] and m5_last['EMA50'] < m5_last['EMA200']

    if uptrend:
        if (
            last['Close'] > last['EMA50'] and
            last['RSI'] > prev['RSI'] and
            last['RSI'] < 70 and
            strong_candle(df_m1)
        ):
            signal = "BUY"
            confidence += 30
            reasons.append("MTF Uptrend")

            if last['Close'] > prev['High']:
                confidence += 20
                reasons.append("Breakout")

            if last['ATR'] > df_m1['ATR'].mean():
                confidence += 15
                reasons.append("Volatility")

            if not is_fake_breakout(df_m1):
                confidence += 20
                reasons.append("Valid Break")

            if is_active_market():
                confidence += 15
                reasons.append("Active Session")

    if downtrend:
        if (
            last['Close'] < last['EMA50'] and
            last['RSI'] < prev['RSI'] and
            last['RSI'] > 30 and
            strong_candle(df_m1)
        ):
            signal = "SELL"
            confidence += 30
            reasons.append("MTF Downtrend")

            if last['Close'] < prev['Low']:
                confidence += 20
                reasons.append("Breakdown")

            if last['ATR'] > df_m1['ATR'].mean():
                confidence += 15
                reasons.append("Volatility")

            if not is_fake_breakout(df_m1):
                confidence += 20
                reasons.append("Valid Break")

            if is_active_market():
                confidence += 15
                reasons.append("Active Session")

    grade = None
    if confidence >= 90:
        grade = "S-TIER"
    elif confidence >= 80:
        grade = "A+"
    elif confidence >= 70:
        grade = "A"

    return signal, confidence, grade, reasons

# ==============================
# FORMAT
# ==============================
def format_message(pair, signal, confidence, grade, reasons, price):
    return f"""
🚀 PRO SIGNAL (5 MIN TRADE)

📊 Pair: {pair}
🔥 Signal: {signal}
🏆 Grade: {grade}
📈 Confidence: {confidence}%

💰 Entry: {price:.5f}
⏳ Expiry: 5 Minutes

📌 Reasons:
{', '.join(reasons)}
"""

# ==============================
# MAIN
# ==============================
def run_bot():
    print("🔥 MULTI-PAIR PRO BOT RUNNING...")

    send_telegram("✅ Multi-Pair Bot Started 🚀")

    last_trade_time = 0

    while True:
        try:
            now = time.time()

            if now - last_trade_time < COOLDOWN:
                time.sleep(5)
                continue

            best_signal = None
            best_conf = 0

            for pair_name, symbol in PAIRS.items():

                df_m1 = add_indicators(get_data(symbol, "1m"))
                df_m5 = add_indicators(get_data(symbol, "5m"))

                signal, confidence, grade, reasons = generate_signal(df_m1, df_m5)

                if signal and grade in ["A+", "S-TIER"]:
                    if confidence > best_conf:
                        best_conf = confidence
                        best_signal = (pair_name, signal, confidence, grade, reasons, df_m1.iloc[-1]['Close'])

            if best_signal:
                msg = format_message(*best_signal)
                send_telegram(msg)

                print("✅ BEST SIGNAL SENT")
                last_trade_time = time.time()
            else:
                print("❌ No strong signals across pairs")

            time.sleep(SLEEP_TIME)

        except Exception as e:
            print("Error:", e)
            time.sleep(60)

# ==============================
# START
# ==============================
if __name__ == "__main__":
    run_bot()