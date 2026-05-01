import pandas as pd
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange

def add_indicators(df):
    df = df.copy()
    df["EMA50"] = EMAIndicator(df["Close"], 50).ema_indicator()
    df["EMA200"] = EMAIndicator(df["Close"], 200).ema_indicator()
    df["RSI"] = RSIIndicator(df["Close"], 14).rsi()
    df["ATR"] = AverageTrueRange(df["High"], df["Low"], df["Close"], 14).average_true_range()
    return df


def calculate_score(df):
    if df is None or len(df) < 2:
        return 0, "B"

    last = df.iloc[-1]

    score = 0

    # Trend strength
    if last["EMA50"] > last["EMA200"]:
        score += 25
    elif last["EMA50"] < last["EMA200"]:
        score += 25

    # RSI strength
    if last["RSI"] > 60 or last["RSI"] < 40:
        score += 25

    # Momentum
    if abs(last["Close"] - df.iloc[-2]["Close"]) > 0:
        score += 25

    # Volatility (ATR)
    if last["ATR"] > df["ATR"].mean():
        score += 25

    confidence = score

    # 🎯 GRADING
    if confidence >= 85:
        grade = "S-TIER 🔥"
    elif confidence >= 75:
        grade = "A+ ✅"
    elif confidence >= 65:
        grade = "A"
    else:
        grade = "B"

    return confidence, grade