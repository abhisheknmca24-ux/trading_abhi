import pandas as pd
from ta.momentum import RSIIndicator
from ta.trend import EMAIndicator
from ta.volatility import AverageTrueRange

REQUIRED_INDICATORS = ("EMA50", "EMA200", "RSI", "ATR", "TrendStrength")


def add_indicators(df):
    if df is None or len(df) == 0:
        return df

    if all(column in df.columns for column in REQUIRED_INDICATORS):
        return df

    df = df.copy()

    if "EMA50" not in df.columns:
        df["EMA50"] = EMAIndicator(df["Close"], 50).ema_indicator()
    if "EMA200" not in df.columns:
        df["EMA200"] = EMAIndicator(df["Close"], 200).ema_indicator()
    if "RSI" not in df.columns:
        df["RSI"] = RSIIndicator(df["Close"], 14).rsi()
    if "ATR" not in df.columns:
        df["ATR"] = AverageTrueRange(df["High"], df["Low"], df["Close"], 14).average_true_range()

    if "TrendStrength" not in df.columns:
        df["TrendStrength"] = (df["EMA50"] - df["EMA200"]).abs()

    return df


def calculate_score(df):
    last = df.iloc[-1]
    prev = df.iloc[-2]

    score = 0

    atr = last["ATR"]
    atr_mean = df["ATR"].mean()
    trend_strength = last["TrendStrength"]

    if pd.isna(atr):
        atr = 0

    # Trend (EMA): wider EMA separation means a stronger trend.
    weak_trend_threshold = max(0.0001, atr * 0.10)
    strong_trend_threshold = max(0.0002, atr * 0.25)

    if not pd.isna(last["EMA50"]) and not pd.isna(last["EMA200"]) and last["EMA50"] != last["EMA200"]:
        if trend_strength >= strong_trend_threshold:
            score += 35
        elif trend_strength >= weak_trend_threshold:
            score += 20
        else:
            score += 10

    # RSI strength
    if last["RSI"] > 60 or last["RSI"] < 40:
        score += 20

    # Momentum: require a meaningful move, not just any price change.
    momentum_threshold = max(0.0002, atr * 0.10)
    if abs(last["Close"] - prev["Close"]) > momentum_threshold:
        score += 20

    # Volatility (ATR)
    if not pd.isna(atr_mean) and atr > atr_mean:
        score += 25

    confidence = min(score, 100)

    # 🎯 Grade
    if confidence >= 85:
        grade = "S-TIER 🔥"
    elif confidence >= 75:
        grade = "A+ ✅"
    elif confidence >= 65:
        grade = "A"
    else:
        grade = "B"

    return confidence, grade
