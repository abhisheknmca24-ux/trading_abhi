import pandas as pd
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange

def add_indicators(df):
    # Shallow copy protects the cached original from accidental in-place mutation.
    # deep=False avoids duplicating column data arrays — only column references are copied.
    df = df.copy(deep=False)
    df["EMA50"] = EMAIndicator(df["Close"], 50).ema_indicator()
    df["EMA200"] = EMAIndicator(df["Close"], 200).ema_indicator()
    df["RSI"] = RSIIndicator(df["Close"], 14).rsi()
    df["ATR"] = AverageTrueRange(df["High"], df["Low"], df["Close"], 14).average_true_range()
    df["TrendStrength"] = (df["EMA50"] - df["EMA200"]).abs()
    return df


def calculate_score(df):
    if len(df) < 3:
        return 0, "B"

    last = df.iloc[-1]
    prev = df.iloc[-2]

    score = 0

    atr = last["ATR"]
    atr_mean = df["ATR"].mean()
    if pd.isna(atr): atr = 0
    if pd.isna(atr_mean): atr_mean = atr

    trend_strength = last["TrendStrength"]

    # Capping flags
    ema_trend_strong = False
    rsi_strong = False
    atr_strong = False
    candle_strong = False

    # 1. EMA Trend (Max 25)
    strong_trend_threshold = max(0.0002, atr * 0.25)
    weak_trend_threshold = max(0.0001, atr * 0.10)

    if not pd.isna(last["EMA50"]) and not pd.isna(last["EMA200"]):
        if trend_strength >= strong_trend_threshold:
            score += 25
            ema_trend_strong = True
        elif trend_strength >= weak_trend_threshold:
            score += 15
        else:
            score += 5

    # 2. EMA Position (Max 10)
    if not pd.isna(last["EMA50"]) and not pd.isna(last["EMA200"]):
        if last["Close"] > last["EMA50"] > last["EMA200"]:
            score += 10
        elif last["Close"] < last["EMA50"] < last["EMA200"]:
            score += 10
        elif last["Close"] > last["EMA50"]:
            score += 5
        elif last["Close"] < last["EMA50"]:
            score += 5

    # 3. RSI Momentum (Max 25)
    rsi_now = last["RSI"]
    rsi_prev = prev["RSI"]

    if (rsi_now > 60 and rsi_now > rsi_prev) or (rsi_now < 40 and rsi_now < rsi_prev):
        score += 25
        rsi_strong = True
    elif (rsi_now > 55 and rsi_now > rsi_prev) or (rsi_now < 45 and rsi_now < rsi_prev):
        score += 15
    elif (rsi_now > 50 and rsi_now > rsi_prev) or (rsi_now < 50 and rsi_now < rsi_prev):
        score += 5

    # 4. ATR Strength (Max 20)
    atr_prev = prev["ATR"]
    if atr > atr_mean and atr > atr_prev:
        score += 20
        atr_strong = True
    elif atr > atr_mean * 0.8:
        score += 10

    # 5. Candle Momentum (Max 20)
    body_now = abs(last["Close"] - last["Open"])
    avg_body = (df["Close"] - df["Open"]).abs().mean()
    momentum_threshold = max(0.0002, atr * 0.10)

    now_dir = 1 if last["Close"] > last["Open"] else -1
    prev_dir = 1 if prev["Close"] > prev["Open"] else -1

    if body_now > avg_body * 1.5 and now_dir == prev_dir and body_now > momentum_threshold:
        score += 20
        candle_strong = True
    elif body_now > avg_body and body_now > momentum_threshold:
        score += 10
    elif body_now > momentum_threshold:
        score += 5

    # --- Penalties ---
    # Small candle body
    if body_now < avg_body * 0.5:
        score -= 10
    # Weak ATR
    if atr < atr_mean * 0.6:
        score -= 10
    # Flat RSI
    if abs(rsi_now - rsi_prev) < 1.0:
        score -= 10
    # EMA compression
    if trend_strength < atr * 0.05:
        score -= 10
    # Sideways decay
    if atr < atr_mean * 0.8 and abs(last["Close"] - df["Close"].tail(10).mean()) < atr * 0.5:
        score -= 15

    # Bound base score
    score = max(0, min(score, 100))

    # Prevent confidence > 80 unless specific strong conditions met
    if score > 80:
        if not (atr_strong and rsi_strong and candle_strong and ema_trend_strong):
            score = 80

    confidence = int(score)

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
