import pandas as pd

def calculate_real_confidence(df: pd.DataFrame, direction: str, boost: int, trade_stats: dict) -> int:
    """
    Calculate realistic confidence probability based on multiple factors.
    Prevents fake 95-100% confidence by capping at 90%.
    """
    if df is None or len(df) < 2:
        return 50

    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    atr = float(last["ATR"]) if not pd.isna(last.get("ATR")) else 0.0
    atr_mean = float(df["ATR"].mean()) if not pd.isna(df["ATR"].mean()) else 0.0
    candle_size = abs(float(last["Close"]) - float(last["Open"]))
    avg_candle = float((df["Close"] - df["Open"]).abs().tail(10).mean()) if len(df) >= 10 else candle_size
    distance = abs(float(last["Close"]) - float(last["EMA50"]))
    ema_gap = abs(float(last["EMA50"]) - float(last["EMA200"]))
    rsi = float(last["RSI"]) if not pd.isna(last.get("RSI")) else 50.0
    rsi_prev = float(prev["RSI"]) if not pd.isna(prev.get("RSI")) else 50.0

    # Start with a baseline realistic probability of 50%
    confidence = 50.0

    # 1. EMA Slope / Trend (+10 to +15 max)
    if direction == "CALL":
        if last["EMA50"] > last["EMA200"]:
            confidence += 10
            if ema_gap > atr * 0.5:
                confidence += 5 # Strong trend
    else:
        if last["EMA50"] < last["EMA200"]:
            confidence += 10
            if ema_gap > atr * 0.5:
                confidence += 5 # Strong trend

    # 2. RSI Strength (+5 to +10 max)
    if direction == "CALL":
        if rsi > 55 and rsi > rsi_prev:
            confidence += 5
            if rsi > 60:
                confidence += 5
    else:
        if rsi < 45 and rsi < rsi_prev:
            confidence += 5
            if rsi < 40:
                confidence += 5

    # 3. ATR Strength / Volatility (+0 to +10)
    if atr > atr_mean:
        confidence += 5
        if atr > atr_mean * 1.2:
            confidence += 5

    # 4. Candle Strength / Momentum (+5)
    if candle_size > avg_candle:
        confidence += 5
        
    # 5. Distance to EMA (Value area) (+5)
    if distance <= max(0.0003, atr * 0.50):
        confidence += 5

    # 6. Source Quality (boost)
    confidence += min(boost, 15)

    # 7. Recent Bot Performance / Historical Win Rate (-10 to +10)
    recent_win_rate = trade_stats.get("recent_10_win_rate", 50.0)
    total_trades = trade_stats.get("total_trades", 0)
    if total_trades > 0:
        if recent_win_rate >= 70:
            confidence += 5
        elif recent_win_rate >= 60:
            confidence += 2
        elif recent_win_rate < 40:
            confidence -= 10 # recent losses high -> reduce confidence
        elif recent_win_rate < 50:
            confidence -= 5

    # --- Downgrades ---
    
    # Flat EMA (weak trend)
    if ema_gap < atr * 0.15:
        confidence -= 10
        
    # Weak RSI
    if direction == "CALL" and rsi < 50:
        confidence -= 10
    elif direction == "PUT" and rsi > 50:
        confidence -= 10
        
    # Weak ATR
    if atr < atr_mean * 0.8:
        confidence -= 10
        
    # Weak momentum
    if candle_size < avg_candle * 0.5:
        confidence -= 10

    # Cap confidence: maximum 90%. Prevent fake 95-100% confidence.
    confidence = max(10, min(confidence, 90))

    return int(confidence)
