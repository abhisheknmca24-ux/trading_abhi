import pandas as pd

def validate_live_signal(df: pd.DataFrame, direction: str) -> tuple[bool, str]:
    """
    Lightweight live confirmation engine.
    Purpose:
    - Sudden reversal detection
    - Momentum collapse
    - Dangerous spike detection
    
    This does NOT repeat full indicator validation. It focuses on immediate price action anomalies.
    """
    if df is None or len(df) < 3:
        return True, "Insufficient data for live confirmation"

    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # Calculate basic price action metrics
    candle_body = abs(last["Close"] - last["Open"])
    candle_range = last["High"] - last["Low"]
    prev_candle_body = abs(prev["Close"] - prev["Open"])
    
    # Use ATR if available for normalized thresholding, otherwise estimate
    if "ATR" in df.columns and not pd.isna(last["ATR"]):
        atr = last["ATR"]
    else:
        # Fallback approximation if ATR isn't calculated
        atr = (df["High"] - df["Low"]).tail(14).mean()
        
    # Prevent division/comparison issues if ATR is 0
    if pd.isna(atr) or atr <= 0.00001:
        atr = 0.0001 

    # ==========================================
    # 1. Sudden Reversal Detection
    # ==========================================
    if direction == "CALL":
        # Sudden bearish reversal: large bearish candle body right before entry
        if last["Close"] < last["Open"] and candle_body > (atr * 1.5):
            return False, "Sudden bearish reversal detected"
            
        # Upper wick rejection: price pushed up but slammed down by sellers
        upper_wick = last["High"] - max(last["Open"], last["Close"])
        if upper_wick > (candle_body * 2.0) and upper_wick > (atr * 0.8):
            return False, "Strong upper wick rejection (Bearish pressure)"
            
    elif direction == "PUT":
        # Sudden bullish reversal: large bullish candle body right before entry
        if last["Close"] > last["Open"] and candle_body > (atr * 1.5):
            return False, "Sudden bullish reversal detected"
            
        # Lower wick rejection: price pushed down but bought back up by buyers
        lower_wick = min(last["Open"], last["Close"]) - last["Low"]
        if lower_wick > (candle_body * 2.0) and lower_wick > (atr * 0.8):
            return False, "Strong lower wick rejection (Bullish pressure)"

    # ==========================================
    # 2. Momentum Collapse
    # ==========================================
    # If the current candle has almost no body after a strong previous move, 
    # it indicates exhaustion exactly when we need momentum.
    if candle_body < (atr * 0.15) and prev_candle_body > (atr * 1.0):
        return False, "Momentum collapse (exhaustion after strong move)"

    # ==========================================
    # 3. Dangerous Spike Detection
    # ==========================================
    # Huge anomalous candle range (potential un-tracked news event or manipulation)
    if candle_range > (atr * 3.5):
        return False, "Dangerous volatility spike detected"
        
    return True, "Live confirmation passed"
