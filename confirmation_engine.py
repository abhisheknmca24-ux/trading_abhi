import pandas as pd
from typing import Tuple

def validate_live_signal(minute_df: pd.DataFrame, direction: str, pre_confidence: float) -> Tuple[bool, str]:
    """
    Performs real-time confirmation exactly at the moment of execution using the cached 1-minute dataframe.
    """
    if minute_df is None or len(minute_df) < 5:
        return False, "Insufficient 1m data for live validation"

    last = minute_df.iloc[-1]
    prev = minute_df.iloc[-2]
    prev2 = minute_df.iloc[-3]

    required_values = [last.get("EMA50"), last.get("RSI"), prev.get("RSI"), last.get("ATR"), last.get("Close"), last.get("Open"), prev.get("Close")]
    
    if any(pd.isna(v) for v in required_values if v is not None) or any(v is None for v in required_values):
        return False, "Indicator values missing in 1m data"

    atr = float(last["ATR"])
    rsi_value = float(last["RSI"])
    rsi_prev = float(prev["RSI"])
    close_now = float(last["Close"])
    open_now = float(last["Open"])
    close_prev = float(prev["Close"])
    ema50 = float(last["EMA50"])
    ema50_prev = float(prev["EMA50"])

    # 1. LIVE RSI MOMENTUM RECHECK
    if direction == "CALL":
        if rsi_value <= rsi_prev and pre_confidence < 75:
            return False, f"Live RSI momentum weakened ({rsi_prev:.1f} -> {rsi_value:.1f})"
    else:
        if rsi_value >= rsi_prev and pre_confidence < 75:
            return False, f"Live RSI momentum weakened ({rsi_prev:.1f} -> {rsi_value:.1f})"

    # 2. CANDLE STRENGTH / MOMENTUM CHECK
    candle_size = abs(close_now - open_now)
    if candle_size < atr * 0.1 and pre_confidence < 75:
        return False, "Live candle strength too weak"

    if direction == "CALL":
        if close_now < close_prev and pre_confidence < 75:
            return False, "Live price action is bearish against CALL"
    else:
        if close_now > close_prev and pre_confidence < 75:
            return False, "Live price action is bullish against PUT"

    # 3. LIVE EMA SLOPE CHECK (Micro-trend)
    if direction == "CALL":
        if ema50 < ema50_prev and pre_confidence < 75:
            return False, "Live EMA50 slope turned negative"
    else:
        if ema50 > ema50_prev and pre_confidence < 75:
            return False, "Live EMA50 slope turned positive"

    return True, "Live validation passed"
