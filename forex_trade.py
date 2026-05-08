"""
Forex Signal Builder
Generates entry, TP, SL and safe leverage recommendations.
Leverage is CAPPED at 30x maximum for risk safety.
"""


def get_forex_signal(df, signal, confidence):
    last = df.iloc[-1]
    atr = last["ATR"]
    entry = last["Close"]

    if confidence >= 85:
        tp_factor = 2.0
        sl_factor = 0.8
        hold = "30-60 min"
        multiplier = "30x"   # was 150x — capped at 30x
        auto_close = "7%"
    elif confidence >= 75:
        tp_factor = 1.5
        sl_factor = 1.0
        hold = "15-30 min"
        multiplier = "20x"   # was 100x — capped at 20x
        auto_close = "5%"
    else:
        tp_factor = 1.0
        sl_factor = 1.2
        hold = "10-15 min"
        multiplier = "10x"   # was 50x — capped at 10x
        auto_close = "3%"

    if signal == "CALL":
        tp = entry + (tp_factor * atr)
        sl = entry - (sl_factor * atr)
        direction = "BUY"
    else:
        tp = entry - (tp_factor * atr)
        sl = entry + (sl_factor * atr)
        direction = "SELL"

    return {
        "direction": direction,
        "entry": round(entry, 5),
        "tp": round(tp, 5),
        "sl": round(sl, 5),
        "hold": hold,
        "multiplier": multiplier,
        "auto_close": auto_close,
        "entry_window": "1-2 min"
    }
