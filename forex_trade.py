def get_forex_signal(df, signal):
    last = df.iloc[-1]
    atr = last["ATR"]

    entry = last["Close"]

    if signal == "CALL":
        tp = entry + (1.5 * atr)
        sl = entry - (1.0 * atr)
        direction = "BUY"
    else:
        tp = entry - (1.5 * atr)
        sl = entry + (1.0 * atr)
        direction = "SELL"

    confidence = 80

    # Hold time
    if confidence >= 85:
        hold = "30–60 min"
        multiplier = "150x"
    elif confidence >= 75:
        hold = "15–30 min"
        multiplier = "100x"
    else:
        hold = "10–15 min"
        multiplier = "50x"

    return {
        "direction": direction,
        "entry": round(entry, 5),
        "tp": round(tp, 5),
        "sl": round(sl, 5),
        "hold": hold,
        "multiplier": multiplier,
        "auto_close": "5%",
        "entry_window": "1–2 min"
    }