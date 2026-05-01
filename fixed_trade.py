import pandas as pd

def get_fixed_signal(df):
    last = df.iloc[-1]

    signal = None

    if last["EMA50"] > last["EMA200"] and last["RSI"] > 55:
        signal = "CALL"
    elif last["EMA50"] < last["EMA200"] and last["RSI"] < 45:
        signal = "PUT"

    if not signal:
        return None

    now = pd.Timestamp.now()

    # ⏱ Round to next minute
    next_minute = now.ceil("min")

    # 🎯 Entry = 2 minutes ahead (perfect buffer)
    entry = next_minute + pd.Timedelta(minutes=2)

    # ⏳ Expiry = 5 minutes
    expiry = entry + pd.Timedelta(minutes=5)

    # 🚫 Prevent late signals
    if (entry - now).total_seconds() < 90:
        return None

    return {
        "signal": signal,
        "entry": entry.strftime("%H:%M"),
        "expiry": expiry.strftime("%H:%M"),
        "seconds_left": int((entry - now).total_seconds())
    }