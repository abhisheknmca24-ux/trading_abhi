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
    entry = (now + pd.Timedelta(minutes=1)).floor("min")
    expiry = entry + pd.Timedelta(minutes=5)

    return {
        "signal": signal,
        "entry": entry.strftime("%H:%M"),
        "expiry": expiry.strftime("%H:%M")
    }