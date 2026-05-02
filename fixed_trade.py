import pandas as pd


def get_fixed_signal(df):
    last = df.iloc[-1]

    ema50 = last["EMA50"]
    ema200 = last["EMA200"]
    atr = last["ATR"]

    if pd.isna(ema50) or pd.isna(ema200):
        return None

    if pd.isna(atr):
        atr = 0

    trend_strength = abs(ema50 - ema200)
    trend_threshold = max(0.0002, atr * 0.15)

    if trend_strength <= trend_threshold:
        return None

    signal = None

    if ema50 > ema200 and last["RSI"] > 55:
        signal = "CALL"
    elif ema50 < ema200 and last["RSI"] < 45:
        signal = "PUT"

    if not signal:
        return None

    now = pd.Timestamp.now()

    # Round to next minute.
    next_minute = now.ceil("min")

    # Entry = 2 minutes ahead.
    entry = next_minute + pd.Timedelta(minutes=2)

    # Expiry = 5 minutes.
    expiry = entry + pd.Timedelta(minutes=5)

    # Prevent late signals.
    if (entry - now).total_seconds() < 90:
        return None

    return {
        "signal": signal,
        "entry": entry.strftime("%H:%M"),
        "expiry": expiry.strftime("%H:%M"),
        "seconds_left": int((entry - now).total_seconds())
    }
