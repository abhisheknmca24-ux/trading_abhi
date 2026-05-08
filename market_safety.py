import pandas as pd

SIDEWAYS_ATR_THRESHOLD = 0.6
DANGEROUS_VOL_ATR_RATIO = 2.5
WICK_BODY_RATIO_THRESHOLD = 2.0
EXTREME_SPIKE_BODY_RATIO = 3.0


def check_market_session(df: pd.DataFrame) -> tuple[bool, str]:
    if df is None or len(df) < 200:
        return False, "insufficient data"
    return True, "ok"


def check_sideways_market(df: pd.DataFrame) -> tuple[bool, str]:
    last = df.iloc[-1]
    if pd.isna(last.get("ATR")) or pd.isna(last.get("EMA50")) or pd.isna(last.get("EMA200")):
        return True, "ok"

    atr = float(last["ATR"])
    atr_mean = float(df["ATR"].tail(50).mean())

    recent_ema_gap = abs(float(last["EMA50"]) - float(last["EMA200"]))
    avg_ema_gap = abs(df["EMA50"] - df["EMA200"]).tail(50).mean()

    if atr < atr_mean * SIDEWAYS_ATR_THRESHOLD and recent_ema_gap < avg_ema_gap * 0.5:
        return False, "heavy sideways market detected"

    return True, "ok"


def check_dangerous_volatility(df: pd.DataFrame) -> tuple[bool, str]:
    last = df.iloc[-1]
    if pd.isna(last.get("ATR")):
        return True, "ok"

    atr = float(last["ATR"])
    atr_mean = float(df["ATR"].tail(50).mean())

    if atr_mean > 0 and atr > atr_mean * DANGEROUS_VOL_ATR_RATIO:
        return False, "dangerous volatility detected"

    return True, "ok"


def check_wick_rejection(df: pd.DataFrame) -> tuple[bool, str]:
    last = df.iloc[-1]
    if pd.isna(last.get("High")) or pd.isna(last.get("Low")) or pd.isna(last.get("Open")) or pd.isna(last.get("Close")):
        return True, "ok"

    high, low, open_, close = float(last["High"]), float(last["Low"]), float(last["Open"]), float(last["Close"])
    body = abs(close - open_)
    upper_wick = high - max(open_, close)
    lower_wick = min(open_, close) - low

    prev = df.iloc[-2]
    prev_body = abs(float(prev["Close"]) - float(prev["Open"]))
    avg_body = (df["Close"] - df["Open"]).abs().tail(20).mean()

    if body > avg_body * EXTREME_SPIKE_BODY_RATIO:
        return False, "extreme spike detected"

    if body > 0 and avg_body > 0:
        max_wick = max(upper_wick, lower_wick)
        if max_wick > body * WICK_BODY_RATIO_THRESHOLD and body < avg_body * 0.5:
            return False, "extreme reversal rejection detected"

    return True, "ok"


def run_market_safety(df: pd.DataFrame) -> tuple[bool, str]:
    checks = [
        ("session", check_market_session(df)),
        ("volatility", check_dangerous_volatility(df)),
        ("sideways", check_sideways_market(df)),
        ("wick", check_wick_rejection(df)),
    ]

    for name, (passed, reason) in checks:
        if not passed:
            return False, reason

    return True, "ok"
