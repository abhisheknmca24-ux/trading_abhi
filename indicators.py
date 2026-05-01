import pandas as pd
from ta.trend import EMAIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange

def add_indicators(df):
    df = df.copy()
    df["EMA50"] = EMAIndicator(df["Close"], 50).ema_indicator()
    df["EMA200"] = EMAIndicator(df["Close"], 200).ema_indicator()
    df["RSI"] = RSIIndicator(df["Close"], 14).rsi()
    df["ATR"] = AverageTrueRange(df["High"], df["Low"], df["Close"], 14).average_true_range()
    return df