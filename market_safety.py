from datetime import datetime, timezone
import os
import time
import requests
import pandas as pd
from logger import logger

_NEWS_CACHE = {
    "data": [],
    "last_fetched": 0
}


# London Session: 08:00 - 17:00 UTC
# NY Session: 13:00 - 22:00 UTC
# Tokyo Session: 00:00 - 09:00 UTC

def check_market_session():
    """
    Check market session strength.
    Aligns with 13:00 - 22:00 IST (07:30 - 16:30 UTC).
    Returns (is_rejected, message, penalty)
    """
    now_utc = datetime.now(timezone.utc)
    minutes_utc = now_utc.hour * 60 + now_utc.minute
    
    # PRIME SESSION: 07:30 - 16:30 UTC (13:00 - 22:00 IST)
    if 450 <= minutes_utc <= 990:
        return False, "Prime session active", 0
        
    # WEAK SESSION: Convert weakness into small confidence penalty instead of hard reject
    return False, "Outside prime session (Weak liquidity)", 5

def check_high_impact_news():
    """
    Check if we are within 15 mins of a high-impact news event.
    Target keywords: CPI, NFP, FOMC, Interest Rate Decision, ECB, Powell
    """
    global _NEWS_CACHE
    now = time.time()
    
    # Fetch at most once per hour
    if now - _NEWS_CACHE["last_fetched"] > 3600:
        try:
            res = requests.get("https://nfs.faireconomy.media/ff_calendar_thisweek.json", timeout=10)
            if res.status_code == 200:
                _NEWS_CACHE["data"] = res.json()
                _NEWS_CACHE["last_fetched"] = now
            else:
                logger.warning(f"Failed to fetch news calendar, status: {res.status_code}")
        except Exception as e:
            logger.error(f"News API error: {e}")
            # If it fails, keep using old cache. Wait at least 5 mins before retrying.
            _NEWS_CACHE["last_fetched"] = now - 3300 
            
    events = _NEWS_CACHE["data"]
    if not events:
        return False, "No news data", 0
        
    keywords = ["cpi", "nfp", "fomc", "interest rate", "rate decision", "ecb", "powell"]
    
    current_dt = datetime.now(timezone.utc)
    
    for ev in events:
        country = ev.get("country", "")
        impact = ev.get("impact", "")
        title = ev.get("title", "").lower()
        
        if country in ["USD", "EUR"] and impact == "High":
            # Check keywords
            if any(k in title for k in keywords):
                try:
                    ev_time = pd.Timestamp(ev["date"])
                    if ev_time.tzinfo is None:
                        ev_time = ev_time.tz_localize("UTC")
                    else:
                        ev_time = ev_time.tz_convert("UTC")
                    
                    diff_minutes = (current_dt - ev_time).total_seconds() / 60.0
                    
                    # 15 mins before to 15 mins after
                    if -15 <= diff_minutes <= 15:
                        logger.warning(f"High impact news detected — trading paused ({ev['title']})")
                        return True, f"High impact news: {ev['title']}", 100
                except Exception as e:
                    logger.error(f"Error parsing news date {ev.get('date')}: {e}")
                    
    return False, "No high impact news", 0

def check_sideways_market(df):
    """Detect if the market is moving too sideways (lack of trend)."""
    if df is None or len(df) < 50:
        return True, "Insufficient data", 0
        
    last = df.iloc[-1]
    atr_mean = df["ATR"].tail(50).mean()
    atr_now = last["ATR"]
    
    # Extreme sideways: ATR is less than 40% of recent average
    if atr_now < (atr_mean * 0.4):
        return True, f"Dead sideways market (ATR {atr_now:.5f})", 20
    
    # Mild sideways: ATR less than 60%
    if atr_now < (atr_mean * 0.6):
        return False, "Low volatility sideways", 10
        
    return False, "Market active", 0

def check_dangerous_volatility(df):
    """Detect extreme volatility spikes (often caused by news events)."""
    if df is None or len(df) < 20:
        return False, "Insufficient data", 0
        
    last = df.iloc[-1]
    atr = last["ATR"]
    candle_range = last["High"] - last["Low"]
    
    # Extreme spike: Current candle is 4.5x the ATR. Hard reject.
    if candle_range > (atr * 4.5):
        return True, "Dangerous volatility spike", 30
        
    return False, "Volatility normal", 0

def check_momentum_strength(df, direction):
    """
    Detect if momentum is stalling or reversing.
    Rejects if RSI is not moving in the direction of the trade or if price action is flat.
    """
    if df is None or len(df) < 3:
        return False, "Insufficient data", 0
        
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    rsi_now = last["RSI"]
    rsi_prev = prev["RSI"]
    
    # RSI Momentum Check
    if direction == "CALL":
        if rsi_now <= rsi_prev:
            return True, "Momentum stalling (RSI flat or decreasing)", 20
        if rsi_now < 52:
            return False, "Weak bullish momentum", 10
    else: # PUT
        if rsi_now >= rsi_prev:
            return True, "Momentum stalling (RSI flat or increasing)", 20
        if rsi_now > 48:
            return False, "Weak bearish momentum", 10
            
    # Price Action Momentum (Last 3 candles)
    if len(df) >= 4:
        # Check if the last few candles are actually moving
        avg_body = (df["Close"] - df["Open"]).abs().tail(5).mean()
        last_body = abs(last["Close"] - last["Open"])
        if last_body < (avg_body * 0.5):
            return False, "Momentum weakening (small candle)", 10

    return False, "Momentum strong", 0

def check_atr_floor(df):
    """Reject if the market is literally not moving (Dead Floor)."""
    if df is None or len(df) < 1:
        return False, "Insufficient data", 0
        
    last = df.iloc[-1]
    atr = last["ATR"]
    
    # Hard floor for EUR/USD (e.g. 0.00008)
    if atr < 0.00008:
        return True, "Market too dead (Flat ATR floor)", 25
        
    return False, "Market liquidity ok", 0

def check_wick_rejection(df, direction):
    """Detect extreme reversals indicated by long wicks against the trade."""
    if df is None or len(df) < 2:
        return False, "Insufficient data", 0
        
    last = df.iloc[-1]
    body = abs(last["Close"] - last["Open"])
    if body < 0.00001: body = 0.00001
    
    upper_wick = last["High"] - max(last["Open"], last["Close"])
    lower_wick = min(last["Open"], last["Close"]) - last["Low"]
    
    # Extreme rejection: Wick > 3x Body. Hard reject.
    if direction == "CALL" and upper_wick > (body * 3.0) and upper_wick > 0.0002:
        return True, "Extreme upper wick rejection", 25
            
    if direction == "PUT" and lower_wick > (body * 3.0) and lower_wick > 0.0002:
        return True, "Extreme lower wick rejection", 25
            
    return False, "No extreme rejection", 0

def check_spread_safety(df):
    """
    Estimate spread/slippage using High-Low micro volatility.
    """
    if df is None or len(df) < 5:
        return False, "Insufficient data", 0
        
    last = df.iloc[-1]
    atr = last["ATR"]
    current_spread = last["High"] - last["Low"]
    
    avg_spread = (df["High"] - df["Low"]).tail(5).mean()
    
    # Prevent trading during abnormal spread spikes
    if current_spread > (avg_spread * 3.0) and current_spread > 0.00015:
        return True, "Abnormal spread spike detected", 25
        
    # During dangerous volatility: increase spread sensitivity
    atr_mean = df["ATR"].tail(50).mean()
    is_volatile = atr > (atr_mean * 1.5)
    
    # Reject signals if spread too large
    if is_volatile:
        if current_spread > (atr * 2.0):
            return True, "Spread too large during high volatility", 25
    else:
        if current_spread > (atr * 3.0) and current_spread > 0.0002:
            return True, "Spread/Slippage risk too high", 20
            
    # Add spread penalty to confidence
    if is_volatile and current_spread > (atr * 1.2):
        return False, "High spread penalty (Volatile)", 15
    elif current_spread > (atr * 1.5):
        return False, "Spread penalty", 10
        
    return False, "Spread normal", 0

def run_market_safety(df, direction):
    """
    Aggregate all safety checks.
    Returns (is_safe, message, total_penalty)
    """
    total_penalty = 0
    
    # 0. High Impact News Check (Immediate Reject)
    is_news, news_msg, news_penalty = check_high_impact_news()
    if is_news:
        return False, news_msg, news_penalty
    
    # 1. Session Check
    is_dead, sess_msg, sess_penalty = check_market_session()
    if is_dead:
        return False, sess_msg, sess_penalty
    total_penalty += sess_penalty
        
    # 2. Sideways Check
    is_sideways_dead, side_msg, side_penalty = check_sideways_market(df)
    if is_sideways_dead and side_penalty >= 20:
        return False, side_msg, side_penalty
    total_penalty += side_penalty
        
    # 3. Dangerous Volatility
    is_volatile, vol_msg, vol_penalty = check_dangerous_volatility(df)
    if is_volatile:
        return False, vol_msg, vol_penalty
    total_penalty += vol_penalty
        
    # 4. Wick Rejection
    is_rejected, wick_msg, wick_penalty = check_wick_rejection(df, direction)
    if is_rejected:
        return False, wick_msg, wick_penalty
    total_penalty += wick_penalty
    
    # 5. Momentum Strength
    is_weak_mom, mom_msg, mom_penalty = check_momentum_strength(df, direction)
    if is_weak_mom:
        return False, mom_msg, mom_penalty
    total_penalty += mom_penalty
    
    # 6. ATR Floor
    is_dead, atr_msg, atr_penalty = check_atr_floor(df)
    if is_dead:
        return False, atr_msg, atr_penalty
    total_penalty += atr_penalty
        
    # 7. Spread/Slippage Safety
    is_spread_high, spread_msg, spread_penalty = check_spread_safety(df)
    if is_spread_high:
        return False, spread_msg, spread_penalty
    total_penalty += spread_penalty
        
    # Cap total combined penalty to 25 to avoid over-filtering
    total_penalty = min(total_penalty, 25)
        
    return True, "Market conditions validated", total_penalty

