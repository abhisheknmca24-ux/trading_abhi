"""
PHASE 6: Market Filtering System

Detects and avoids weak market conditions:
- Sideways market detection (tiny EMA gap + weak ATR)
- Volatility spike detection (abnormal huge candles)
- Candle wick analysis (large opposite-direction wicks)
- Spread/volatility instability protection
- Market session filtering (London/NY optimal hours)
"""

import pandas as pd
from typing import Tuple


# ============================================================================
# MARKET SESSION FILTERING (Forex EUR/USD)
# ============================================================================

def is_optimal_trading_session() -> Tuple[bool, str]:
    """
    Check if current time is in optimal trading sessions.
    
    Optimal: London/New York overlap (14:00-17:00 UTC)
    Good: London session (08:00-17:00 UTC) or NY session (14:00-23:00 UTC)
    Avoid: Dead hours (00:00-08:00, 23:00-24:00 UTC)
    """
    now = pd.Timestamp.now(tz="UTC")
    hour = now.hour
    
    # DEAD HOURS: Avoid completely
    if hour >= 23 or hour < 8:
        return False, f"Dead market hours ({hour:02d}:00 UTC, avoid 23:00-08:00)"
    
    # OPTIMAL: London/NY overlap (14:00-17:00 UTC)
    if 14 <= hour < 17:
        return True, f"Optimal London/NY overlap ({hour:02d}:00 UTC)"
    
    # GOOD: London session (08:00-14:00 UTC, excluding overlap)
    if 8 <= hour < 14:
        return True, f"Good London session ({hour:02d}:00 UTC)"
    
    # GOOD: NY session (17:00-23:00 UTC, excluding overlap)
    if 17 <= hour < 23:
        return True, f"Good NY session ({hour:02d}:00 UTC)"
    
    return False, f"Unknown market session ({hour:02d}:00 UTC)"


# ============================================================================
# SIDEWAYS MARKET DETECTION
# ============================================================================

def is_sideways_market(df: pd.DataFrame, direction: str) -> Tuple[bool, str]:
    """
    Detect sideways markets where:
    - EMA gap is tiny (TrendStrength < threshold)
    - AND ATR is weak (ATR <= mean)
    
    Returns: (is_sideways, reason)
    """
    if df is None or len(df) < 50:
        return False, "Insufficient data"
    
    last = df.iloc[-1]
    atr = float(last["ATR"]) if not pd.isna(last.get("ATR")) else 0
    atr_mean = float(df["ATR"].tail(20).mean()) if not pd.isna(df["ATR"].tail(20).mean()) else 0
    trend_strength = float(last["TrendStrength"]) if not pd.isna(last.get("TrendStrength")) else 0
    
    # Threshold: Tiny EMA gap
    min_trend_strength = max(0.0002, atr_mean * 0.15)  # PHASE 6: More lenient than Phase 5
    trend_too_weak = trend_strength < min_trend_strength
    
    # Weak ATR
    atr_weak = atr <= atr_mean
    
    # Both conditions trigger sideways detection
    if trend_too_weak and atr_weak:
        return True, (
            f"Sideways market: "
            f"TrendStrength {trend_strength:.6f} < {min_trend_strength:.6f} AND "
            f"ATR {atr:.6f} <= {atr_mean:.6f}"
        )
    
    return False, "Market trending normally"


# ============================================================================
# VOLATILITY SPIKE DETECTION
# ============================================================================

def detect_volatility_spike(df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Detect abnormal huge candles that indicate volatility spikes.
    
    Abnormal candle = size > 2.0× recent average
    """
    if df is None or len(df) < 20:
        return False, "Insufficient data"
    
    last = df.iloc[-1]
    current_candle_size = abs(float(last["Close"]) - float(last["Open"]))
    
    # Average candle size from last 10 candles
    avg_candle = (df["Close"] - df["Open"]).abs().tail(10).mean()
    
    # Spike threshold: 2.0× average
    spike_threshold = avg_candle * 2.0
    
    if current_candle_size > spike_threshold:
        return True, (
            f"Volatility spike: "
            f"Current candle {current_candle_size:.6f} > {spike_threshold:.6f} "
            f"(2.0× avg of {avg_candle:.6f})"
        )
    
    return False, "Normal volatility"


# ============================================================================
# CANDLE WICK ANALYSIS
# ============================================================================

def analyze_candle_wicks(df: pd.DataFrame, direction: str) -> Tuple[bool, str]:
    """
    Detect large opposite-direction wicks (rejection candles).
    
    For CALL signals (bullish):
    - Large lower wick (rejection of bearish move) is OK
    - Large upper wick (rejection of bullish move) is BAD
    
    For PUT signals (bearish):
    - Large upper wick (rejection of bullish move) is OK
    - Large lower wick (rejection of bearish move) is BAD
    
    Rejection wick threshold: > 60% of candle body
    """
    if df is None or len(df) < 10:
        return False, "Insufficient data"
    
    last = df.iloc[-1]
    open_price = float(last["Open"])
    close_price = float(last["Close"])
    high = float(last["High"])
    low = float(last["Low"])
    
    # Candle body (size)
    body_size = abs(close_price - open_price)
    if body_size < 0.00001:  # Prevent division by tiny numbers
        return False, "Doji candle (no body)"
    
    # Wick sizes
    if close_price > open_price:  # Bullish candle
        upper_wick = high - close_price
        lower_wick = open_price - low
        direction_name = "bullish"
    else:  # Bearish candle
        upper_wick = high - open_price
        lower_wick = close_price - low
        direction_name = "bearish"
    
    wick_threshold = body_size * 0.60  # 60% of body size
    
    # CALL signals (we want lower wick, not upper wick)
    if direction == "CALL":
        if upper_wick > wick_threshold:
            return True, (
                f"Rejection candle for CALL: "
                f"Large upper wick {upper_wick:.6f} > {wick_threshold:.6f} "
                f"(60% of body {body_size:.6f})"
            )
    
    # PUT signals (we want upper wick, not lower wick)
    elif direction == "PUT":
        if lower_wick > wick_threshold:
            return True, (
                f"Rejection candle for PUT: "
                f"Large lower wick {lower_wick:.6f} > {wick_threshold:.6f} "
                f"(60% of body {body_size:.6f})"
            )
    
    return False, f"Candle wick structure OK (direction: {direction_name})"


# ============================================================================
# VOLATILITY INSTABILITY DETECTION
# ============================================================================

def detect_volatility_instability(df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Detect sudden changes in volatility that indicate instability.
    
    Instability = Recent ATR varies dramatically from recent norm
    """
    if df is None or len(df) < 30:
        return False, "Insufficient data"
    
    # Get last 5 ATR values
    recent_atr = df["ATR"].tail(5).values
    # Get average of ATR from 5-25 candles ago (middle range)
    historical_atr = df["ATR"].iloc[-25:-5].mean()
    
    if pd.isna(historical_atr) or historical_atr == 0:
        return False, "Insufficient historical data"
    
    # Check if any recent candle shows extreme ATR variation
    max_recent_atr = max(recent_atr)
    min_recent_atr = min(recent_atr)
    
    # Instability: Recent ATR changes by more than 50%
    atr_range = max_recent_atr - min_recent_atr
    range_percent = (atr_range / historical_atr) * 100
    
    if range_percent > 50:
        return True, (
            f"Volatility instability detected: "
            f"ATR range {range_percent:.1f}% > 50% threshold "
            f"(recent: {min_recent_atr:.6f}-{max_recent_atr:.6f}, "
            f"historical: {historical_atr:.6f})"
        )
    
    return False, f"Volatility stable (range: {range_percent:.1f}%)"


# ============================================================================
# COMPOSITE MARKET QUALITY CHECK
# ============================================================================

def check_market_quality(df: pd.DataFrame, direction: str) -> Tuple[bool, str]:
    """
    Comprehensive market quality check combining all Phase 6 filters.
    
    Returns: (market_is_good, reason_or_summary)
    """
    if df is None or len(df) < 50:
        return False, "Insufficient data for market quality check"
    
    checks = []
    all_passed = True
    
    # Check 1: Trading session
    session_ok, session_reason = is_optimal_trading_session()
    checks.append(("Session", session_ok, session_reason))
    if not session_ok and pd.Timestamp.now(tz="UTC").hour < 8:
        all_passed = False
    
    # Check 2: Sideways market
    sideways, sideways_reason = is_sideways_market(df, direction)
    if sideways:
        all_passed = False
    checks.append(("Sideways", not sideways, sideways_reason))
    
    # Check 3: Volatility spike
    spike, spike_reason = detect_volatility_spike(df)
    if spike:
        all_passed = False
    checks.append(("Vol Spike", not spike, spike_reason))
    
    # Check 4: Candle wicks
    bad_wicks, wick_reason = analyze_candle_wicks(df, direction)
    if bad_wicks:
        all_passed = False
    checks.append(("Wicks", not bad_wicks, wick_reason))
    
    # Check 5: Volatility instability
    unstable, unstable_reason = detect_volatility_instability(df)
    if unstable:
        all_passed = False
    checks.append(("Instability", not unstable, unstable_reason))
    
    # Format summary
    if all_passed:
        summary = "✅ Market quality excellent"
        for check_name, passed, reason in checks:
            summary += f"\n  ✓ {check_name}: {reason}"
        return True, summary
    else:
        failures = [
            f"{name}: {reason}"
            for name, passed, reason in checks
            if not passed
        ]
        summary = f"❌ Market quality issues detected:\n"
        summary += "\n".join(f"  ✗ {failure}" for failure in failures)
        return False, summary


# ============================================================================
# DETAILED REPORT FOR LOGGING
# ============================================================================

def generate_market_filter_report(df: pd.DataFrame, direction: str) -> str:
    """Generate detailed market filter report for logging."""
    if df is None:
        return "No data available for market filter report"
    
    report = "\n" + "=" * 60 + "\n"
    report += "PHASE 6: MARKET QUALITY ANALYSIS\n"
    report += "=" * 60 + "\n\n"
    
    # Session
    session_ok, session_msg = is_optimal_trading_session()
    report += f"Market Session:\n  {session_msg}\n\n"
    
    # Sideways
    sideways, sideways_msg = is_sideways_market(df, direction)
    report += f"Sideways Detection:\n  {'❌ ' if sideways else '✅ '}{sideways_msg}\n\n"
    
    # Volatility spike
    spike, spike_msg = detect_volatility_spike(df)
    report += f"Volatility Spike:\n  {'❌ ' if spike else '✅ '}{spike_msg}\n\n"
    
    # Wicks
    bad_wicks, wick_msg = analyze_candle_wicks(df, direction)
    report += f"Candle Wick Analysis:\n  {'❌ ' if bad_wicks else '✅ '}{wick_msg}\n\n"
    
    # Instability
    unstable, unstable_msg = detect_volatility_instability(df)
    report += f"Volatility Instability:\n  {'❌ ' if unstable else '✅ '}{unstable_msg}\n\n"
    
    # Overall
    market_ok, market_summary = check_market_quality(df, direction)
    report += f"Overall Assessment:\n{market_summary}\n"
    report += "\n" + "=" * 60 + "\n"
    
    return report
