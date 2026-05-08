"""
Market Safety Filters
Prevents trading in weak and dangerous market conditions.
"""

import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
from typing import Tuple, Optional, Dict
from dataclasses import dataclass
from enum import Enum


class MarketSession(Enum):
    """Forex market sessions in Asia/Kolkata timezone."""
    SYDNEY = "sydney"       # 05:00 - 14:00 IST
    TOKYO = "tokyo"         # 06:00 - 15:00 IST
    LONDON = "london"       # 13:30 - 23:00 IST
    NEW_YORK = "new_york"   # 18:00 - 03:00+1 IST
    OVERLAP_LONDON_NY = "london_ny_overlap"  # 18:00 - 23:00 IST
    DEAD_HOURS = "dead"     # Low volume periods


@dataclass
class MarketSafetyConfig:
    """Configuration for market safety filters."""
    # Sideways market detection — relaxed to allow medium-quality setups
    sideways_range_multiple: float = 2.0   # was 2.5 — more relaxed
    sideways_adx_threshold: float = 15.0   # was 18.0 — more relaxed
    min_trend_ema_gap_atr: float = 0.15    # was 0.20 — more relaxed
    
    # Volatility spike rejection - STILL block dangerous spikes
    max_atr_spike_multiple: float = 2.5    # Keep dangerous spikes blocked
    min_atr_threshold: float = 0.50        # was 0.65 — more relaxed
    
    # Wick rejection — relaxed to allow more setups
    max_wick_ratio: float = 0.75           # was 0.70 — more lenient
    max_upper_wick_bullish: float = 0.55   # was 0.50 — more lenient
    max_lower_wick_bearish: float = 0.55   # was 0.50 — more lenient
    
    # Session filtering - ALLOW more sessions
    prefer_overlap_only: bool = False      # Changed: allow London/NY/overlap
    block_dead_sessions: bool = True      # Block only truly dead periods
    
    # Minimum session requirements
    require_strong_trend: bool = False     # Changed: reduced strictness
    require_momentum: bool = False          # Changed: reduced strictness
    
    # Choppy market detection — slightly relaxed
    max_choppy_candles: int = 6             # was 5 — one extra alternation allowed
    min_consecutive_direction: int = 2       # Keep at 2 consecutive candles


class MarketSafetyEngine:
    """
    Comprehensive market safety engine for filtering weak/dangerous conditions.
    """
    
    def __init__(self, config: Optional[MarketSafetyConfig] = None):
        self.config = config or MarketSafetyConfig()
        self.blocked_until: Optional[datetime] = None
        self.rejection_count = 0
        self.last_check_time: Optional[datetime] = None
    
    def _now(self) -> datetime:
        """Get current time in Asia/Kolkata timezone."""
        try:
            from zoneinfo import ZoneInfo
            return datetime.now(ZoneInfo("Asia/Kolkata"))
        except ImportError:
            return datetime.now()
    
    def get_current_session(self, now: Optional[datetime] = None) -> MarketSession:
        """Determine current forex market session."""
        if now is None:
            now = self._now()
        
        current_time = now.time()
        
        # London/NY Overlap: 18:00 - 23:00 IST (best trading)
        if time(18, 0) <= current_time <= time(23, 0):
            return MarketSession.OVERLAP_LONDON_NY
        
        # London: 13:30 - 23:00 IST
        if time(13, 30) <= current_time <= time(23, 0):
            return MarketSession.LONDON
        
        # New York: 18:00 - 03:00+1 IST (spans midnight)
        if current_time >= time(18, 0) or current_time <= time(3, 0):
            return MarketSession.NEW_YORK
        
        # Tokyo: 06:00 - 15:00 IST
        if time(6, 0) <= current_time <= time(15, 0):
            return MarketSession.TOKYO
        
        # Sydney: 05:00 - 14:00 IST
        if time(5, 0) <= current_time <= time(14, 0):
            return MarketSession.SYDNEY
        
        # Dead hours: 03:00 - 05:00 and 23:00 - 06:00 low volume
        return MarketSession.DEAD_HOURS
    
    def is_optimal_session(self, now: Optional[datetime] = None) -> Tuple[bool, str]:
        """
        Check if current session is optimal for trading.
        Returns (is_optimal, reason).
        """
        session = self.get_current_session(now)
        
        if self.config.prefer_overlap_only:
            if session == MarketSession.OVERLAP_LONDON_NY:
                return True, "London/NY overlap - optimal session"
            elif session == MarketSession.LONDON:
                return True, "London session - acceptable"
            elif session == MarketSession.NEW_YORK:
                return True, "NY session - acceptable"
            elif session == MarketSession.DEAD_HOURS:
                return False, "Dead hours - low volume, no trading"
            else:
                return False, f"{session.value} session - prefer London/NY overlap only"
        
        if self.config.block_dead_sessions and session == MarketSession.DEAD_HOURS:
            return False, "Dead hours - low volume period"
        
        return True, f"{session.value} session - trading allowed"
    
    def check_sideways_market(self, df: pd.DataFrame) -> Tuple[bool, str]:
        """
        Detect sideways/choppy market conditions.
        Returns (ok_to_trade, reason).
        """
        if df is None or len(df) < 20:
            return False, "Insufficient data for sideways detection"
        
        last = df.iloc[-1]
        
        # Method 1: Price range over lookback period
        recent_high = df["High"].tail(20).max()
        recent_low = df["Low"].tail(20).min()
        price_range = recent_high - recent_low
        
        atr_mean = float(df["ATR"].mean())
        if atr_mean <= 0:
            return False, "Invalid ATR mean"
        
        if price_range < atr_mean * self.config.sideways_range_multiple:
            return False, (
                f"Sideways market: range {price_range:.5f} < "
                f"{self.config.sideways_range_multiple}x ATR ({atr_mean * self.config.sideways_range_multiple:.5f})"
            )
        
        # Method 2: EMA convergence = weak trend
        ema50 = float(last["EMA50"]) if not pd.isna(last.get("EMA50")) else 0.0
        ema200 = float(last["EMA200"]) if not pd.isna(last.get("EMA200")) else 0.0
        ema_gap = abs(ema50 - ema200)
        
        if ema_gap < atr_mean * self.config.min_trend_ema_gap_atr:
            return False, (
                f"EMA convergence indicates weak trend: "
                f"gap {ema_gap:.5f} < {self.config.min_trend_ema_gap_atr}x ATR"
            )
        
        # Method 3: Choppy candles (too many alternating directions)
        recent_candles = min(len(df), self.config.max_choppy_candles + 5)
        directions = []
        for i in range(1, recent_candles):
            if df.iloc[-i]["Close"] > df.iloc[-i]["Open"]:
                directions.append("up")
            else:
                directions.append("down")
        
        # Count direction changes
        direction_changes = sum(1 for i in range(1, len(directions)) if directions[i] != directions[i-1])
        if direction_changes >= self.config.max_choppy_candles:
            return False, f"Choppy market: {direction_changes} direction changes in {len(directions)} candles"
        
        # Method 4: Check for consistent directional momentum
        consecutive_needed = self.config.min_consecutive_direction
        if len(directions) >= consecutive_needed:
            # Check if we have enough consecutive candles in same direction
            current_streak = 1
            max_streak = 1
            for i in range(1, len(directions)):
                if directions[i] == directions[i-1]:
                    current_streak += 1
                    max_streak = max(max_streak, current_streak)
                else:
                    current_streak = 1
            
            if max_streak < consecutive_needed:
                return False, f"No consistent momentum: max streak {max_streak} < {consecutive_needed}"
        
        return True, "Trending market detected"
    
    def check_volatility_spike(self, df: pd.DataFrame) -> Tuple[bool, str]:
        """
        Check for dangerous volatility spikes.
        Returns (ok_to_trade, reason).
        """
        if df is None or len(df) < 10:
            return False, "Insufficient data for volatility check"
        
        last = df.iloc[-1]
        atr = float(last["ATR"]) if not pd.isna(last.get("ATR")) else 0.0
        atr_mean = float(df["ATR"].mean())
        
        if atr_mean <= 0:
            return False, "Invalid ATR mean"
        
        # Reject high volatility spikes
        if atr > atr_mean * self.config.max_atr_spike_multiple:
            return False, (
                f"VOLATILITY SPIKE: ATR {atr:.5f} > "
                f"{self.config.max_atr_spike_multiple}x mean ({atr_mean:.5f})"
            )
        
        # Reject extremely low volatility (dead market)
        if atr < atr_mean * self.config.min_atr_threshold:
            return False, (
                f"Weak ATR: {atr:.5f} < {self.config.min_atr_threshold}x mean "
                f"({atr_mean * self.config.min_atr_threshold:.5f}) - dead market"
            )
        
        return True, f"Normal volatility: ATR {atr:.5f} vs mean {atr_mean:.5f}"
    
    def check_wick_rejection(self, df: pd.DataFrame, direction: str) -> Tuple[bool, str]:
        """
        Check for dangerous wick patterns.
        Returns (ok_to_trade, reason).
        """
        if df is None or len(df) < 2:
            return False, "Insufficient data for wick check"
        
        last = df.iloc[-1]
        prev = df.iloc[-2]
        
        open_price = float(last["Open"])
        high = float(last["High"])
        low = float(last["Low"])
        close = float(last["Close"])
        
        candle_range = high - low
        if candle_range <= 0:
            return False, "Invalid candle range"
        
        body_size = abs(close - open_price)
        upper_wick = high - max(open_price, close)
        lower_wick = min(open_price, close) - low
        
        upper_wick_ratio = upper_wick / candle_range
        lower_wick_ratio = lower_wick / candle_range
        body_ratio = body_size / candle_range
        
        # General wick rejection - any wick too large
        if upper_wick_ratio > self.config.max_wick_ratio:
            return False, f"Long upper wick: {upper_wick_ratio*100:.1f}% > {self.config.max_wick_ratio*100:.0f}%"
        
        if lower_wick_ratio > self.config.max_wick_ratio:
            return False, f"Long lower wick: {lower_wick_ratio*100:.1f}% > {self.config.max_wick_ratio*100:.0f}%"
        
        # Direction-specific wick checks
        direction_upper = direction.upper()
        
        if direction_upper in {"CALL", "BUY"}:
            # For CALL: upper wick is resistance/rejection
            if upper_wick_ratio > self.config.max_upper_wick_bullish:
                return False, (
                    f"Bullish rejection: upper wick {upper_wick_ratio*100:.1f}% > "
                    f"{self.config.max_upper_wick_bullish*100:.0f}%"
                )
            # Check for bearish pin bar (long upper wick, small body at bottom)
            if upper_wick > body_size * 2 and close < open_price:
                return False, "Bearish pin bar detected - rejection at highs"
                
        elif direction_upper in {"PUT", "SELL"}:
            # For PUT: lower wick is support/rejection
            if lower_wick_ratio > self.config.max_lower_wick_bearish:
                return False, (
                    f"Bearish rejection: lower wick {lower_wick_ratio*100:.1f}% > "
                    f"{self.config.max_lower_wick_bearish*100:.0f}%"
                )
            # Check for bullish pin bar (long lower wick, small body at top)
            if lower_wick > body_size * 2 and close > open_price:
                return False, "Bullish pin bar detected - rejection at lows"
        
        # Doji/spinning top rejection (small body, large wicks)
        if body_ratio < 0.2 and (upper_wick_ratio + lower_wick_ratio) > 0.7:
            return False, f"Doji/Spinning top: indecision candle (body {body_ratio*100:.1f}%)"
        
        return True, "Wick pattern acceptable"
    
    def check_weak_atr(self, df: pd.DataFrame) -> Tuple[bool, str]:
        """
        Check for weak/expanding ATR conditions.
        Returns (ok_to_trade, reason).
        """
        if df is None or len(df) < 10:
            return False, "Insufficient data for ATR check"
        
        atr_series = df["ATR"].tail(10)
        current_atr = float(atr_series.iloc[-1])
        atr_mean = float(df["ATR"].mean())
        atr_trend = float(atr_series.iloc[-1]) - float(atr_series.iloc[0])
        
        # Reject only if ATR is contracting severely (was 0.3 — relaxed to 0.5)
        if atr_trend < -atr_mean * 0.5:
            return False, f"ATR contracting severely: trend {-atr_trend:.5f} - volatility dying"
        
        # Reject if current ATR is very weak — relaxed from 60% to 50% of mean
        if current_atr < atr_mean * 0.50:
            return False, (
                f"Very weak ATR: {current_atr:.5f} < 50% of mean ({atr_mean:.5f})"
            )
        
        # Reject if ATR is extremely erratic — relaxed from 50% to 65%
        atr_std = float(atr_series.std())
        if atr_std > atr_mean * 0.65:
            return False, f"Extremely erratic volatility: ATR std {atr_std:.5f} > 65% of mean"
        
        return True, f"ATR acceptable: {current_atr:.5f} vs mean {atr_mean:.5f}"
    
    def check_strong_breakout(self, df: pd.DataFrame, direction: str) -> Tuple[bool, str]:
        """
        Check for strong momentum breakout that can override some weak filters.
        Returns (is_strong_breakout, reason).
        """
        if df is None or len(df) < 10:
            return False, "Insufficient data"
        
        last = df.iloc[-1]
        prev = df.iloc[-2]
        
        # Strong EMA alignment
        ema50 = float(last["EMA50"]) if not pd.isna(last.get("EMA50")) else 0.0
        ema200 = float(last["EMA200"]) if not pd.isna(last.get("EMA200")) else 0.0
        ema_gap = abs(ema50 - ema200)
        atr = float(last["ATR"]) if not pd.isna(last.get("ATR")) else 0.0
        
        # Strong EMA gap
        if ema_gap < atr * 0.5:
            return False, "Weak EMA gap"
        
        # Strong momentum
        price_change = abs(float(last["Close"]) - float(prev["Close"]))
        if price_change < atr * 0.8:
            return False, "Weak momentum"
        
        # Strong EMA slope
        ema50_prev = float(prev["EMA50"]) if not pd.isna(prev.get("EMA50")) else ema50
        if direction.upper() in {"CALL", "BUY"}:
            if ema50 <= ema50_prev:
                return False, "EMA50 not rising"
            if ema50 <= ema200:
                return False, "EMA50 below EMA200"
        else:
            if ema50 >= ema50_prev:
                return False, "EMA50 not falling"
            if ema50 >= ema200:
                return False, "EMA50 above EMA200"
        
        # Strong ATR
        atr_mean = float(df["ATR"].mean())
        if atr < atr_mean * 0.8:
            return False, "Weak ATR"
        
        return True, "Strong breakout detected"
    
    def run_all_checks(
        self, 
        df: pd.DataFrame, 
        direction: str,
        now: Optional[datetime] = None
    ) -> Tuple[bool, Dict[str, Tuple[bool, str]]]:
        """
        Run all market safety checks.
        Returns (overall_ok, check_results).
        """
        results = {}
        
# Session check - never bypass
        session_ok, session_reason = self.is_optimal_session(now)
        
        # Check for strong breakout - can override WEAK filters only
        breakout_ok, breakout_reason = self.check_strong_breakout(df, direction)
        
        # Sideways check - allow if strong breakout
        sideways_ok, sideways_reason = self.check_sideways_market(df)
        if breakout_ok:
            sideways_ok = True  # Override for strong breakout
        results["sideways"] = (sideways_ok, sideways_reason)
        
        # Volatility spike check - DO NOT bypass dangerous spikes
        vol_ok, vol_reason = self.check_volatility_spike(df)
        # Removed breakout override for volatility - keep dangerous spikes blocked!
        results["volatility"] = (vol_ok, vol_reason)
        
        # Wick check - allow if strong breakout (less dangerous)
        wick_ok, wick_reason = self.check_wick_rejection(df, direction)
        if breakout_ok:
            wick_ok = True  # Override for strong breakout
        results["wick"] = (wick_ok, wick_reason)
        
        # Weak ATR check - allow if strong breakout (less dangerous)
        atr_ok, atr_reason = self.check_weak_atr(df)
        if breakout_ok:
            atr_ok = True  # Override for strong breakout
        results["atr"] = (atr_ok, atr_reason)
        
        # Add breakout status to results
        results["breakout"] = (breakout_ok, breakout_reason)
        
        # Overall check - session and volatility must always pass (dangerous)
        all_passed = results["session"][0] and results["volatility"][0] and (
            sideways_ok or wick_ok or atr_ok or breakout_ok
        )
        
        return all_passed, results
    
    def get_rejection_summary(self, results: Dict[str, Tuple[bool, str]]) -> str:
        """Generate summary of failed checks."""
        failures = [f"{name}: {reason}" for name, (ok, reason) in results.items() if not ok]
        if failures:
            return " | ".join(failures)
        return "All checks passed"


# Global engine instance
_market_safety_engine: Optional[MarketSafetyEngine] = None


def get_market_safety_engine() -> MarketSafetyEngine:
    """Get or create the global market safety engine."""
    global _market_safety_engine
    if _market_safety_engine is None:
        _market_safety_engine = MarketSafetyEngine()
    return _market_safety_engine


def reset_market_safety_engine() -> None:
    """Reset the global market safety engine."""
    global _market_safety_engine
    _market_safety_engine = MarketSafetyEngine()


def is_optimal_session(now: Optional[datetime] = None) -> Tuple[bool, str]:
    """Convenience function to check trading session."""
    engine = get_market_safety_engine()
    return engine.is_optimal_session(now)


def check_market_safety(
    df: pd.DataFrame, 
    direction: str,
    now: Optional[datetime] = None
) -> Tuple[bool, str]:
    """
    Convenience function to run all market safety checks.
    Returns (ok_to_trade, reason).
    """
    engine = get_market_safety_engine()
    all_ok, results = engine.run_all_checks(df, direction, now)
    
    if all_ok:
        return True, "Market conditions favorable"
    
    summary = engine.get_rejection_summary(results)
    return False, summary


def get_current_session(now: Optional[datetime] = None) -> str:
    """Get current market session name."""
    engine = get_market_safety_engine()
    session = engine.get_current_session(now)
    return session.value
