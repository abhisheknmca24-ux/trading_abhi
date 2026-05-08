"""
Martingale Safety Engine
Prevents dangerous martingale behavior through strict validation.
"""

import pandas as pd
from datetime import datetime, timedelta
from typing import Tuple, Optional
from dataclasses import dataclass


@dataclass
class MartingaleSafetyConfig:
    """Configuration for martingale safety checks."""
    # Confidence requirements - relaxed from 80 to 75
    min_confidence: int = 75
    
    # ATR requirements - relaxed from 1.2 to 1.1
    strong_atr_multiplier: float = 1.1  # ATR must be > 1.1x mean
    max_volatility_spike_multiplier: float = 2.5  # ATR > 2.5x mean = spike
    
    # EMA slope requirements - relaxed from 0.3 to 0.25
    min_ema_slope_consistency: int = 3  # candles
    min_ema_gap_atr_multiple: float = 0.25  # EMA50-EMA200 gap must be > 0.25x ATR
    
    # Momentum requirements - relaxed from 0.5 to 0.4
    min_momentum_atr_multiple: float = 0.4  # price change > 0.4x ATR
    
    # Limits
    max_martingale_steps: int = 1
    max_consecutive_losses: int = 2
    
    # Emergency stop
    emergency_stop_after_losses: int = 3
    emergency_stop_duration_minutes: int = 30


class MartingaleSafetyEngine:
    """
    Safety engine to prevent dangerous martingale behavior.
    """
    
    def __init__(self, config: Optional[MartingaleSafetyConfig] = None):
        self.config = config or MartingaleSafetyConfig()
        self.consecutive_losses = 0
        self.total_martingale_steps_today = 0
        self.emergency_stop_until: Optional[datetime] = None
        self.last_reset_date: Optional[datetime.date] = None
        self.martingale_history: list = []  # Track which signals used martingale
    
    def reset_daily(self, now: datetime) -> None:
        """Reset daily counters."""
        current_date = now.date()
        if self.last_reset_date != current_date:
            self.total_martingale_steps_today = 0
            self.consecutive_losses = 0
            self.martingale_history = []
            self.last_reset_date = current_date
    
    def is_emergency_stop_active(self, now: datetime) -> bool:
        """Check if emergency stop mode is currently active."""
        if self.emergency_stop_until is None:
            return False
        return now < self.emergency_stop_until
    
    def activate_emergency_stop(self, now: datetime) -> None:
        """Activate emergency stop mode."""
        self.emergency_stop_until = now + timedelta(
            minutes=self.config.emergency_stop_duration_minutes
        )
    
    def record_result(self, is_win: bool, signal_time: datetime, is_martingale: bool = False) -> None:
        """Record trade result for loss tracking."""
        if is_martingale:
            self.martingale_history.append({
                "signal_time": signal_time,
                "result": "WIN" if is_win else "LOSS"
            })
        
        if is_win:
            self.consecutive_losses = 0
        else:
            self.consecutive_losses += 1
            # Activate emergency stop after too many consecutive losses
            if self.consecutive_losses >= self.config.emergency_stop_after_losses:
                self.activate_emergency_stop(signal_time)
    
    def check_consecutive_loss_protection(self) -> Tuple[bool, str]:
        """
        Check if consecutive loss protection is triggered.
        Returns (allowed, reason).
        """
        if self.consecutive_losses >= self.config.max_consecutive_losses:
            return False, f"Consecutive loss protection: {self.consecutive_losses} losses in a row"
        return True, ""
    
    def check_step_limit(self, signal_key: str) -> Tuple[bool, str]:
        """
        Check if maximum martingale steps reached for this signal.
        Returns (allowed, reason).
        """
        # Count martingale entries for this signal
        signal_martingales = sum(
            1 for h in self.martingale_history 
            if h.get("signal_key") == signal_key
        )
        
        if signal_martingales >= self.config.max_martingale_steps:
            return False, f"Max martingale steps ({self.config.max_martingale_steps}) reached for this signal"
        
        return True, ""
    
    def _check_confidence(self, confidence: int) -> Tuple[bool, str]:
        """Check if confidence meets minimum requirement."""
        if confidence < self.config.min_confidence:
            return False, f"Confidence {confidence}% < required {self.config.min_confidence}%"
        return True, ""
    
    def _check_atr(self, df: pd.DataFrame) -> Tuple[bool, str]:
        """Check ATR strength and volatility spikes."""
        if df is None or len(df) < 10:
            return False, "Insufficient data for ATR check"
        
        last = df.iloc[-1]
        atr = float(last["ATR"]) if not pd.isna(last.get("ATR")) else 0.0
        atr_mean = float(df["ATR"].mean()) if not pd.isna(df["ATR"].mean()) else 0.0
        
        if atr_mean <= 0:
            return False, "Invalid ATR mean"
        
        # Check for volatility spike
        if atr > atr_mean * self.config.max_volatility_spike_multiplier:
            return False, f"High volatility spike detected (ATR {atr:.5f} > {self.config.max_volatility_spike_multiplier}x mean)"
        
        # Check for strong ATR
        if atr < atr_mean * self.config.strong_atr_multiplier:
            return False, f"Weak ATR ({atr:.5f} < {self.config.strong_atr_multiplier}x mean {atr_mean:.5f})"
        
        return True, ""
    
    def _check_ema_slope(self, df: pd.DataFrame, direction: str) -> Tuple[bool, str]:
        """Check EMA slope consistency and strength."""
        if df is None or len(df) < self.config.min_ema_slope_consistency + 2:
            return False, "Insufficient data for EMA slope check"
        
        last = df.iloc[-1]
        
        # Check EMA gap strength
        ema50 = float(last["EMA50"]) if not pd.isna(last.get("EMA50")) else 0.0
        ema200 = float(last["EMA200"]) if not pd.isna(last.get("EMA200")) else 0.0
        atr = float(last["ATR"]) if not pd.isna(last.get("ATR")) else 0.0
        
        ema_gap = abs(ema50 - ema200)
        min_gap = max(0.0002, atr * self.config.min_ema_gap_atr_multiple)
        
        if ema_gap < min_gap:
            return False, f"Weak trend (EMA gap {ema_gap:.5f} < {min_gap:.5f})"
        
        # Check slope consistency over multiple candles
        direction_ok = True
        if direction.upper() in {"CALL", "BUY"}:
            for i in range(self.config.min_ema_slope_consistency):
                if not (df.iloc[-1-i]["EMA50"] > df.iloc[-2-i]["EMA50"]):
                    direction_ok = False
                    break
            if not direction_ok:
                return False, "EMA50 not consistently rising"
            if ema50 <= ema200:
                return False, "EMA50 below EMA200 for CALL"
        else:
            for i in range(self.config.min_ema_slope_consistency):
                if not (df.iloc[-1-i]["EMA50"] < df.iloc[-2-i]["EMA50"]):
                    direction_ok = False
                    break
            if not direction_ok:
                return False, "EMA50 not consistently falling"
            if ema50 >= ema200:
                return False, "EMA50 above EMA200 for PUT"
        
        return True, ""
    
    def _check_momentum(self, df: pd.DataFrame) -> Tuple[bool, str]:
        """Check for strong momentum."""
        if df is None or len(df) < 3:
            return False, "Insufficient data for momentum check"
        
        last = df.iloc[-1]
        prev = df.iloc[-2]
        
        atr = float(last["ATR"]) if not pd.isna(last.get("ATR")) else 0.0
        price_change = abs(float(last["Close"]) - float(prev["Close"]))
        
        min_momentum = max(0.0002, atr * self.config.min_momentum_atr_multiple)
        
        if price_change < min_momentum:
            return False, f"Weak momentum ({price_change:.5f} < {min_momentum:.5f})"
        
        # Check candle strength
        candle_size = abs(float(last["Close"]) - float(last["Open"]))
        avg_candle = float((df["Close"] - df["Open"]).abs().tail(10).mean())
        
        if candle_size < avg_candle * 0.7:
            return False, f"Weak candle strength"
        
        return True, ""
    
    def _check_sideways_market(self, df: pd.DataFrame) -> Tuple[bool, str]:
        """Check if market is in sideways/choppy condition."""
        if df is None or len(df) < 20:
            return False, "Insufficient data for sideways check"
        
        # Calculate price range over last 20 candles
        recent_high = df["High"].tail(20).max()
        recent_low = df["Low"].tail(20).min()
        price_range = recent_high - recent_low
        
        atr_mean = float(df["ATR"].mean())
        
        # If price range is small relative to ATR, market is sideways
        if price_range < atr_mean * 3:
            return False, f"Sideways market detected (range {price_range:.5f} < 3x ATR {atr_mean:.5f})"
        
        # Check for ADX-like condition using EMA convergence
        last = df.iloc[-1]
        ema_gap = abs(float(last["EMA50"]) - float(last["EMA200"]))
        atr = float(last["ATR"])
        
        if ema_gap < atr * 0.2:
            return False, "EMA convergence indicates weak trend/sideways"
        
        return True, ""
    
    def can_enter_martingale(
        self, 
        df: pd.DataFrame, 
        direction: str, 
        confidence: int,
        signal_key: str,
        now: datetime
    ) -> Tuple[bool, str]:
        """
        Comprehensive check if martingale entry is safe.
        Returns (allowed, reason).
        """
        self.reset_daily(now)
        
        # 1. Emergency stop check
        if self.is_emergency_stop_active(now):
            remaining = (self.emergency_stop_until - now).total_seconds() // 60
            return False, f"Emergency stop active ({remaining:.0f} min remaining)"
        
        # 2. Consecutive loss protection
        allowed, reason = self.check_consecutive_loss_protection()
        if not allowed:
            return False, reason
        
        # 3. Step limit check
        allowed, reason = self.check_step_limit(signal_key)
        if not allowed:
            return False, reason
        
        # 4. Confidence check
        allowed, reason = self._check_confidence(confidence)
        if not allowed:
            return False, reason
        
        # 5. ATR check (strong ATR + no volatility spike)
        allowed, reason = self._check_atr(df)
        if not allowed:
            return False, reason
        
        # 6. EMA slope check
        allowed, reason = self._check_ema_slope(df, direction)
        if not allowed:
            return False, reason
        
        # 7. Momentum check
        allowed, reason = self._check_momentum(df)
        if not allowed:
            return False, reason
        
        # 8. Sideways market check
        allowed, reason = self._check_sideways_market(df)
        if not allowed:
            return False, reason
        
        return True, "All safety checks passed"
    
    def get_status_report(self) -> str:
        """Get current safety engine status."""
        status = []
        status.append(f"Consecutive Losses: {self.consecutive_losses}/{self.config.max_consecutive_losses}")
        status.append(f"Martingale Steps Today: {self.total_martingale_steps_today}")
        
        if self.emergency_stop_until:
            now = datetime.now(self.emergency_stop_until.tzinfo)
            if self.is_emergency_stop_active(now):
                remaining = (self.emergency_stop_until - now).total_seconds() // 60
                status.append(f"Emergency Stop: ACTIVE ({remaining:.0f} min remaining)")
            else:
                status.append("Emergency Stop: EXPIRED")
        else:
            status.append("Emergency Stop: INACTIVE")
        
        return " | ".join(status)


# Global safety engine instance
_martingale_safety_engine: Optional[MartingaleSafetyEngine] = None


def get_martingale_safety_engine() -> MartingaleSafetyEngine:
    """Get or create the global martingale safety engine."""
    global _martingale_safety_engine
    if _martingale_safety_engine is None:
        _martingale_safety_engine = MartingaleSafetyEngine()
    return _martingale_safety_engine


def reset_martingale_safety_engine() -> None:
    """Reset the global martingale safety engine."""
    global _martingale_safety_engine
    _martingale_safety_engine = MartingaleSafetyEngine()


def can_enter_martingale(
    df: pd.DataFrame, 
    direction: str, 
    confidence: int,
    signal_key: str,
    now: datetime
) -> Tuple[bool, str]:
    """
    Convenience function to check if martingale is safe.
    """
    engine = get_martingale_safety_engine()
    return engine.can_enter_martingale(df, direction, confidence, signal_key, now)


def record_martingale_result(is_win: bool, signal_time: datetime) -> None:
    """
    Record the result of a martingale trade.
    """
    engine = get_martingale_safety_engine()
    engine.record_result(is_win, signal_time, is_martingale=True)


def get_martingale_status() -> str:
    """Get current safety engine status."""
    engine = get_martingale_safety_engine()
    return engine.get_status_report()
