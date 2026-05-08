"""
Risk Management Engine
Comprehensive risk controls: daily trade limits, cooldowns, safe mode, dynamic thresholds, and loss limits.
"""

import os
import json
import time
from datetime import datetime, timedelta, date
from enum import Enum
from typing import Optional, Dict, List, Tuple
from dataclasses import dataclass, asdict

try:
    from zoneinfo import ZoneInfo
    TIMEZONE = ZoneInfo("Asia/Kolkata")
except ImportError:
    TIMEZONE = None

try:
    from compression_utils import CompressedJsonStorage
    USE_COMPRESSION = True
except ImportError:
    USE_COMPRESSION = False


class TradingMode(Enum):
    """Trading modes based on risk conditions."""
    AGGRESSIVE = "aggressive"  # Max confidence, normal thresholds
    NORMAL = "normal"           # Standard operation
    CAUTIOUS = "cautious"       # Elevated thresholds, higher confidence needed
    SAFE_MODE = "safe_mode"     # Minimal trades, very high confidence, reduced position
    EMERGENCY_STOP = "emergency_stop"  # No trading


class CooldownReason(Enum):
    """Reasons for cooldown activation."""
    LOSS = "loss"              # After losing trade
    CONSECUTIVE_LOSSES = "consecutive_losses"
    DAILY_LOSS_LIMIT = "daily_loss_limit"
    MAX_DAILY_TRADES = "max_daily_trades"
    WEAK_MARKET = "weak_market"


@dataclass
class RiskConfig:
    """Risk management configuration."""
    # Daily trade limits - safer
    max_daily_trades: int = 6           # Reduced from 8 for safer trading
    max_daily_loss_pips: float = 120.0  # Keep at 120
    
    # Cooldown system - reduced cooldown times
    cooldown_after_loss_minutes: int = 10   # reduced from 15
    cooldown_after_consecutive_losses_minutes: int = 20  # reduced from 30
    consecutive_loss_threshold: int = 3  # increased from 2
    
    # Safe mode triggers - much less aggressive
    enable_safe_mode: bool = True
    safe_mode_weak_market_threshold: float = 35.0  # reduced from 40
    safe_mode_max_trades: int = 4      # increased from 3
    safe_mode_min_confidence: int = 75  # reduced from 80
    safe_mode_position_reduction: float = 0.8  # reduced from 0.7
    
    # Dynamic threshold adjustment - capped
    enable_dynamic_thresholds: bool = True
    base_confidence_threshold: int = 68  # reduced from 70
    weak_market_threshold_bonus: int = 5  # reduced from 7 - CAP at +5
    recent_loss_threshold_bonus: int = 3   # reduced from 3 - CAP at +3
    max_threshold_bonus: int = 10  # NEW: Cap total threshold increase at +10
    
    # Session-based adjustments
    early_session_threshold_bonus: int = 5  # Earlier in day, need more confidence
    consolidation_period_minutes: int = 5   # Wait after weak signals


@dataclass
class DailyStats:
    """Daily trading statistics."""
    date: date
    trades_count: int = 0
    wins: int = 0
    losses: int = 0
    total_profit_pips: float = 0.0
    total_loss_pips: float = 0.0
    consecutive_losses: int = 0
    last_trade_time: Optional[datetime] = None
    last_trade_result: Optional[str] = None  # 'win', 'loss', 'breakeven'
    mode_history: List[Dict] = None
    
    def __post_init__(self):
        if self.mode_history is None:
            self.mode_history = []
    
    @property
    def net_profit_pips(self) -> float:
        """Net profit in pips."""
        return self.total_profit_pips - self.total_loss_pips
    
    @property
    def win_rate(self) -> float:
        """Win rate percentage."""
        if self.trades_count == 0:
            return 0.0
        return (self.wins / self.trades_count) * 100
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON storage."""
        data = asdict(self)
        data['date'] = self.date.isoformat()
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'DailyStats':
        """Create from dictionary."""
        data = data.copy()
        data['date'] = date.fromisoformat(data['date'])
        return cls(**data)


class RiskManagementEngine:
    """
    Comprehensive risk management system.
    Handles daily limits, cooldowns, safe mode, and dynamic threshold adjustment.
    """
    
    STATE_FILE = "risk_management_state.json"
    SAVE_DEBOUNCE_SECONDS = 45  # Save at most every 45 seconds
    
    def __init__(self, config: Optional[RiskConfig] = None):
        self.config = config or RiskConfig()
        self.daily_stats: Dict[date, DailyStats] = {}
        self.cooldown_until: Optional[datetime] = None
        self.cooldown_reason: Optional[CooldownReason] = None
        self.trading_mode: TradingMode = TradingMode.NORMAL
        self.safe_mode_trades_today: int = 0
        self._last_save_time = 0  # For debouncing
        self._pending_saves = 0   # Track pending updates
        self._load_state()
    
    def _now(self) -> datetime:
        """Get current time in Asia/Kolkata timezone."""
        if TIMEZONE:
            return datetime.now(TIMEZONE)
        return datetime.now()
    
    def _today(self) -> date:
        """Get today's date."""
        return self._now().date()
    
    def _load_state(self) -> None:
        """Load risk management state from file."""
        try:
            data = None
            
            if USE_COMPRESSION:
                data = CompressedJsonStorage.load(self.STATE_FILE)
            else:
                # Fallback to manual JSON loading
                if os.path.exists(self.STATE_FILE):
                    with open(self.STATE_FILE, 'r') as f:
                        data = json.load(f)
            
            if not data:
                return
            
            # Restore daily stats
            for stats_data in data.get('daily_stats', []):
                stats = DailyStats.from_dict(stats_data)
                self.daily_stats[stats.date] = stats
            
            # Restore cooldown
            cooldown_until_str = data.get('cooldown_until')
            if cooldown_until_str:
                self.cooldown_until = datetime.fromisoformat(cooldown_until_str)
            
            # Restore mode
            mode_str = data.get('trading_mode')
            if mode_str:
                try:
                    self.trading_mode = TradingMode(mode_str)
                except ValueError:
                    self.trading_mode = TradingMode.NORMAL
        except Exception as e:
            print(f"[RiskManager] Error loading state: {e}")
    
    def _save_state(self, force: bool = False) -> None:
        """Save risk management state to file with debouncing."""
        now_ts = time.time()
        self._pending_saves += 1
        
        # Debounce: save if forced, or enough updates, or enough time passed
        if not force and self._pending_saves < 3 and (now_ts - self._last_save_time) < self.SAVE_DEBOUNCE_SECONDS:
            return
        
        try:
            data = {
                'daily_stats': [
                    stats.to_dict() for stats in self.daily_stats.values()
                ],
                'cooldown_until': self.cooldown_until.isoformat() if self.cooldown_until else None,
                'cooldown_reason': self.cooldown_reason.value if self.cooldown_reason else None,
                'trading_mode': self.trading_mode.value,
                'saved_at': self._now().isoformat()
            }
            
            if USE_COMPRESSION:
                CompressedJsonStorage.save(self.STATE_FILE, data, compress=True)
            else:
                # Fallback to manual JSON saving
                with open(self.STATE_FILE, 'w') as f:
                    json.dump(data, f, indent=2)
            
            self._last_save_time = now_ts
            self._pending_saves = 0
        except Exception as e:
            print(f"[RiskManager] Error saving state: {e}")
    
    def _get_today_stats(self) -> DailyStats:
        """Get or create today's statistics."""
        today = self._today()
        
        if today not in self.daily_stats:
            self.daily_stats[today] = DailyStats(date=today)
        
        return self.daily_stats[today]
    
    def _cleanup_old_stats(self, days_to_keep: int = 60) -> None:
        """Remove stats older than specified days."""
        cutoff_date = self._today() - timedelta(days=days_to_keep)
        old_dates = [d for d in self.daily_stats.keys() if d < cutoff_date]
        
        for old_date in old_dates:
            del self.daily_stats[old_date]
    
    def _update_trading_mode(self) -> None:
        """Update trading mode based on current conditions."""
        now = self._now()
        stats = self._get_today_stats()
        
        # Check if in cooldown
        if self.cooldown_until and now < self.cooldown_until:
            self.trading_mode = TradingMode.CAUTIOUS
            return
        
        # Check emergency stop conditions
        if stats.net_profit_pips <= -self.config.max_daily_loss_pips:
            self.trading_mode = TradingMode.EMERGENCY_STOP
            return
        
        if stats.trades_count >= self.config.max_daily_trades:
            self.trading_mode = TradingMode.EMERGENCY_STOP
            return
        
        # Check safe mode triggers - less aggressive
        if self.config.enable_safe_mode:
            # Only trigger after 4+ trades with very poor win rate (< 35%)
            if stats.trades_count >= 4:
                if stats.win_rate < 35:  # was 50% - now much less aggressive
                    self.trading_mode = TradingMode.SAFE_MODE
                    return
            
            # Only trigger after 3+ consecutive losses (was 2)
            if stats.consecutive_losses >= 3:
                self.trading_mode = TradingMode.SAFE_MODE
                return
        
        # Default to normal
        self.trading_mode = TradingMode.NORMAL
    
    def can_open_trade(self, current_confidence: int, market_quality: str = "normal") -> Tuple[bool, str]:
        """
        Check if a new trade can be opened.
        
        Args:
            current_confidence: Current signal confidence (0-100)
            market_quality: Market condition ('weak', 'normal', 'strong')
        
        Returns:
            (can_trade, reason)
        """
        now = self._now()
        stats = self._get_today_stats()
        
        # Update mode first
        self._update_trading_mode()
        
        # Check emergency stop
        if self.trading_mode == TradingMode.EMERGENCY_STOP:
            if stats.net_profit_pips <= -self.config.max_daily_loss_pips:
                return False, f"Daily loss limit reached ({self.config.max_daily_loss_pips} pips)"
            else:
                return False, f"Maximum daily trades reached ({self.config.max_daily_trades})"
        
        # Check cooldown
        if self.cooldown_until and now < self.cooldown_until:
            remaining = (self.cooldown_until - now).total_seconds() / 60
            return False, f"Cooldown active - {remaining:.0f} min remaining ({self.cooldown_reason.value})"
        
        # Get adjusted confidence threshold
        required_confidence = self.get_required_confidence_threshold(market_quality)
        
        if current_confidence < required_confidence:
            return False, f"Confidence {current_confidence} < required {required_confidence} (mode: {self.trading_mode.value})"
        
        # Check daily trade limit
        if stats.trades_count >= self.config.max_daily_trades:
            return False, f"Daily trade limit reached ({self.config.max_daily_trades})"
        
        # Check safe mode specific limits
        if self.trading_mode == TradingMode.SAFE_MODE:
            if self.safe_mode_trades_today >= self.config.safe_mode_max_trades:
                return False, f"Safe mode trade limit reached ({self.config.safe_mode_max_trades})"
        
        return True, f"✓ Trade allowed (mode: {self.trading_mode.value}, confidence: {current_confidence})"
    
    def record_trade_open(self, confidence: int) -> None:
        """Record a trade opening."""
        stats = self._get_today_stats()
        stats.trades_count += 1
        stats.last_trade_time = self._now()
        
        if self.trading_mode == TradingMode.SAFE_MODE:
            self.safe_mode_trades_today += 1
        
        self._save_state()
    
    def record_trade_close(self, profit_pips: float, confidence: int) -> None:
        """
        Record a trade closure.
        
        Args:
            profit_pips: Profit/loss in pips (positive = win, negative = loss)
            confidence: Original confidence level
        """
        stats = self._get_today_stats()
        stats.last_trade_time = self._now()
        
        is_win = profit_pips > 0
        is_loss = profit_pips < 0
        is_breakeven = profit_pips == 0
        
        # Update stats
        if is_win:
            stats.wins += 1
            stats.total_profit_pips += profit_pips
            stats.last_trade_result = 'win'
            stats.consecutive_losses = 0
        elif is_loss:
            stats.losses += 1
            stats.total_loss_pips += abs(profit_pips)
            stats.last_trade_result = 'loss'
            stats.consecutive_losses += 1
            
            # Activate cooldown after loss
            self._activate_cooldown(profit_pips, stats.consecutive_losses)
        else:
            stats.last_trade_result = 'breakeven'
            stats.consecutive_losses = 0
        
        # Log mode change
        stats.mode_history.append({
            'time': self._now().isoformat(),
            'mode': self.trading_mode.value,
            'trade_result': stats.last_trade_result,
            'consecutive_losses': stats.consecutive_losses,
            'win_rate': stats.win_rate
        })
        
        self._save_state()
    
    def _activate_cooldown(self, loss_pips: float, consecutive_losses: int) -> None:
        """Activate cooldown after a loss."""
        now = self._now()
        
        if consecutive_losses >= self.config.consecutive_loss_threshold:
            # Multiple consecutive losses - longer cooldown
            cooldown_minutes = self.config.cooldown_after_consecutive_losses_minutes
            self.cooldown_reason = CooldownReason.CONSECUTIVE_LOSSES
        else:
            # Single loss - standard cooldown
            cooldown_minutes = self.config.cooldown_after_loss_minutes
            self.cooldown_reason = CooldownReason.LOSS
        
        self.cooldown_until = now + timedelta(minutes=cooldown_minutes)
        
        print(f"[RiskManager] Cooldown activated: {cooldown_minutes} min ({self.cooldown_reason.value})")
    
    def activate_safe_mode(self, reason: str) -> None:
        """Manually activate safe mode."""
        self.trading_mode = TradingMode.SAFE_MODE
        self.safe_mode_trades_today = 0
        
        print(f"[RiskManager] Safe mode activated: {reason}")
        self._save_state()
    
    def deactivate_safe_mode(self) -> None:
        """Deactivate safe mode."""
        self.trading_mode = TradingMode.NORMAL
        self.safe_mode_trades_today = 0
        
        print(f"[RiskManager] Safe mode deactivated")
        self._save_state()
    
    def get_required_confidence_threshold(self, market_quality: str = "normal") -> int:
        """
        Get dynamic confidence threshold based on conditions.
        
        Args:
            market_quality: 'weak', 'normal', or 'strong'
        
        Returns:
            Required confidence threshold (0-100)
        """
        base_threshold = self.config.base_confidence_threshold
        
        # Mode-based adjustments
        if self.trading_mode == TradingMode.AGGRESSIVE:
            base_threshold = max(50, base_threshold - 10)
        elif self.trading_mode == TradingMode.CAUTIOUS:
            base_threshold += 10
        elif self.trading_mode == TradingMode.SAFE_MODE:
            base_threshold = self.config.safe_mode_min_confidence
        
        # Market quality adjustments
        if market_quality == "weak":
            base_threshold += self.config.weak_market_threshold_bonus
        elif market_quality == "strong":
            base_threshold = max(50, base_threshold - 5)
        
        # Recent loss penalty
        stats = self._get_today_stats()
        if stats.consecutive_losses > 0:
            base_threshold += self.config.recent_loss_threshold_bonus
        
        # Ensure reasonable bounds
        return min(100, max(50, base_threshold))
    
    def get_position_size_multiplier(self) -> float:
        """
        Get position size multiplier based on mode.
        
        Returns:
            Multiplier (1.0 = normal, 0.7 = 30% smaller, etc.)
        """
        if self.trading_mode == TradingMode.SAFE_MODE:
            return self.config.safe_mode_position_reduction
        elif self.trading_mode == TradingMode.CAUTIOUS:
            return 0.8
        elif self.trading_mode == TradingMode.AGGRESSIVE:
            return 1.2
        
        return 1.0
    
    def get_status(self) -> Dict:
        """Get current risk management status."""
        now = self._now()
        stats = self._get_today_stats()
        
        self._update_trading_mode()
        
        status = {
            'mode': self.trading_mode.value,
            'daily_trades': stats.trades_count,
            'daily_wins': stats.wins,
            'daily_losses': stats.losses,
            'win_rate': f"{stats.win_rate:.1f}%",
            'net_profit_pips': f"{stats.net_profit_pips:.1f}",
            'consecutive_losses': stats.consecutive_losses,
            'in_cooldown': self.cooldown_until and now < self.cooldown_until,
            'cooldown_reason': self.cooldown_reason.value if self.cooldown_reason else None,
        }
        
        if status['in_cooldown']:
            remaining = (self.cooldown_until - now).total_seconds() / 60
            status['cooldown_remaining_min'] = f"{remaining:.1f}"
        
        return status
    
    def daily_reset(self) -> None:
        """Reset daily trading state."""
        stats = self._get_today_stats()
        
        if stats.trades_count == 0:
            return  # Nothing to reset
        
        print(f"\n[RiskManager] Daily Summary:")
        print(f"  Trades: {stats.trades_count} | Wins: {stats.wins} | Losses: {stats.losses}")
        print(f"  Win Rate: {stats.win_rate:.1f}%")
        print(f"  Net P/L: {stats.net_profit_pips:.1f} pips")
        
        # Reset daily values but keep history
        self.cooldown_until = None
        self.cooldown_reason = None
        self.safe_mode_trades_today = 0
        self.trading_mode = TradingMode.NORMAL
        
        # Cleanup old stats
        self._cleanup_old_stats()
        
        self._save_state()


# Global instance
_risk_engine: Optional[RiskManagementEngine] = None


def get_risk_manager(config: Optional[RiskConfig] = None) -> RiskManagementEngine:
    """Get or create global risk management engine."""
    global _risk_engine
    
    if _risk_engine is None:
        _risk_engine = RiskManagementEngine(config)
    
    return _risk_engine
