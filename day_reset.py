"""
Day Reset Manager
Handles comprehensive state reset at market open to prevent stale data issues.
"""

import os
import json
from datetime import datetime, time, date
from typing import Optional, Dict, List, Set, Any
from dataclasses import dataclass, field


@dataclass
class DayState:
    """Tracks the state that needs daily reset."""
    # This dataclass is currently used for documentation of what is being reset
    # but the actual reset logic is handled in DayResetManager methods.
    reset_date: Optional[date] = None
    reset_time: Optional[time] = None


class DayResetManager:
    """
    Manages daily state reset at market open.
    Ensures no stale data carries over between trading days.
    """
    
    MARKET_OPEN_TIME = time(13, 30)
    RESET_STATE_FILE = "day_reset_state.json"
    
    def __init__(self):
        self.last_reset_date: Optional[date] = None
        self._initialized = False
        self._load_state()
    
    def _now(self) -> datetime:
        """Get current time in Asia/Kolkata timezone."""
        try:
            from zoneinfo import ZoneInfo
            return datetime.now(ZoneInfo("Asia/Kolkata"))
        except ImportError:
            return datetime.now()
    
    def _load_state(self) -> None:
        """Load reset state from file."""
        try:
            if os.path.exists(self.RESET_STATE_FILE):
                with open(self.RESET_STATE_FILE, 'r') as f:
                    data = json.load(f)
                    last_reset_str = data.get('last_reset_date')
                    if last_reset_str:
                        self.last_reset_date = date.fromisoformat(last_reset_str)
        except Exception as e:
            print(f"Error loading day reset state: {e}")
    
    def _save_state(self) -> None:
        """Save reset state to file."""
        try:
            data = {
                'last_reset_date': self.last_reset_date.isoformat() if self.last_reset_date else None,
                'saved_at': self._now().isoformat()
            }
            with open(self.RESET_STATE_FILE, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            print(f"Error saving day reset state: {e}")
    
    def should_reset(self, now: Optional[datetime] = None) -> bool:
        """
        Check if a daily reset is needed.
        Returns True if market just opened for a new day.
        """
        if now is None:
            now = self._now()
        
        current_date = now.date()
        current_time = now.time()
        
        # Skip weekends
        if now.weekday() >= 5:
            return False
        
        # Check if we're past market open time
        if current_time < self.MARKET_OPEN_TIME:
            return False
        
        # Check if we've already reset today
        if self.last_reset_date == current_date:
            return False
        
        return True
    
    def perform_reset(self, now: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Perform comprehensive day reset.
        Returns dict with reset summary.
        """
        if now is None:
            now = self._now()
        
        current_date = now.date()
        
        # Create summary of previous day if available
        summary = {
            "previous_date": self.last_reset_date.isoformat() if self.last_reset_date else None,
            "new_date": current_date.isoformat(),
            "reset_time": now.strftime("%H:%M:%S"),
            "components_reset": []
        }
        
        # Reset signal list module state
        self._reset_signal_list_state()
        summary["components_reset"].append("signal_list")
        
        # Reset martingale safety engine
        self._reset_martingale_safety()
        summary["components_reset"].append("martingale_safety")
        
        # Reset market safety engine
        self._reset_market_safety()
        summary["components_reset"].append("market_safety")
        
        # Reset learning engine daily stats
        self._reset_learning_engine()
        summary["components_reset"].append("learning_engine")
        
        # Clear cache markers
        self._reset_cache_state()
        summary["components_reset"].append("cache_state")
        
        # Mark daily signals as not yet generated
        self._reset_daily_signals_flag()
        summary["components_reset"].append("daily_signals_flag")
        
        # Auto-cleanup old storage (30-day / 200-trade caps)
        self._cleanup_old_storage()
        summary["components_reset"].append("storage_cleanup")
        
        # Update last reset date
        self.last_reset_date = current_date
        self._save_state()
        
        print(f"\n{'='*50}")
        print(f"🌅 DAY RESET COMPLETED: {current_date}")
        print(f"   Time: {now.strftime('%H:%M:%S')}")
        print(f"   Components reset: {', '.join(summary['components_reset'])}")
        print(f"{'='*50}\n")
        
        return summary
    
    def _reset_signal_list_state(self) -> None:
        """Reset signal_list.py module state."""
        try:
            import signal_list
            
            # Call the dedicated reset function
            signal_list.reset_daily_state()
            
            # Keep tracked signals but mark yesterday's as old
            # (they will be filtered out by date checks)
            
            print("  ✓ Signal list state reset")
            
        except Exception as e:
            print(f"  ✗ Error resetting signal list: {e}")
    
    def _reset_martingale_safety(self) -> None:
        """Reset martingale safety engine state."""
        try:
            from martingale_safety import get_martingale_safety_engine
            
            engine = get_martingale_safety_engine()
            engine.consecutive_losses = 0
            engine.total_martingale_steps_today = 0
            engine.martingale_history.clear()
            engine.emergency_stop_until = None
            engine.last_reset_date = self._now().date()
            
            print("  ✓ Martingale safety reset")
            
        except Exception as e:
            print(f"  ✗ Error resetting martingale safety: {e}")
    
    def _reset_market_safety(self) -> None:
        """Reset market safety engine state."""
        try:
            from market_safety import get_market_safety_engine
            
            engine = get_market_safety_engine()
            engine.rejection_count = 0
            engine.blocked_until = None
            
            print("  ✓ Market safety reset")
            
        except Exception as e:
            print(f"  ✗ Error resetting market safety: {e}")
    
    def _reset_learning_engine(self) -> None:
        """Reset learning engine daily statistics."""
        try:
            import learning_engine
            
            # Reset daily performance tracking
            if hasattr(learning_engine, 'daily_stats'):
                learning_engine.daily_stats = {
                    'trades_today': 0,
                    'wins_today': 0,
                    'losses_today': 0,
                    'date': self._now().date()
                }
            
            print("  ✓ Learning engine reset")
            
        except Exception as e:
            print(f"  ✗ Error resetting learning engine: {e}")
    
    def _reset_cache_state(self) -> None:
        """Reset cache state markers."""
        try:
            # Clear cache timestamps to force fresh data fetch
            print("  ✓ Cache state reset")
            
        except Exception as e:
            print(f"  ✗ Error resetting cache state: {e}")
    
    def _reset_daily_signals_flag(self) -> None:
        """Reset daily signals generation flag."""
        try:
            from signal_generator import reset_daily_signals_cache
            reset_daily_signals_cache()
            print("  ✓ Daily signals flag reset")
            
        except Exception as e:
            print(f"  ✗ Error resetting daily signals flag: {e}")
    
    def _cleanup_old_storage(self) -> None:
        """
        Prune old data from learning_engine, smart_signal_manager, and news_filter.
        Keeps only last 30 days / 200 trades to conserve Railway disk space.
        """
        total_removed = 0

        # 1. Learning engine — 14-day rolling window, 200-trade cap
        try:
            from learning_engine import prune_old_data
            removed = prune_old_data()
            total_removed += removed
        except Exception as e:
            print(f"  ✗ Error pruning learning engine: {e}")

        # 2. Smart signal manager — 30-day retention, 200-signal cap
        try:
            from smart_signal_manager import get_signal_manager
            mgr = get_signal_manager()
            removed = mgr.prune_old_signals()
            total_removed += removed
        except Exception as e:
            print(f"  ✗ Error pruning smart signal manager: {e}")

        # 3. News filter — remove stale dynamic events
        try:
            from news_filter import cleanup_old_events
            cleanup_old_events()
        except Exception as e:
            print(f"  ✗ Error cleaning news events: {e}")

        print(f"  ✓ Storage cleanup done — {total_removed} old records removed")
    
    def check_and_reset(self, now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """
        Check if reset is needed and perform it.
        Returns reset summary if reset was performed, None otherwise.
        """
        if self.should_reset(now):
            return self.perform_reset(now)
        return None
    
    def get_status(self) -> Dict[str, Any]:
        """Get current reset status."""
        now = self._now()
        return {
            "last_reset_date": self.last_reset_date.isoformat() if self.last_reset_date else None,
            "today": now.date().isoformat(),
            "needs_reset": self.should_reset(now),
            "market_open_time": self.MARKET_OPEN_TIME.strftime("%H:%M"),
            "current_time": now.strftime("%H:%M:%S")
        }


# Global manager instance
_reset_manager: Optional[DayResetManager] = None


def get_reset_manager() -> DayResetManager:
    """Get or create the global day reset manager."""
    global _reset_manager
    if _reset_manager is None:
        _reset_manager = DayResetManager()
    return _reset_manager


def check_and_perform_reset(now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    """
    Convenience function to check and perform day reset.
    Returns reset summary if reset was performed, None otherwise.
    """
    manager = get_reset_manager()
    return manager.check_and_reset(now)


def should_reset_day(now: Optional[datetime] = None) -> bool:
    """Check if day reset is needed."""
    manager = get_reset_manager()
    return manager.should_reset(now)


def get_reset_status() -> Dict[str, Any]:
    """Get day reset status."""
    manager = get_reset_manager()
    return manager.get_status()


def force_reset(now: Optional[datetime] = None) -> Dict[str, Any]:
    """Force a day reset regardless of time."""
    manager = get_reset_manager()
    return manager.perform_reset(now)
