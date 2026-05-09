from logger import logger
import os
import json
from datetime import datetime, timedelta

class LearningEngine:
    def __init__(self, memory_file="trade_memory.json"):
        self.memory_file = memory_file
        self.memory = self._load_memory()
        self.trades_since_cleanup = 0
        self._cleanup_old_memory()
        
    def _load_memory(self):
        if os.path.exists(self.memory_file):
            try:
                with open(self.memory_file, "r") as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading learning memory: {e}")
                return []
        return []

    def _save_memory(self):
        try:
            with open(self.memory_file, "w") as f:
                json.dump(self.memory, f, indent=4)
        except Exception as e:
            logger.error(f"Error saving learning memory: {e}")

    def _cleanup_old_memory(self):
        """Keep ONLY last 14 days memory and cap at 500 entries. Auto remove older data."""
        cutoff_date = datetime.now() - timedelta(days=14)
        original_length = len(self.memory)
        
        # 1. Filter by date
        valid_memory = []
        for trade in self.memory:
            try:
                ts_str = trade.get("timestamp", datetime.now().isoformat())
                trade_date = datetime.fromisoformat(ts_str)
                if trade_date >= cutoff_date:
                    valid_memory.append(trade)
            except ValueError:
                pass
                
        # 2. Enforce hard cap of 500 (keep latest)
        if len(valid_memory) > 500:
            valid_memory = valid_memory[-500:]
            
        self.memory = valid_memory
        
        if len(self.memory) < original_length:
            self._save_memory()

    def record_trade(self, time_of_day: str, direction: str, confidence: int, atr: float, rsi: float, result: str, source: str = "telegram"):
        """
        Record winning/losing timings and conditions.
        result should be 'WIN' or 'LOSS'
        """
        trade_data = {
            "timestamp": datetime.now().isoformat(),
            "time_of_day": time_of_day,
            "direction": direction,
            "confidence": confidence,
            "atr": atr,
            "rsi": rsi,
            "result": result.upper(),
            "source": source,
        }
        self.memory.append(trade_data)
        self._save_memory()
        
        self.trades_since_cleanup += 1
        if self.trades_since_cleanup >= 20:
            self._cleanup_old_memory()
            self.trades_since_cleanup = 0

    def get_adaptive_adjustment(self, time_of_day: str, direction: str, confidence: int, atr: float, rsi: float, source: str = "telegram") -> int:
        """
        Analyze past trades and return a confidence adjustment.
        Boosts strong timings slightly. Reduces weak timings gradually.
        Source-aware: timings filtered by origin when history is sufficient.
        """
        if not self.memory:
            return 0
            
        adjustment = 0
        
        # 1. Analyze Timings (winning vs losing timings)
        # Prefer source-filtered trades if there are enough; fall back to all
        source_trades = [t for t in self.memory if t.get("source", "telegram") == source]
        timing_pool = source_trades if len(source_trades) >= 5 else self.memory
        timing_trades = [t for t in timing_pool if t["time_of_day"] == time_of_day and t["direction"] == direction]
        if len(timing_trades) >= 2:
            wins = sum(1 for t in timing_trades if t["result"] == "WIN")
            win_rate = wins / len(timing_trades)
            
            if win_rate >= 0.70:
                adjustment += 3  # Boost strong timings
            elif win_rate >= 0.51:
                adjustment += 1
            elif win_rate <= 0.30:
                adjustment -= 3  # Reduce weak timings
            elif win_rate <= 0.50:
                adjustment -= 1

        # 2. Analyze RSI / ATR behavior patterns
        # Look for historical trades with similar volatility and momentum
        atr_threshold = max(atr * 0.2, 0.0001)
        similar_conditions = [
            t for t in self.memory 
            if t["direction"] == direction 
            and abs(t["rsi"] - rsi) <= 5 
            and abs(t["atr"] - atr) <= atr_threshold
        ]
        
        if len(similar_conditions) >= 3:
            cond_wins = sum(1 for t in similar_conditions if t["result"] == "WIN")
            cond_win_rate = cond_wins / len(similar_conditions)
            
            if cond_win_rate >= 0.65:
                adjustment += 2
            elif cond_win_rate <= 0.40:
                adjustment -= 2

        # 3. Analyze best confidence ranges
        # Determine if this level of confidence historically succeeds or fails
        conf_range = [
            t for t in self.memory
            if abs(t["confidence"] - confidence) <= 3
        ]
        if len(conf_range) >= 3:
            conf_wins = sum(1 for t in conf_range if t["result"] == "WIN")
            conf_win_rate = conf_wins / len(conf_range)
            
            if conf_win_rate >= 0.75:
                adjustment += 1
            elif conf_win_rate <= 0.35:
                adjustment -= 2

        # Cap the total adjustment to prevent excessive overriding of base logic
        return max(min(adjustment, 6), -6)

# Global singleton instance for easy access
learning_engine = LearningEngine()
