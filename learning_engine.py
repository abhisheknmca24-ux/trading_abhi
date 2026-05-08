import json
import os
import time
from datetime import datetime, timedelta
import pandas as pd

MEMORY_FILE = "learning_memory.json"
LEARNING_WINDOW_DAYS = 14
MAX_MEMORY_TRADES = 200  # Hard cap on stored trades to save Railway disk space

# In-memory cache to reduce disk IO
_memory_cache = None
_cache_loaded_time = 0
_CACHE_MAX_AGE_SECONDS = 300  # Cache lasts 5 minutes

# Cleanup scheduler
_last_cleanup_time = 0
_last_save_time = 0
CLEANUP_INTERVAL_SECONDS = 3600  # Cleanup every hour
SAVE_DEBOUNCE_SECONDS = 60  # Save at most once per minute

def _load_memory(force_reload: bool = False) -> List:
    """Load memory with in-memory caching to reduce disk IO."""
    global _memory_cache, _cache_loaded_time
    import time
    
    now = time.time()
    
    # Return cached if still valid
    if not force_reload and _memory_cache is not None:
        if now - _cache_loaded_time < _CACHE_MAX_AGE_SECONDS:
            return _memory_cache.copy()
    
    # Load from disk
    if not os.path.exists(MEMORY_FILE):
        _memory_cache = []
        _cache_loaded_time = now
        return []
    try:
        with open(MEMORY_FILE, "r") as f:
            _memory_cache = json.load(f)
        _cache_loaded_time = now
        return _memory_cache.copy()
    except Exception as e:
        print(f"Error loading memory: {e}")
        _memory_cache = []
        _cache_loaded_time = now
        return []

def _save_memory(memory, force: bool = False) -> None:
    """Save memory with debouncing to reduce disk IO."""
    import time
    global _last_save_time
    
    now = time.time()
    
    # Debounce: only save if forced or enough time passed
    if not force and (now - _last_save_time) < SAVE_DEBOUNCE_SECONDS:
        return
    
    try:
        with open(MEMORY_FILE, "w") as f:
            json.dump(memory, f, indent=2)
        _last_save_time = now
    except Exception as e:
        print(f"Error saving memory: {e}")

def _cleanup_old_data(memory):
    """Remove data older than LEARNING_WINDOW_DAYS, and cap at MAX_MEMORY_TRADES."""
    cutoff_time = datetime.now() - timedelta(days=LEARNING_WINDOW_DAYS)
    recent = [m for m in memory if datetime.fromisoformat(m["time"]) >= cutoff_time]
    # Enforce hard cap — keep most recent trades
    if len(recent) > MAX_MEMORY_TRADES:
        recent = sorted(recent, key=lambda x: x["time"], reverse=True)[:MAX_MEMORY_TRADES]
    return recent


def prune_old_data() -> int:
    """Public function: prune memory file to 14-day window and 200-trade cap. Returns count removed."""
    memory = _load_memory()
    before = len(memory)
    pruned = _cleanup_old_data(memory)
    removed = before - len(pruned)
    if removed > 0:
        _save_memory(pruned)
        print(f"  ✂ LearningEngine: pruned {removed} old trade records ({len(pruned)} kept)")
    return removed

def record_trade_result(trade_data: dict):
    """
    Stores trade data to learning_memory.json.
    Uses in-memory caching with debounced saves.
    """
    global _memory_cache
    
    memory = _load_memory()
    memory = _cleanup_old_data(memory)
    
    # Store relevant data
    entry = {
        "time": trade_data.get("time", datetime.now().isoformat()),
        "direction": trade_data.get("direction"),
        "pair": trade_data.get("pair", "EURUSD"),
        "result": trade_data.get("result"),
        "confidence": trade_data.get("confidence", 0),
        "source": trade_data.get("source", "unknown"),
        "rsi": trade_data.get("rsi", 50),
        "atr": trade_data.get("atr", 0.0),
    }
    
    memory.append(entry)
    
    # Update in-memory cache
    _memory_cache = memory
    
    # Apply cap every time we save
    if len(memory) > MAX_MEMORY_TRADES:
        memory = sorted(memory, key=lambda x: x["time"], reverse=True)[:MAX_MEMORY_TRADES]
        _memory_cache = memory
    
    # Debounced save
    _save_memory(memory, force=False)

def _get_weighted_score(trades):
    """
    Weight recent trades more heavily.
    Trades from today = weight 1.0. 14 days ago = weight 0.1.
    """
    if not trades:
        return 0, 0
    
    now = datetime.now()
    weighted_wins = 0.0
    weighted_total = 0.0
    
    for t in trades:
        trade_time = datetime.fromisoformat(t["time"])
        days_old = (now - trade_time).days
        # Weight formula: recent is 1.0, drops to 0.1 at 14 days
        weight = max(0.1, 1.0 - (days_old / LEARNING_WINDOW_DAYS) * 0.9)
        
        weighted_total += weight
        if t["result"] == "WIN":
            weighted_wins += weight
            
    return weighted_wins, weighted_total

def should_block(signal_time: datetime, direction: str) -> bool:
    """
    DEPRECATED: Use get_confidence_penalty instead.
    Returns True only for extremely poor historical performance.
    """
    penalty = get_confidence_penalty(signal_time, direction)
    return penalty >= 25  # Only hard block for extreme penalty


def get_confidence_penalty(signal_time: datetime, direction: str) -> int:
    """
    Get confidence penalty (0-12) based on historical performance.
    Uses SOFTER penalties instead of hard blocking.
    """
    memory = _load_memory()
    memory = _cleanup_old_data(memory)
    
    signal_hm = signal_time.strftime("%H:%M")
    matching_trades = [
        t for t in memory 
        if datetime.fromisoformat(t["time"]).strftime("%H:%M") == signal_hm and t["direction"] == direction
    ]
    
    # Need at least 3 trades to calculate penalty
    if len(matching_trades) < 3:
        return 0
    
    wins, total = _get_weighted_score(matching_trades)
    win_rate = wins / total if total > 0 else 0.5
    
    # Convert win rate to SOFTER penalty (0-12)
    # Win rate >= 55% = 0 penalty
    # Win rate < 30% = max 12 penalty
    if win_rate >= 0.55:
        return 0
    elif win_rate >= 0.45:
        return 3   # was 5
    elif win_rate >= 0.35:
        return 7   # was 15
    else:
        return 12  # was 25 - softer cap

def get_signal_source_ranking() -> dict:
    """
    Rank signal sources automatically based on weighted performance.
    """
    memory = _load_memory()
    memory = _cleanup_old_data(memory)
    
    sources = {}
    for t in memory:
        s = t["source"]
        if s not in sources:
            sources[s] = []
        sources[s].append(t)
        
    ranking = {}
    for source, trades in sources.items():
        wins, total = _get_weighted_score(trades)
        ranking[source] = {
            "win_rate": (wins / total * 100) if total > 0 else 0,
            "total_trades": len(trades)
        }
    
    return ranking

def adapt_confidence_threshold(base_threshold: int) -> int:
    """
    Adapt confidence thresholds dynamically based on recent overall performance.
    If the bot is losing recently, increase the threshold to be safer.
    If winning consistently, allow slightly riskier trades.
    """
    memory = _load_memory()
    memory = _cleanup_old_data(memory)
    
    if len(memory) < 10:
        return base_threshold
        
    wins, total = _get_weighted_score(memory)
    recent_win_rate = (wins / total * 100) if total > 0 else 0
    
    if recent_win_rate < 45:
        return base_threshold + 10  # Be much stricter
    elif recent_win_rate < 55:
        return base_threshold + 5   # Be slightly stricter
    elif recent_win_rate > 75:
        return base_threshold - 5   # Allow more trades
        
    return base_threshold


def scheduled_cleanup() -> None:
    """
    Periodic cleanup - runs automatically to prevent memory growth.
    Call this periodically (e.g., every hour) to clean up old data.
    """
    global _last_cleanup_time
    now = time.time()
    
    if now - _last_cleanup_time < CLEANUP_INTERVAL_SECONDS:
        return
    
    _last_cleanup_time = now
    
    # Prune old data from memory
    removed = prune_old_data()
    if removed > 0:
        print(f"  ✂ Learning engine: scheduled cleanup removed {removed} old records")
