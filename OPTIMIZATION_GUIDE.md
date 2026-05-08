# Performance Optimization & Risk Management Implementation Guide

## Summary of Changes

This document outlines all the optimization and risk management improvements made to the trading bot.

### Key Features Implemented

#### 1. PERFORMANCE OPTIMIZATION ✅

##### Single Indicator Computation
- **File**: `market_cache.py`
- **Change**: Indicators are now computed ONCE and cached in `processed_df`
- **Benefit**: Eliminates repeated expensive indicator calculations
- **How it works**:
  - `update_processed_df()` fetches raw 5m data and computes all indicators
  - Uses MD5 hash to detect when new data arrives
  - Reuses cached indicators if data hasn't changed
  - Returns processed_df as the single source of truth

**Usage**:
```python
from market_cache import get_processed_df, update_processed_df

# Get processed data (updates if needed)
df = get_processed_df()  # Returns cached processed_df

# Force update if needed
df = get_processed_df(force_update=True)
```

##### Candle Limits (2000-3000)
- **File**: `market_cache.py`
- **Change**: MAX_CANDLES set to 3000, MIN_CANDLES set to 2000
- **Benefit**: Maintains memory efficiency while keeping sufficient history
- Implementation:
  ```python
  MIN_CANDLES = 2000
  MAX_CANDLES = 3000
  ```
- Each data fetch automatically trims to keep between 2000-3000 recent candles

##### Trade History Compression (200-500)
- **File**: `compression_utils.py`
- **Class**: `TradeHistoryCompressor`
- **Benefit**: Keeps only 200-500 recent trades in active memory
- **Usage**:
  ```python
  from compression_utils import TradeHistoryCompressor
  
  compressed_trades = TradeHistoryCompressor.compress_trades(all_trades)
  # Keeps only recent 500 trades
  
  remaining, archived_count = TradeHistoryCompressor.archive_trades(
      trades, 
      "trade_archive.json"
  )
  ```

##### JSON Compression
- **File**: `compression_utils.py`
- **Class**: `CompressedJsonStorage`
- **Benefit**: Automatically compresses JSON files > 10KB with gzip
- **Features**:
  - Transparent compression/decompression
  - Automatic format detection
  - Handles both compressed and uncompressed files
- **Usage**:
  ```python
  from compression_utils import CompressedJsonStorage
  
  # Save (auto-compressed if > 10KB)
  CompressedJsonStorage.save("state.json", data, compress=True)
  
  # Load (auto-detects compression)
  data = CompressedJsonStorage.load("state.json")
  ```

##### Avoid Unnecessary Loops
- **File**: `bot.py`
- **Change**: Eliminated redundant indicator computations
- **Before**: 
  ```python
  df = get_data("5min")
  df = add_indicators(df)  # EXPENSIVE
  # df used multiple times with re-computed indicators
  ```
- **After**:
  ```python
  df = update_processed_df()  # Computed once, cached
  # df reused everywhere without recomputation
  ```

---

#### 2. RISK MANAGEMENT ✅

##### Core Risk Management Engine
- **File**: `risk_management.py`
- **Class**: `RiskManagementEngine`

###### Daily Trade Limits (5-8 max)
```python
from risk_management import get_risk_manager

risk_manager = get_risk_manager()

# Check before opening trade
can_trade, reason = risk_manager.can_open_trade(
    current_confidence=75,
    market_quality="normal"
)

# Record trade opening
risk_manager.record_trade_open(confidence=75)

# Record trade closure
risk_manager.record_trade_close(
    profit_pips=50,  # Positive = win, Negative = loss
    confidence=75
)
```

**Configuration**:
```python
from risk_management import RiskConfig

config = RiskConfig(
    max_daily_trades=7,  # Maximum 7 trades per day
    max_daily_loss_pips=100.0,  # Stop after 100 pips loss
)
```

###### Cooldown After Losses
- **Duration**: 
  - 15 minutes after single loss
  - 30 minutes after 2+ consecutive losses
- **Implementation**:
  ```python
  # Automatic in record_trade_close()
  risk_manager.record_trade_close(
      profit_pips=-25,  # Loss triggers cooldown
      confidence=70
  )
  # Cooldown automatically activated
  ```
- **Status Check**:
  ```python
  status = risk_manager.get_status()
  print(status['in_cooldown'])  # True/False
  print(status['cooldown_remaining_min'])  # Time left
  ```

###### Safe Mode During Weak Markets
- **Triggers**:
  - Win rate < 50% after 2+ trades
  - 2+ consecutive losses
  - Manual activation: `risk_manager.activate_safe_mode(reason)`
- **Effects in Safe Mode**:
  - Max 3 trades (vs 7 daily)
  - Requires 80+ confidence (vs 70)
  - 30% smaller position size
  - More aggressive market filtering
- **Usage**:
  ```python
  # Automatic activation
  risk_manager.activate_safe_mode("Win rate dropped below 50%")
  
  # Manual deactivation
  risk_manager.deactivate_safe_mode()
  
  # Check mode
  if risk_manager.trading_mode == TradingMode.SAFE_MODE:
      print("In safe mode!")
      position_size = base_size * risk_manager.get_position_size_multiplier()
      # position_size = base_size * 0.7  (30% reduction)
  ```

###### Dynamic Threshold Adjustment
- **Base Confidence Threshold**: 70%
- **Adjustments**:
  - Weak market: +10% confidence needed
  - Recent losses: +5% confidence needed
  - Safe mode: Requires 80% minimum
  - Aggressive mode: -10% (if triggered)
- **Usage**:
  ```python
  # Get dynamic threshold
  required = risk_manager.get_required_confidence_threshold(
      market_quality="weak"  # weak, normal, strong
  )
  
  if current_confidence < required:
      print(f"Need {required}% confidence, have {current_confidence}%")
  ```

###### Daily Loss Limit
- **Configuration**: `max_daily_loss_pips = 100.0`
- **Behavior**: 
  - Stops all trading when daily loss reaches limit
  - Mode switches to `EMERGENCY_STOP`
  - No trades allowed until next day
- **Monitoring**:
  ```python
  status = risk_manager.get_status()
  print(f"Net P/L: {status['net_profit_pips']} pips")
  
  if status['net_profit_pips'] < -100:
      # Emergency stop automatically activated
      print("Daily loss limit reached!")
  ```

---

### Integration with Bot

The main bot (`bot.py`) has been updated to:

1. **Initialize Risk Manager** at startup
   ```python
   risk_manager = get_risk_manager()
   ```

2. **Reset daily at market open**
   ```python
   risk_manager.daily_reset()
   ```

3. **Check risk before trades**
   ```python
   can_trade, reason = risk_manager.can_open_trade(confidence, market_quality)
   if not can_trade:
       print(f"Trade blocked: {reason}")
       continue
   ```

4. **Record trades**
   ```python
   risk_manager.record_trade_open(confidence)
   # Later...
   risk_manager.record_trade_close(profit_pips, confidence)
   ```

5. **Print status every cycle**
   ```python
   risk_status = risk_manager.get_status()
   print(f"[Risk Status] {risk_status['daily_trades']}/{risk_manager.config.max_daily_trades} trades | "
         f"W/L: {risk_status['daily_wins']}/{risk_status['daily_losses']} | "
         f"Mode: {risk_status['mode']}")
   ```

---

## Testing & Validation

### 1. Performance Tests

#### Test 1: Indicator Computation
```python
import time
from market_cache import get_processed_df, update_processed_df

# First call - should compute
start = time.time()
df = update_processed_df()
first_time = time.time() - start
print(f"First computation: {first_time:.3f}s")

# Second call - should use cache
start = time.time()
df = get_processed_df()
cache_time = time.time() - start
print(f"Cached access: {cache_time:.3f}s")
print(f"Speedup: {first_time/cache_time:.1f}x")
```

**Expected**: Cache access should be 10-100x faster

#### Test 2: Candle Limits
```python
from market_cache import get_processed_df, get_cache_stats

df = get_processed_df()
stats = get_cache_stats()

assert 2000 <= stats['processed_df_candles'] <= 3000, \
    f"Candles {stats['processed_df_candles']} out of range!"
print(f"✓ Candles within limits: {stats['processed_df_candles']}")
```

#### Test 3: Trade History Compression
```python
from compression_utils import TradeHistoryCompressor

trades = [{'id': i, 'profit': 10} for i in range(1000)]
compressed = TradeHistoryCompressor.compress_trades(trades)

assert len(compressed) <= 500, f"Too many trades: {len(compressed)}"
print(f"✓ Compressed from 1000 to {len(compressed)} trades")
```

### 2. Risk Management Tests

#### Test 4: Daily Trade Limit
```python
from risk_management import get_risk_manager, RiskConfig

config = RiskConfig(max_daily_trades=3)  # Low limit for testing
rm = get_risk_manager(config)

# Open 3 trades
for i in range(3):
    can_trade, reason = rm.can_open_trade(80, "normal")
    assert can_trade, f"Trade {i} should be allowed"
    rm.record_trade_open(80)
    print(f"✓ Trade {i+1} opened")

# 4th trade should be blocked
can_trade, reason = rm.can_open_trade(80, "normal")
assert not can_trade, "4th trade should be blocked"
assert "trade limit" in reason.lower()
print(f"✓ 4th trade blocked: {reason}")
```

#### Test 5: Cooldown After Loss
```python
from risk_management import get_risk_manager

rm = get_risk_manager()

# Open and lose trade
rm.record_trade_open(70)
rm.record_trade_close(-25, 70)  # Loss

# Check cooldown
status = rm.get_status()
assert status['in_cooldown'], "Should be in cooldown"
assert status['cooldown_reason'] == 'loss'
print(f"✓ Cooldown active for {status['cooldown_remaining_min']:.1f} min")

# Try to open trade
can_trade, reason = rm.can_open_trade(80, "normal")
assert not can_trade, "Should be blocked by cooldown"
assert "cooldown" in reason.lower()
print(f"✓ Trade blocked: {reason}")
```

#### Test 6: Safe Mode Activation
```python
from risk_management import get_risk_manager, TradingMode

rm = get_risk_manager()

# Simulate 2 consecutive losses
rm.record_trade_open(70)
rm.record_trade_close(-20, 70)
rm.record_trade_open(70)
rm.record_trade_close(-15, 70)

# Check mode
status = rm.get_status()
if status['consecutive_losses'] >= 2:
    rm.activate_safe_mode("Consecutive losses")
    assert rm.trading_mode == TradingMode.SAFE_MODE
    print("✓ Safe mode activated")
    
    # Check constraints
    required = rm.get_required_confidence_threshold("normal")
    assert required >= 80, f"Should require 80%+, got {required}%"
    print(f"✓ Safe mode requires {required}% confidence")
    
    pos_mult = rm.get_position_size_multiplier()
    assert pos_mult == 0.7, f"Should reduce position by 30%, got {pos_mult}"
    print(f"✓ Position size multiplied by {pos_mult:.1%}")
```

#### Test 7: Dynamic Thresholds
```python
from risk_management import get_risk_manager

rm = get_risk_manager()

# Normal market
normal = rm.get_required_confidence_threshold("normal")
print(f"Normal market requires: {normal}%")

# Weak market
weak = rm.get_required_confidence_threshold("weak")
print(f"Weak market requires: {weak}%")
assert weak > normal, "Weak market should require higher confidence"
print(f"✓ Weak market penalty: +{weak-normal}%")

# Strong market
strong = rm.get_required_confidence_threshold("strong")
assert strong < normal, "Strong market should require lower confidence"
print(f"✓ Strong market bonus: -{normal-strong}%")
```

#### Test 8: Daily Loss Limit
```python
from risk_management import get_risk_manager, RiskConfig, TradingMode

config = RiskConfig(max_daily_loss_pips=50.0)  # Low for testing
rm = get_risk_manager(config)

# Accumulate losses
rm.record_trade_open(70)
rm.record_trade_close(-30, 70)

rm.record_trade_open(70)
rm.record_trade_close(-25, 70)  # Total -55, exceeds limit

status = rm.get_status()
assert float(status['net_profit_pips']) <= -50
print(f"✓ Daily loss accumulated: {status['net_profit_pips']} pips")

# Should be in emergency stop
can_trade, reason = rm.can_open_trade(100, "normal")
assert not can_trade, "Should be blocked"
assert "loss limit" in reason.lower() or "Emergency" in reason
assert rm.trading_mode == TradingMode.EMERGENCY_STOP
print(f"✓ Emergency stop activated: {reason}")
```

### 3. Integration Tests

#### Test 9: Complete Workflow
```python
from market_cache import get_processed_df, update_processed_df
from risk_management import get_risk_manager

# 1. Get processed data
df = get_processed_df()
assert df is not None and len(df) > 0
print("✓ Got processed dataframe")

# 2. Calculate confidence (example)
confidence = 75

# 3. Check risk manager
rm = get_risk_manager()
can_trade, reason = rm.can_open_trade(confidence, "normal")
print(f"✓ Risk check: {reason}")

# 4. If allowed, record trade
if can_trade:
    rm.record_trade_open(confidence)
    print("✓ Trade recorded as open")
    
    # Simulate trade result
    profit_pips = 30  # Win
    rm.record_trade_close(profit_pips, confidence)
    
    status = rm.get_status()
    print(f"✓ Trade closed: {status['daily_wins']} wins, {status['daily_losses']} losses")
```

### 4. Stress Tests

#### Test 10: High Volume Trades
```python
from risk_management import get_risk_manager, RiskConfig
import time

config = RiskConfig(max_daily_trades=100)  # High limit
rm = get_risk_manager(config)

start = time.time()
for i in range(100):
    rm.record_trade_open(70)
    rm.record_trade_close(10 if i % 2 == 0 else -10, 70)

elapsed = time.time() - start
print(f"✓ Processed 100 trades in {elapsed:.3f}s ({100/elapsed:.0f} trades/sec)")

status = rm.get_status()
print(f"✓ Final state: {status['daily_trades']} trades, {status['daily_wins']} wins")
```

---

## Performance Metrics

### Expected Improvements

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Indicator computation per cycle | 50-100ms | 1-5ms (cached) | 10-100x |
| Memory usage (candles) | 3000+ | 2000-3000 | ~33% reduction |
| State file size | 100KB+ | 10-20KB (compressed) | 5-10x |
| Loop iteration time | 500ms | <100ms | 5x |
| API calls per minute | 6 | Same | (Rate limited) |

### Monitoring

**Check Performance**:
```python
from market_cache import get_cache_stats
from compression_utils import StorageOptimizer

# Cache stats
stats = get_cache_stats()
print(f"Cached candles: {stats['processed_df_candles']}")
print(f"API calls this minute: {stats['api_calls_in_minute']}")

# Storage stats
report = StorageOptimizer.get_storage_report()
print(f"Total storage: {report['total_size_mb']} MB")
for file, info in report['files'].items():
    print(f"  {file}: {info['size_kb']} KB (compressed: {info['compressed']})")
```

---

## Configuration Tuning

### Risk Management Parameters

Edit these in your code or create a config file:

```python
from risk_management import RiskConfig, get_risk_manager

# Aggressive trading
aggressive_config = RiskConfig(
    max_daily_trades=10,
    max_daily_loss_pips=150,
    cooldown_after_loss_minutes=10,
    safe_mode_min_confidence=75,
)

# Conservative trading
conservative_config = RiskConfig(
    max_daily_trades=5,
    max_daily_loss_pips=50,
    cooldown_after_loss_minutes=30,
    safe_mode_min_confidence=85,
)

rm = get_risk_manager(conservative_config)
```

---

## Troubleshooting

### Issue: "Confidence X < required Y"
- **Cause**: Market quality is poor or recent losses increased threshold
- **Solution**: Wait for better market conditions or more time after losses

### Issue: "Cooldown active"
- **Cause**: Recent loss triggered cooldown
- **Solution**: Wait cooldown period or check market conditions

### Issue: "Safe mode trade limit reached"
- **Cause**: Too many trades in safe mode
- **Solution**: Wait until safe mode exits (when win rate improves)

### Issue: "Emergency stop"
- **Cause**: Daily loss limit reached
- **Solution**: Wait until next trading day for reset

---

## Next Steps

1. **Monitor Daily**: Check risk status in logs
2. **Tune Thresholds**: Adjust based on your trading patterns
3. **Archive Trades**: Periodically archive old trades using `TradeHistoryCompressor`
4. **Review Results**: Use `get_status()` to track daily performance
5. **Optimize**: Adjust `max_daily_trades` and `max_daily_loss_pips` based on results

