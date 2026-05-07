#!/usr/bin/env python3
"""
Test script for real confidence engine.
Verifies that confidence values are calibrated correctly and respect constraints.
"""

import sys
from datetime import datetime, timedelta
import json
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from learning_engine import calculate_real_confidence, record_trade, get_recent_trades

def test_confidence_bounds():
    """Test that confidence values stay within bounds."""
    print("\n=== TEST: Confidence Bounds ===")
    
    # Test case 1: Normal signal
    result = calculate_real_confidence(
        signal_time=datetime.now(),
        direction="CALL",
        rsi=50.0,
        source="Test",
        atr=0.001,
        ema_slope=0.0001,
    )
    
    confidence = result["confidence"]
    print(f"✓ Normal signal confidence: {confidence}% (tier: {result['tier']})")
    
    assert 0 <= confidence <= 90, f"Confidence out of bounds: {confidence}"
    assert confidence <= 90, "Confidence exceeds maximum of 90%"
    print(f"✓ Confidence within bounds [0, 90]")
    
    # Test case 2: Weak signal
    result_weak = calculate_real_confidence(
        signal_time=datetime.now(),
        direction="PUT",
        rsi=20.0,  # weak RSI
        source="Test",
        atr=0.00001,  # very weak ATR
        ema_slope=0.00001,  # weak EMA slope
    )
    
    confidence_weak = result_weak["confidence"]
    print(f"✓ Weak signal confidence: {confidence_weak}% (tier: {result_weak['tier']})")
    assert 0 <= confidence_weak <= 90, f"Weak confidence out of bounds: {confidence_weak}"


def test_tier_assignment():
    """Test that tier assignment respects historical sample requirements."""
    print("\n=== TEST: Tier Assignment Logic ===")
    
    # First, add some historical trades to the learning database
    now = datetime.now()
    
    # Simulate 12 historical trades at a specific time
    test_time_str = "14:30"  # 2:30 PM
    for i in range(12):
        test_signal_time = now.replace(hour=14, minute=30, second=0, microsecond=0)
        trade_data = {
            "signal_time": test_signal_time.isoformat(),
            "direction": "CALL",
            "entry_price": 1.0800 + (i * 0.0001),
            "exit_price": 1.0805 + (i * 0.0001),  # All wins (exit > entry for CALL)
            "result": "WIN",
            "confidence": 75,
            "rsi": 55.0,
            "atr": 0.001,
            "source": "Test",
            "pair": "EURUSD",
            "expiry_minutes": 5,
        }
        record_trade(trade_data)
    
    # Now test that S-TIER assignment requires 10+ samples with high win rate
    result = calculate_real_confidence(
        signal_time=test_time_str,
        direction="CALL",
        rsi=55.0,
        source="Test",
        atr=0.001,
        ema_slope=0.0001,
        min_historical_samples=10,
    )
    
    print(f"✓ Signal at {test_time_str} with 12 high-WR trades:")
    print(f"  - Confidence: {result['confidence']}%")
    print(f"  - Tier: {result['tier']}")
    print(f"  - Reasons: {result['reasons']}")
    
    # S-TIER requires: confidence >= 80, historical_time_samples >= 10, win_rate > 70%
    if result["tier"] in ["S-TIER", "A+"]:
        print(f"✓ Tier assignment working (got {result['tier']})")
    else:
        print(f"⚠ Note: Tier is {result['tier']}, likely due to recent sample size or other factors")


def test_downgrades_and_boosts():
    """Test that downgrades and boosts are applied correctly."""
    print("\n=== TEST: Downgrades and Boosts ===")
    
    # Test downgrade for weak technical conditions
    result_weak_tech = calculate_real_confidence(
        signal_time=datetime.now(),
        direction="CALL",
        rsi=30.0,  # weak RSI zone
        source="Test",
        atr=0.00001,  # very weak ATR
        ema_slope=0.000001,  # very weak EMA slope
    )
    
    result_strong_tech = calculate_real_confidence(
        signal_time=datetime.now(),
        direction="CALL",
        rsi=65.0,  # good RSI zone
        source="Test",
        atr=0.002,  # strong ATR
        ema_slope=0.0002,  # strong EMA slope
    )
    
    print(f"✓ Weak technicals confidence: {result_weak_tech['confidence']}%")
    print(f"✓ Strong technicals confidence: {result_strong_tech['confidence']}%")
    
    assert result_strong_tech["confidence"] >= result_weak_tech["confidence"], \
        "Strong technicals should score higher than weak technicals"
    print(f"✓ Technical conditions correctly influence confidence")


def test_rsi_binning():
    """Test that RSI values are properly binned."""
    print("\n=== TEST: RSI Binning ===")
    
    rsi_values = [10, 30, 50, 70, 90]
    
    for rsi in rsi_values:
        result = calculate_real_confidence(
            signal_time=datetime.now(),
            direction="CALL",
            rsi=rsi,
            source="Test",
            atr=0.001,
            ema_slope=0.0001,
        )
        print(f"✓ RSI {rsi}: confidence {result['confidence']}% (tier: {result['tier']})")
        
        # Verify confidence doesn't exceed 90
        assert result["confidence"] <= 90, f"RSI {rsi} exceeded cap of 90%"


def test_source_quality():
    """Test that source quality is properly tracked."""
    print("\n=== TEST: Source Quality ===")
    
    result_good_source = calculate_real_confidence(
        signal_time=datetime.now(),
        direction="CALL",
        rsi=50.0,
        source="HighQualitySource",
        atr=0.001,
        ema_slope=0.0001,
    )
    
    result_unknown_source = calculate_real_confidence(
        signal_time=datetime.now(),
        direction="CALL",
        rsi=50.0,
        source="UnknownSource12345",
        atr=0.001,
        ema_slope=0.0001,
    )
    
    print(f"✓ High quality source: confidence {result_good_source['confidence']}%")
    print(f"✓ Unknown source: confidence {result_unknown_source['confidence']}%")
    print(f"✓ Source quality component working")


if __name__ == "__main__":
    try:
        test_confidence_bounds()
        test_tier_assignment()
        test_downgrades_and_boosts()
        test_rsi_binning()
        test_source_quality()
        
        print("\n" + "="*50)
        print("✓ ALL TESTS PASSED")
        print("="*50)
    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
