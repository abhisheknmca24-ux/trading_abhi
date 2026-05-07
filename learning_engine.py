import json
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import statistics
from datetime import timedelta

# Retain learning records only for this many days
LEARNING_RETENTION_DAYS = 14

LEARNING_FILE = Path("learning_memory.json")
MAX_STORED_TRADES = 2000


def _load() -> Dict[str, Any]:
    if not LEARNING_FILE.exists():
        return {"trades": []}
    try:
        with LEARNING_FILE.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"trades": []}


def _prune_and_normalize(data: Dict[str, Any]) -> Dict[str, Any]:
    """Prune trades older than retention window and normalize signal_time to ISO strings."""
    trades = data.get("trades", []) or []
    now = datetime.utcnow()
    cutoff = now - timedelta(days=LEARNING_RETENTION_DAYS)
    out: List[Dict[str, Any]] = []
    changed = False
    for tr in trades:
        st = tr.get("signal_time")
        dt = None
        if st is None:
            continue
        if isinstance(st, str):
            try:
                dt = datetime.fromisoformat(st)
            except Exception:
                try:
                    dt = datetime.strptime(st, "%Y-%m-%dT%H:%M:%S")
                except Exception:
                    dt = None
        elif isinstance(st, datetime):
            dt = st

        if dt is None:
            # drop entries without valid time
            changed = True
            continue

        # Convert naive datetimes to UTC-equivalent (assume naive are UTC)
        # and compare
        if dt.tzinfo is not None:
            dt_compare = dt.astimezone(tz=None).replace(tzinfo=None)
        else:
            dt_compare = dt

        if dt_compare >= cutoff:
            # keep only compressed, already-stored fields (assume file may already be compressed)
            # ensure signal_time is ISO
            tr["signal_time"] = dt.isoformat()
            out.append(tr)
        else:
            changed = True

    if changed:
        data["trades"] = out
        try:
            _save(data)
        except Exception:
            pass
    else:
        data["trades"] = out
    return data


def _save(data: Dict[str, Any]) -> None:
    try:
        with LEARNING_FILE.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)
    except Exception as e:
        print(f"learning_engine: failed to save learning file: {e}")


def record_trade(trade: Dict[str, Any]) -> None:
    """Record a finished trade to learning memory.

    Expected trade keys: signal_time (ISO or datetime), result ('WIN'|'LOSS'),
    confidence (number), rsi (number), atr (number), source (str), pair (str)
    """
    data = _load()
    # Ensure loaded data is pruned and normalized
    try:
        data = _prune_and_normalize(data)
    except Exception:
        pass

    trades: List[Dict[str, Any]] = data.get("trades", []) or []

    # Compress trade: avoid storing full candles or large payloads
    def _compress(t: Dict[str, Any]) -> Dict[str, Any]:
        keep_keys = ["signal_time", "result", "confidence", "rsi", "atr", "source", "pair", "direction"]
        out: Dict[str, Any] = {}
        for k in keep_keys:
            if k in t and t[k] is not None:
                out[k] = t[k]
        # Normalize time to ISO
        st = out.get("signal_time") or t.get("signal_time")
        if isinstance(st, datetime):
            out["signal_time"] = st.isoformat()
        elif isinstance(st, str):
            try:
                # Validate/normalize
                _ = datetime.fromisoformat(st)
                out["signal_time"] = st
            except Exception:
                try:
                    dt = datetime.strptime(st, "%Y-%m-%dT%H:%M:%S")
                    out["signal_time"] = dt.isoformat()
                except Exception:
                    out["signal_time"] = datetime.utcnow().isoformat()
        else:
            out["signal_time"] = datetime.utcnow().isoformat()

        # Ensure numeric types are simple floats/ints
        if "confidence" in out:
            try:
                out["confidence"] = float(out["confidence"])
            except Exception:
                out.pop("confidence", None)

        if "rsi" in out:
            try:
                out["rsi"] = float(out["rsi"])
            except Exception:
                out.pop("rsi", None)

        if "atr" in out:
            try:
                out["atr"] = float(out["atr"])
            except Exception:
                out.pop("atr", None)

        return out

    compressed = _compress(trade)
    trades.append(compressed)

    # Keep size bounded
    if len(trades) > MAX_STORED_TRADES:
        trades = trades[-MAX_STORED_TRADES:]

    data["trades"] = trades
    _save(data)


def get_recent_trades(n: int = 200) -> List[Dict[str, Any]]:
    data = _load()
    trades: List[Dict[str, Any]] = data.get("trades", [])
    return trades[-n:]


def _time_key(trade: Dict[str, Any]) -> Optional[str]:
    st = trade.get("signal_time")
    if st is None:
        return None
    if isinstance(st, str):
        try:
            st = datetime.fromisoformat(st)
        except Exception:
            return None
    try:
        return f"{st:%H:%M}_{trade.get('direction','') }"
    except Exception:
        return None


def stats_by_time() -> Dict[str, Dict[str, int]]:
    """Return win/loss counts keyed by time+direction (e.g. '15:20_CALL')."""
    trades = get_recent_trades(MAX_STORED_TRADES)
    stats: Dict[str, Dict[str, int]] = {}
    for tr in trades:
        key = _time_key(tr)
        if key is None:
            continue
        s = stats.setdefault(key, {"wins": 0, "losses": 0, "total": 0})
        if tr.get("result") == "WIN":
            s["wins"] += 1
        else:
            s["losses"] += 1
        s["total"] += 1
    return stats


def _rsi_bin(rsi: Optional[float]) -> Optional[str]:
    try:
        r = float(rsi)
    except Exception:
        return None
    low = int(r // 10) * 10
    high = low + 9
    return f"RSI_{low}-{high}"


def stats_by_rsi() -> Dict[str, Dict[str, int]]:
    trades = get_recent_trades(MAX_STORED_TRADES)
    stats: Dict[str, Dict[str, int]] = {}
    for tr in trades:
        bin_key = _rsi_bin(tr.get("rsi"))
        if bin_key is None:
            continue
        s = stats.setdefault(bin_key, {"wins": 0, "losses": 0, "total": 0})
        if tr.get("result") == "WIN":
            s["wins"] += 1
        else:
            s["losses"] += 1
        s["total"] += 1
    return stats


def stats_by_source() -> Dict[str, Dict[str, int]]:
    trades = get_recent_trades(MAX_STORED_TRADES)
    stats: Dict[str, Dict[str, int]] = {}
    for tr in trades:
        src = tr.get("source") or "unknown"
        s = stats.setdefault(src, {"wins": 0, "losses": 0, "total": 0})
        if tr.get("result") == "WIN":
            s["wins"] += 1
        else:
            s["losses"] += 1
        s["total"] += 1
    return stats


def learning_summary() -> Dict[str, Any]:
    trades = get_recent_trades(MAX_STORED_TRADES)
    total = len(trades)
    wins = sum(1 for t in trades if t.get("result") == "WIN")
    losses = total - wins
    avg_conf = None
    confs = [float(t.get("confidence")) for t in trades if t.get("confidence") is not None]
    if confs:
        avg_conf = statistics.mean(confs)
    return {
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate": (wins / total * 100.0) if total else None,
        "avg_confidence": avg_conf,
    }


def adjust_threshold(base_threshold: int = 70, lookback: int = 50) -> int:
    """Auto-adjust threshold based on recent performance.

    - If recent win rate < 50% -> increase threshold by 5
    - If recent win rate > 70% -> decrease threshold by 2 (not below 60)
    - Otherwise keep base
    """
    trades = get_recent_trades(lookback)
    if not trades:
        return base_threshold
    wins = sum(1 for t in trades if t.get("result") == "WIN")
    total = len(trades)
    win_rate = wins / total
    if win_rate < 0.5:
        return base_threshold + 5
    if win_rate > 0.7:
        return max(60, base_threshold - 2)
    return base_threshold


def record_and_learn(trade: Dict[str, Any]) -> None:
    """Helper: record trade and optionally update any derived caches.
    Currently simply records trade into JSON.
    """
    try:
        record_trade(trade)
    except Exception as e:
        print(f"learning_engine: error recording trade: {e}")


__all__ = [
    "record_trade",
    "get_recent_trades",
    "stats_by_time",
    "stats_by_rsi",
    "stats_by_source",
    "learning_summary",
    "adjust_threshold",
    "record_and_learn",
    "get_time_win_rate",
    "get_source_win_rate",
    "should_avoid_time",
    "calibrate_confidence",
    "calculate_real_confidence",
]


def get_time_win_rate(signal_time: str | datetime, direction: str, min_records: int = 5) -> dict:
    """Return win-rate and total for a given time+direction.

    signal_time: datetime or ISO string. direction: 'CALL'/'PUT'.
    Returns: {"win_rate": float (0-1), "wins": int, "total": int}
    """
    try:
        if isinstance(signal_time, str):
            st = datetime.fromisoformat(signal_time)
        else:
            st = signal_time
        key = f"{st:%H:%M}_{direction}"
    except Exception:
        return {"win_rate": 0.0, "wins": 0, "total": 0}

    stats = stats_by_time()
    entry = stats.get(key)
    if not entry:
        return {"win_rate": 0.0, "wins": 0, "total": 0}
    wins = entry.get("wins", 0)
    total = entry.get("total", 0)
    win_rate = (wins / total) if total else 0.0
    return {"win_rate": win_rate, "wins": wins, "total": total}


def get_source_win_rate(source: str, min_records: int = 5) -> dict:
    """Return win-rate and total for a given source string."""
    stats = stats_by_source()
    entry = stats.get(source) or stats.get(source.lower()) or stats.get(source.upper())
    if not entry:
        return {"win_rate": 0.0, "wins": 0, "total": 0}
    wins = entry.get("wins", 0)
    total = entry.get("total", 0)
    win_rate = (wins / total) if total else 0.0
    return {"win_rate": win_rate, "wins": wins, "total": total}


def should_avoid_time(signal_time: str | datetime, direction: str, loss_threshold: float = 0.6, min_records: int = 10) -> dict:
    """Decide whether to avoid a given time+direction based on historical loss rate.

    Returns dict: {"avoid": bool, "win_rate": float, "total": int}
    """
    res = get_time_win_rate(signal_time, direction, min_records=min_records)
    total = res.get("total", 0)
    win_rate = res.get("win_rate", 0.0)
    loss_rate = 1.0 - win_rate
    if total >= min_records and loss_rate >= loss_threshold:
        return {"avoid": True, "win_rate": win_rate, "total": total}
    return {"avoid": False, "win_rate": win_rate, "total": total}


def calculate_real_confidence(
    signal_time: str | datetime | None = None,
    direction: str | None = None,
    rsi: float | None = None,
    source: str | None = None,
    atr: float | None = None,
    ema_slope: float | None = None,
    indicator_score: float = 50.0,
    lookback: int = 50,
    min_historical_samples: int = 10,
) -> dict:
    """Calculate real, calibrated confidence based on historical performance and current conditions.

    This replaces fake weighted scores with actual win-rate data.

    Returns:
        dict with keys:
            - "confidence": int (0-100, capped at 90)
            - "components": dict of individual contributions
            - "tier": str (S-TIER, A+, A, B) based on statistical significance
            - "reasons": list of explanation strings
    """
    components = {
        "historical_time": 0.0,
        "recent_bot": 0.0,
        "source_quality": 0.0,
        "rsi_zone": 0.0,
        "market_strength": 0.0,
        "indicator_score": float(indicator_score),
    }
    reasons = []

    # 1) Historical time win-rate
    historical_time = 0.0
    historical_time_samples = 0
    if signal_time is not None and direction is not None:
        time_wr = get_time_win_rate(signal_time, direction)
        historical_time_samples = time_wr.get("total", 0)
        if historical_time_samples >= min_historical_samples:
            wr = time_wr.get("win_rate", 0.0)
            historical_time = (wr * 100.0)  # Convert to 0-100
            components["historical_time"] = historical_time
            reasons.append(f"Historical time {signal_time if isinstance(signal_time, str) else signal_time:%H:%M} {direction}: {wr:.1%} ({historical_time_samples} trades)")
        else:
            reasons.append(f"Insufficient time samples: {historical_time_samples}/{min_historical_samples}")

    # 2) Recent bot win-rate (last N trades)
    recent_bot = 50.0  # neutral default
    recent = get_recent_trades(lookback)
    if recent:
        recent_wins = sum(1 for t in recent if t.get("result") == "WIN")
        recent_wr = recent_wins / len(recent)
        recent_bot = (recent_wr * 100.0)  # Convert to 0-100
        components["recent_bot"] = recent_bot
        if recent_wr < 0.5:
            reasons.append(f"⚠️ Recent performance poor: {recent_wr:.1%}")
        else:
            reasons.append(f"Recent performance: {recent_wr:.1%}")

    # 3) Source win-rate
    source_quality = 50.0  # neutral default
    source_samples = 0
    if source:
        src_wr = get_source_win_rate(source)
        source_samples = src_wr.get("total", 0)
        if source_samples >= 5:
            swr = src_wr.get("win_rate", 0.0)
            source_quality = (swr * 100.0)
            components["source_quality"] = source_quality
            reasons.append(f"Source '{source}' quality: {swr:.1%} ({source_samples} trades)")
        else:
            reasons.append(f"Source '{source}' has insufficient data: {source_samples}/5")

    # 4) RSI zone win-rate
    rsi_zone = 50.0  # neutral default
    rsi_zone_samples = 0
    if rsi is not None:
        bin_key = _rsi_bin(rsi)
        rsi_stats = stats_by_rsi()
        bin_entry = rsi_stats.get(bin_key)
        if bin_entry and bin_entry.get("total", 0) >= 5:
            rsi_zone_samples = bin_entry.get("total", 0)
            rsi_wr = (bin_entry.get("wins", 0) / bin_entry.get("total", 1))
            rsi_zone = (rsi_wr * 100.0)
            components["rsi_zone"] = rsi_zone
            reasons.append(f"RSI zone {bin_key}: {rsi_wr:.1%} ({rsi_zone_samples} trades)")
        else:
            if bin_key:
                reasons.append(f"RSI zone {bin_key} has insufficient data")

    # 5) Market strength (ATR, EMA slope)
    market_strength = 50.0  # neutral default
    tech_downgrades = []
    if atr is not None and atr <= 0.0001:
        tech_downgrades.append("ATR very weak")
    if ema_slope is not None and abs(ema_slope) < 0.00005:
        tech_downgrades.append("EMA slope weak")

    if not tech_downgrades:
        market_strength = 65.0  # moderate boost if no technical weaknesses
        components["market_strength"] = market_strength
    else:
        market_strength = 35.0  # downgrade for weak technicals
        components["market_strength"] = market_strength
        reasons.append(f"⚠️ Technical weaknesses: {', '.join(tech_downgrades)}")

    # Compute weighted average
    # Weights: 25% historical_time, 25% recent_bot, 15% source_quality, 15% rsi_zone, 20% market_strength
    weights = {
        "historical_time": 0.25,
        "recent_bot": 0.25,
        "source_quality": 0.15,
        "rsi_zone": 0.15,
        "market_strength": 0.20,
    }
    
    # If we don't have historical time samples, reduce its weight and redistribute
    if historical_time_samples < min_historical_samples:
        weights["historical_time"] = 0.0
        weights["recent_bot"] = 0.35
        weights["source_quality"] = 0.20
        weights["rsi_zone"] = 0.15
        weights["market_strength"] = 0.30

    weighted_sum = sum(components.get(k, 50.0) * w for k, w in weights.items())
    confidence_raw = weighted_sum

    # Downgrade for weak technical conditions
    if tech_downgrades:
        confidence_raw -= 12.0

    # Downgrade for poor recent performance
    if recent_bot < 45.0:
        confidence_raw -= 8.0
    elif recent_bot < 50.0:
        confidence_raw -= 4.0

    # Downgrade for weak RSI zone
    if rsi_zone < 45.0:
        confidence_raw -= 10.0

    # Boost for strong historical time
    if historical_time > 70.0 and historical_time_samples >= min_historical_samples:
        confidence_raw += 8.0

    # Boost for strong source quality
    if source_quality > 70.0 and source_samples >= 5:
        confidence_raw += 6.0

    # Cap at 90%
    confidence_final = int(max(0, min(90, round(confidence_raw))))

    # Determine tier based on confidence and statistical significance
    tier = "B"
    if confidence_final >= 80 and historical_time_samples >= min_historical_samples and historical_time > 70.0:
        tier = "S-TIER"
    elif confidence_final >= 75 and (historical_time_samples >= 10 or source_samples >= 10):
        tier = "A+"
    elif confidence_final >= 65:
        tier = "A"

    reasons.append(f"Final confidence: {confidence_final}% (tier: {tier})")

    return {
        "confidence": confidence_final,
        "components": components,
        "tier": tier,
        "reasons": reasons,
    }


def calibrate_confidence(confidence: float, rsi: float | None = None, source: str | None = None, lookback: int = 50) -> int:
    """Legacy wrapper for backward compatibility. Use calculate_real_confidence instead."""
    result = calculate_real_confidence(
        rsi=rsi,
        source=source,
        indicator_score=float(confidence),
        lookback=lookback,
    )
    return result["confidence"]
