import pandas as pd
from typing import List, Dict, Optional
import math
import market_cache
from indicators import add_indicators, ensure_indicators
from learning_engine import get_time_win_rate, get_recent_trades

# Limits
MAX_SIGNALS_PER_DAY = 5
MIN_CANDIDATE_SCORE = 75.0
MIN_CONSISTENCY_DAYS = 10
MIN_CONSISTENCY_RATE = 0.70

# In-memory storage for generated signals
generated_signals: List[Dict] = []
last_generated_date: Optional[pd.Timestamp] = None


def _get_cached_5m_df():
    # Strictly use cached dataframe; do NOT call API here.
    df = getattr(market_cache, "cached_5m_df", None)
    if df is None:
        df = getattr(market_cache, "last_successful_5m", None)
    return df


def _get_live_df(live_df=None):
    if live_df is not None and len(live_df) >= 200:
        return live_df
    return _get_cached_5m_df()


def _historical_consistency_score(time_str: str, direction: str, lookback_days: int = 10) -> Dict:
    """Score how consistent a time+direction has performed over recent days."""
    trades = get_recent_trades(400)
    if not trades:
        return {"rate": 0.0, "wins": 0, "days": 0, "samples": []}

    by_day: Dict[str, Dict] = {}
    for trade in trades:
        signal_time = trade.get("signal_time")
        trade_direction = str(trade.get("direction", "")).upper()
        if trade_direction != direction:
            continue
        if not signal_time:
            continue
        try:
            dt = pd.to_datetime(signal_time)
        except Exception:
            continue
        if dt.strftime("%H:%M") != time_str:
            continue
        day_key = dt.strftime("%Y-%m-%d")
        by_day[day_key] = trade

    if not by_day:
        return {"rate": 0.0, "wins": 0, "days": 0, "samples": []}

    recent_days = sorted(by_day.keys(), reverse=True)[:lookback_days]
    wins = sum(1 for day in recent_days if str(by_day[day].get("result", "")).upper() == "WIN")
    days = len(recent_days)
    rate = wins / days if days else 0.0
    samples = [by_day[day] for day in recent_days]
    return {"rate": rate, "wins": wins, "days": days, "samples": samples}


def _validate_live_market(direction: str, live_df: pd.DataFrame) -> Dict:
    """Validate current live market for candidate confirmation readiness."""
    if live_df is None or len(live_df) < 200:
        return {"ok": False, "reasons": ["insufficient live data"], "trend": None, "momentum": None, "volatility": None}

    try:
        live_df = ensure_indicators(live_df.copy())
        last = live_df.iloc[-1]
        prev = live_df.iloc[-2]
        prev2 = live_df.iloc[-3]

        reasons = []

        atr = float(last["ATR"]) if not pd.isna(last.get("ATR")) else 0.0
        atr_mean = float(live_df["ATR"].mean()) if "ATR" in live_df.columns else 0.0
        candle_body = abs(float(last["Close"]) - float(last["Open"]))
        avg_body = float((live_df["Close"] - live_df["Open"]).abs().tail(10).mean())

        ema50 = float(last["EMA50"])
        ema200 = float(last["EMA200"])
        ema50_prev = float(prev["EMA50"])
        ema50_prev2 = float(prev2["EMA50"])
        slope_now = ema50 - ema50_prev
        slope_prev = ema50_prev - ema50_prev2

        rsi_now = float(last["RSI"])
        rsi_prev = float(prev["RSI"])
        rsi_prev2 = float(prev2["RSI"])

        flat_threshold = max(0.00008, atr * 0.04)
        slope_threshold = max(0.00005, atr * 0.03)
        spike_threshold = max(avg_body * 2.0, atr * 0.8)

        if abs(slope_now) < flat_threshold:
            return {"ok": False, "reasons": ["EMA flat market"], "trend": "flat", "momentum": None, "volatility": float(atr)}

        trend_ok = False
        momentum_ok = False

        if direction == "CALL":
            trend_ok = ema50 > ema200 and ema50 > ema50_prev and ema50_prev > ema50_prev2 and slope_now >= slope_threshold and slope_now > slope_prev
            momentum_ok = rsi_now >= rsi_prev >= rsi_prev2 and rsi_now >= 58
            if rsi_now > 78:
                reasons.append("RSI overbought")
            if candle_body >= spike_threshold and slope_now <= slope_prev:
                reasons.append("trend exhaustion")
            if float(last["Close"]) <= float(last["Open"]) or float(prev["Close"]) <= float(prev["Open"]):
                reasons.append("bullish continuation weak")
        else:
            trend_ok = ema50 < ema200 and ema50 < ema50_prev and ema50_prev < ema50_prev2 and abs(slope_now) >= slope_threshold and slope_now < slope_prev
            momentum_ok = rsi_now <= rsi_prev <= rsi_prev2 and rsi_now <= 42
            if rsi_now < 22:
                reasons.append("RSI oversold")
            if candle_body >= spike_threshold and slope_now >= slope_prev:
                reasons.append("trend exhaustion")
            if float(last["Close"]) >= float(last["Open"]) or float(prev["Close"]) >= float(prev["Open"]):
                reasons.append("bearish continuation weak")

        if atr <= atr_mean:
            reasons.append("low volatility")

        if trend_ok and momentum_ok and atr > atr_mean and not reasons:
            return {
                "ok": True,
                "reasons": [],
                "trend": "bullish" if direction == "CALL" else "bearish",
                "momentum": "strong",
                "volatility": float(atr),
            }

        return {
            "ok": False,
            "reasons": reasons or ["live validation failed"],
            "trend": "bullish" if ema50 > ema200 else "bearish",
            "momentum": "strong" if momentum_ok else "weak",
            "volatility": float(atr),
        }
    except Exception as e:
        return {"ok": False, "reasons": [f"live validation error: {e}"], "trend": None, "momentum": None, "volatility": None}


def analyze_timing_patterns(pair: str = "EUR/USD", horizon_bars: int = 3, min_occurrences: int = 20):
    """Analyze historical 5m candles and return stats per time-of-day.

    Returns a dict keyed by 'HH:MM' with stats: count, mean_return_pct, win_rate
    """
    df = _get_cached_5m_df()
    if df is None or len(df) < 200:
        return {}

    if "CandleTime" not in df.columns:
        return {}

    df = df.copy()
    # Ensure timezone-aware timestamps
    df["CandleTime"] = pd.to_datetime(df["CandleTime"]) if not pd.api.types.is_datetime64_any_dtype(df["CandleTime"]) else df["CandleTime"]
    df["CandleTime"] = df["CandleTime"].dt.tz_convert("Asia/Kolkata") if df["CandleTime"].dt.tz is not None else df["CandleTime"].dt.tz_localize("Asia/Kolkata")

    df.sort_values("CandleTime", inplace=True)
    df.reset_index(drop=True, inplace=True)

    times = []
    returns_by_time = {}

    closes = df["Close"].values
    times_of_day = df["CandleTime"].dt.strftime("%H:%M").values

    n = len(df)
    for i in range(n - horizon_bars):
        t = times_of_day[i]
        # compute percent return over horizon_bars (e.g., 15 minutes)
        try:
            ret = (closes[i + horizon_bars] - closes[i]) / closes[i] * 100.0
        except Exception:
            continue

        returns_by_time.setdefault(t, []).append(ret)
        times.append(t)

    stats = {}
    for t, vals in returns_by_time.items():
        cnt = len(vals)
        if cnt < min_occurrences:
            continue
        mean_ret = float(pd.Series(vals).mean())
        win_rate = float((pd.Series(vals) > 0).mean()) * 100.0
        stats[t] = {"count": cnt, "mean_return_pct": mean_ret, "win_rate_pct": win_rate}

    return stats


def generate_candidate_signals(pair: str = "EUR/USD", top_n: int = 5, horizon_bars: int = 3, live_df: Optional[pd.DataFrame] = None) -> List[Dict]:
    """Generate candidate signals only.

    These are not direct trades. A candidate must pass live validation and strong
    historical consistency checks before it is shown to the user.
    """
    global generated_signals, last_generated_date

    stats = analyze_timing_patterns(pair=pair, horizon_bars=horizon_bars)
    if not stats:
        return []

    df_stats = pd.DataFrame.from_dict(stats, orient="index")
    if df_stats.empty:
        return []

    # Normalize mean_return_pct for scoring
    mean_min = df_stats["mean_return_pct"].min()
    mean_max = df_stats["mean_return_pct"].max()

    def normalize_mean(x):
        if math.isclose(mean_max, mean_min):
            return 0.0
        return (x - mean_min) / (mean_max - mean_min)

    df_stats["mean_norm"] = df_stats["mean_return_pct"].apply(normalize_mean)

    # Score = timing win rate + timing mean return bias
    df_stats["score"] = df_stats.apply(lambda r: 0.6 * r["win_rate_pct"] + 0.4 * (r["mean_norm"] * 100.0), axis=1)

    df5 = _get_cached_5m_df()
    if df5 is None or len(df5) < 200:
        return []

    # compute indicators once for context
    try:
        df5_proc = add_indicators(df5)
    except Exception:
        df5_proc = ensure_indicators(df5)

    candidates: List[Dict] = []

    live_market_df = _get_live_df(live_df)

    # For each time slot in df_stats, compute multi-factor score
    for idx, row in df_stats.iterrows():
        time_str = idx  # 'HH:MM'
        win_rate = row.get("win_rate_pct", 0.0) / 100.0
        mean_ret = row.get("mean_return_pct", 0.0)
        base_score = row.get("score", 0.0)

        # Find historical rows matching this time to compute ATR and EMA trend consistency
        matches = df5_proc[df5_proc["CandleTime"].dt.strftime("%H:%M") == time_str]
        if matches.empty:
            continue

        atr_mean = float(matches["ATR"].mean()) if "ATR" in matches.columns else 0.0
        ema_diff_mean = float((matches["EMA50"] - matches["EMA200"]).mean())
        volatility = atr_mean

        consistency_days = _historical_consistency_score(time_str, "CALL", lookback_days=10)["days"]

        # Build both CALL and PUT candidates for this time
        for direction in ("CALL", "PUT"):
            direction_score = float(base_score)
            reasons = []

            consistency = _historical_consistency_score(time_str, direction, lookback_days=10)
            consistency_rate = consistency["rate"]
            consistency_days = consistency["days"]

            if consistency_days < MIN_CONSISTENCY_DAYS:
                continue

            if consistency_rate < MIN_CONSISTENCY_RATE:
                continue

            # Favor direction matching historical mean return sign
            if (mean_ret > 0 and direction == "CALL") or (mean_ret < 0 and direction == "PUT"):
                direction_score += 8.0
                reasons.append("historical mean aligned")

            # Favor if EMA trend aligns
            if (ema_diff_mean > 0 and direction == "CALL") or (ema_diff_mean < 0 and direction == "PUT"):
                direction_score += 6.0
                reasons.append("historical EMA trend aligned")

            # Volatility weight: moderate volatility preferred
            if volatility > 0:
                # normalize volatility roughly
                vol_score = min(10.0, (volatility / (volatility + abs(mean_ret) + 1e-9)) * 10.0)
                direction_score += vol_score
                reasons.append(f"volatility score {vol_score:.1f}")

            # Incorporate historical win rate
            direction_score += (win_rate * 25.0)

            # Consistency should materially influence ranking
            direction_score += (consistency_rate * 30.0)

            live_validation = _validate_live_market(direction, live_market_df)
            if not live_validation["ok"]:
                continue

            if direction_score < MIN_CANDIDATE_SCORE:
                continue

            # Clamp
            confidence = float(max(40.0, min(95.0, direction_score)))

            # Compute rank
            rank = "A"
            if confidence >= 85 and win_rate >= 0.7:
                rank = "S-TIER"
            elif confidence >= 75:
                rank = "A+"
            elif confidence >= 65:
                rank = "A"
            else:
                rank = "B"

            candidates.append({
                "time": time_str,
                "pair": pair.replace("/", ""),
                "direction": direction,
                "raw_score": direction_score,
                "confidence": round(confidence, 2),
                "rank": rank,
                "win_rate": round(win_rate, 3),
                "volatility": float(volatility),
                "historical_consistency": round(consistency_rate, 3),
                "historical_consistency_days": consistency_days,
                "validation": live_validation,
                "reasons": reasons,
                "status": "candidate",
            })

    # Rank candidates by raw_score and win_rate
    candidates.sort(key=lambda c: (c["raw_score"], c["win_rate"]), reverse=True)

    # Limit signals per day
    limit = max(3, min(int(top_n), MAX_SIGNALS_PER_DAY))
    selected: List[Dict] = []
    for cand in candidates:
        if len(selected) >= limit:
            break
        # minimal quality gate
        if cand["confidence"] < MIN_CANDIDATE_SCORE:
            continue
        selected.append(cand)

    generated_signals = selected
    last_generated_date = pd.Timestamp.now(tz="Asia/Kolkata").floor("D")

    return selected


def generate_daily_signals(pair: str = "EUR/USD", top_n: int = 5, horizon_bars: int = 3, live_df: Optional[pd.DataFrame] = None) -> List[Dict]:
    """Backward-compatible wrapper for candidate generation."""
    return generate_candidate_signals(pair=pair, top_n=top_n, horizon_bars=horizon_bars, live_df=live_df)


def get_generated_signals(for_date: Optional[pd.Timestamp] = None) -> List[Dict]:
    """Return generated signals for the requested date (defaults to last generated)."""
    if for_date is None:
        return list(generated_signals)

    if last_generated_date is None:
        return []

    if pd.Timestamp(for_date).floor("D") == last_generated_date:
        return list(generated_signals)

    return []


def generate_if_needed(now: Optional[pd.Timestamp] = None, live_df: Optional[pd.DataFrame] = None) -> List[Dict]:
    """Generate signals once per day at 10:00 IST. Safe to call frequently.

    - If `last_generated_date` equals today, returns existing list.
    - Does not call the API; relies on cached 5m df.
    """
    global last_generated_date
    if now is None:
        now = pd.Timestamp.now(tz="Asia/Kolkata")
    today = now.floor("D")
    trigger_time = now.normalize() + pd.Timedelta(hours=10)

    if last_generated_date is not None and last_generated_date == today:
        return list(generated_signals)

    # Only generate at or after 10:00 IST
    if now >= trigger_time:
        return generate_candidate_signals(live_df=live_df)

    return []


def format_candidate_report(signals: List[Dict]) -> str:
    """Format a candidate-only report for Telegram."""
    if not signals:
        return "*Generated Signals*\n\nNo strong candidate signals passed today's filters."

    lines = ["*Generated Signals*", "", "Candidate signals only. No direct trades.", ""]
    for signal in signals[:MAX_SIGNALS_PER_DAY]:
        validation = signal.get("validation", {})
        validation_ok = "PASS" if validation.get("ok") else "FAIL"
        consistency = signal.get("historical_consistency", 0.0)
        lines.extend([
            f"{signal['time']} {signal['direction']}",
            f"Confidence: {signal['confidence']}% | Consistency: {consistency:.0%} ({signal.get('historical_consistency_days', 0)} days)",
            f"Volatility: {signal.get('volatility', 0):.5f} | Trend: {validation.get('trend', 'n/a')} | Momentum: {validation.get('momentum', 'n/a')} | Live: {validation_ok}",
            f"Historical: {signal.get('win_rate', 0):.0%} timing win rate",
            "",
        ])
    return "\n".join(lines).strip()


__all__ = [
    "analyze_timing_patterns",
    "generate_candidate_signals",
    "generate_daily_signals",
    "get_generated_signals",
    "generate_if_needed",
    "format_candidate_report",
    "generated_signals",
]
