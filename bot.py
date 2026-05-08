import os
import time
from datetime import time as clock_time

import pandas as pd
import requests

# ONLY detect Railway (not BOT_TOKEN)
if os.getenv("RAILWAY_ENVIRONMENT"):
    from config_prod import BOT_TOKEN, CHAT_ID, TD_API_KEY
else:
    from config_local import BOT_TOKEN, CHAT_ID, TD_API_KEY

from fixed_trade import get_fixed_signal
from forex_trade import get_forex_signal
from indicators import add_indicators, calculate_score
from signal_list import (
    apply_signal_text,
    get_adaptive_trade_threshold,
    process_signal_list,
    should_force_fast_mode,
    update_signal_list,
)
from market_cache import (
    get_5m_data, 
    get_1m_data, 
    get_data, 
    set_processed_df, 
    get_processed_df,
    update_processed_df,
    get_cache_stats,
)
from risk_management import get_risk_manager, TradingMode
from market_safety import (
    check_market_safety,
    is_optimal_session,
    get_current_session,
)
from day_reset import check_and_perform_reset, should_reset_day
from smart_signal_manager import (
    get_signal_manager,
    create_signal,
    process_signals,
    get_signal_stats,
    SignalStatus,
)
from telegram_queue import (
    get_telegram_queue,
    send_telegram_queued,
    send_telegram_immediate,
    TelegramFormatter,
    MessageType,
)

PAIR = "EUR/USD"
SLEEP_TIME = 120   # 2 minutes
IDLE_SLEEP_TIME = 900   # 15 minutes during weekends/off-market hours
MARKET_OPEN_TIME = clock_time(13, 30)
MARKET_CLOSE_TIME = clock_time(21, 30)
NEWS_BLOCK_MINUTES = 15
TRADE_COOLDOWN_MINUTES = 20
HIGH_IMPACT_NEWS_EVENTS = [
    # Add high-impact event times in Asia/Kolkata timezone.
    # Example: "2026-05-03 18:00"
]
MAX_SIGNAL_MESSAGES_PER_CYCLE = 10

LAST_SIGNAL_INPUT_UPDATE_ID = None


if not TD_API_KEY:
    raise ValueError("TD_API_KEY is missing or empty")


def is_market_open():
    market_open, _ = get_market_status()
    return market_open


def get_market_status(now=None):
    if now is None:
        now = pd.Timestamp.now(tz="Asia/Kolkata")

    if now.weekday() >= 5:
        return False, "weekend"

    current_time = now.time()

    if current_time < MARKET_OPEN_TIME or current_time > MARKET_CLOSE_TIME:
        return False, "closed"

    return True, "open"


def get_next_market_open(now=None):
    if now is None:
        now = pd.Timestamp.now(tz="Asia/Kolkata")

    candidate = now.normalize() + pd.Timedelta(
        hours=MARKET_OPEN_TIME.hour,
        minutes=MARKET_OPEN_TIME.minute
    )

    if now.weekday() < 5 and now < candidate:
        return candidate

    candidate += pd.Timedelta(days=1)

    while candidate.weekday() >= 5:
        candidate += pd.Timedelta(days=1)

    return candidate


def get_idle_sleep_seconds(now=None):
    if now is None:
        now = pd.Timestamp.now(tz="Asia/Kolkata")

    seconds_until_open = (get_next_market_open(now) - now).total_seconds()
    return max(1, int(min(IDLE_SLEEP_TIME, seconds_until_open)))


def is_near_candle_close():
    now = pd.Timestamp.now(tz="Asia/Kolkata")
    return now.second >= 45


def get_current_candle_key(interval):
    now = pd.Timestamp.now(tz="Asia/Kolkata")

    if interval == "1min":
        return now.floor("min")

    if interval == "5min":
        return now.floor("5min")

    return now.floor("min")


def is_high_impact_news_window(now=None):
    if now is None:
        now = pd.Timestamp.now(tz="Asia/Kolkata")

    for event_time in HIGH_IMPACT_NEWS_EVENTS:
        event = pd.Timestamp(event_time)

        if event.tzinfo is None:
            event = event.tz_localize("Asia/Kolkata")
        else:
            event = event.tz_convert("Asia/Kolkata")

        minutes_from_event = abs((now - event).total_seconds()) / 60

        if minutes_from_event <= NEWS_BLOCK_MINUTES:
            return True, event

    return False, None


# ==============================
# TELEGRAM
# ==============================
def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        res = requests.post(url, data={
            "chat_id": CHAT_ID,
            "text": msg,
            "parse_mode": "Markdown"
        })
        print("Telegram:", res.text)

    except Exception as e:
        print("Telegram error:", e)


def fetch_signal_text_from_telegram():
    global LAST_SIGNAL_INPUT_UPDATE_ID

    try:
        params = {"timeout": 5}
        if LAST_SIGNAL_INPUT_UPDATE_ID is not None:
            params["offset"] = LAST_SIGNAL_INPUT_UPDATE_ID + 1

        url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
        res = requests.get(url, params=params, timeout=20).json()

        if not res.get("ok"):
            return None

        updates = res.get("result", [])
        if not updates:
            return None

        latest_text = None
        max_update_id = LAST_SIGNAL_INPUT_UPDATE_ID

        for update in updates:
            update_id = update.get("update_id")
            if not isinstance(update_id, int):
                continue

            if max_update_id is None or update_id > max_update_id:
                max_update_id = update_id

            message = update.get("message") or update.get("edited_message") or {}
            chat_id = str(message.get("chat", {}).get("id", ""))

            if chat_id and str(CHAT_ID) != chat_id:
                continue

            text = (message.get("text") or "").strip()
            if text:
                latest_text = text

        LAST_SIGNAL_INPUT_UPDATE_ID = max_update_id
        return latest_text

    except Exception as e:
        print("Telegram input error:", e)
        return None




def run_external_signal_engine(df, cached_minute_df=None):
    """Run external signal processing, optionally using cached minute data."""
    # Ensure we always pass a usable `df` into process_signal_list so external
    # signal processing runs even when the auto-bot skipped or df is small.
    # Prefer provided `df`, then cached_minute_df, then attempt a fresh 5min fetch.
    if df is None or (hasattr(df, "__len__") and len(df) < 200):
        fallback_df = None
        if cached_minute_df is not None and len(cached_minute_df) >= 200:
            fallback_df = cached_minute_df
        else:
            try:
                fallback_df = get_data("5min")
            except Exception:
                fallback_df = None

        if fallback_df is not None and len(fallback_df) >= 200:
            df = fallback_df

    if cached_minute_df is not None:
        minute_data_fetcher = lambda: cached_minute_df
    else:
        minute_data_fetcher = lambda: get_data("1min")

    signal_messages = process_signal_list(df, minute_data_fetcher=minute_data_fetcher)
    for signal_message in signal_messages[:MAX_SIGNAL_MESSAGES_PER_CYCLE]:
        send_telegram(signal_message)


# ==============================
# MAIN LOOP
# ==============================
def run():
    print("BOT RUNNING")
    send_telegram("*Bot Started*\n\nSmart Mode Active with Risk Management.")

    last_signal_time = None
    last_trade_time = None
    cached_interval = None
    cached_candle_key = None
    cached_df = None
    cached_minute_df = None
    cached_minute_time = None
    
    last_daily_signal_generation = None
    from signal_generator import generate_daily_signals
    
    # Initialize Risk Manager
    risk_manager = get_risk_manager()

    # Send startup diagnostics
    startup_msg = TelegramFormatter.format_startup_diagnostics(
        bot_online=True,
        cache_active=True,
        learning_active=True,
        signals_loaded=0,
    )
    send_telegram_immediate(MessageType.DIAGNOSTIC, startup_msg)

    while True:
        try:
            # 0) Generate automated signals at 10:00 AM IST
            now_tz = pd.Timestamp.now(tz="Asia/Kolkata")
            if now_tz.hour == 10 and last_daily_signal_generation != now_tz.date():
                generate_daily_signals()
                last_daily_signal_generation = now_tz.date()

            # 1) Update external signal list first.
            message_text = fetch_signal_text_from_telegram()
            if message_text:
                apply_signal_text(message_text)
            else:
                update_signal_list()

            # 2) Check market status.
            market_open, _ = get_market_status()

            if not market_open:
                print("Market closed — idle mode")
                print(f"Next market open: {get_next_market_open():%Y-%m-%d %H:%M %Z}")
                time.sleep(get_idle_sleep_seconds())
                continue

            print("Market Open — Running")

            # 2a) Check and perform day reset if needed
            # This ensures clean state at market open, no stale data from yesterday
            reset_summary = check_and_perform_reset()
            if reset_summary:
                # Day was reset, also generate fresh daily signals
                from signal_generator import generate_daily_signals
                generate_daily_signals()
                # Reset last_daily_signal_generation to avoid duplicate generation
                last_daily_signal_generation = pd.Timestamp.now(tz="Asia/Kolkata").date()
                # Reset risk manager daily stats
                risk_manager.daily_reset()

            force_fast_mode = should_force_fast_mode()

            if force_fast_mode:
                interval = "1min"
                sleep_time = 20
            else:
                interval = "5min"
                sleep_time = SLEEP_TIME

            current_candle_key = get_current_candle_key(interval)

            if (
                cached_df is not None
                and cached_interval == interval
                and cached_candle_key == current_candle_key
            ):
                df = cached_df.copy()
                print(f"Using cached {interval} data")
            else:
                df = get_data(interval)

                if df is not None:
                    cached_interval = interval
                    cached_candle_key = current_candle_key
                    cached_df = df.copy()

            # 3) Fetch df is complete above.
            if df is None:
                time.sleep(sleep_time)
                continue

            # Cache 1-minute data once per minute for all signal processing
            now_timestamp = pd.Timestamp.now(tz="Asia/Kolkata")
            now_minute_key = now_timestamp.floor("min")
            
            if cached_minute_df is None or cached_minute_time != now_minute_key:
                cached_minute_df = get_data("1min")
                cached_minute_time = now_minute_key

            # 4) COMPUTE INDICATORS ONCE - use processed_df everywhere
            # This is the single source of truth for processed market data
            df = update_processed_df()
            
            if df is None:
                print("Failed to get processed data")
                time.sleep(sleep_time)
                continue
            
            confidence, grade = calculate_score(df)
            
            # Get dynamic confidence threshold from risk manager
            market_quality = "normal"  # Could be determined from market conditions
            required_threshold = risk_manager.get_required_confidence_threshold(market_quality)
            position_multiplier = risk_manager.get_position_size_multiplier()
            
            print(f"[Risk] Mode: {risk_manager.trading_mode.value} | Confidence: {confidence}% | Required: {required_threshold}% | P.Size: {position_multiplier:.1%}")

            # Check if trade is allowed by risk manager
            can_trade, risk_reason = risk_manager.can_open_trade(confidence, market_quality)
            
            if not can_trade:
                print(f"[Risk Manager] {risk_reason}")
                # 5) Run external signal processing after auto bot.
                run_external_signal_engine(df, cached_minute_df)
                time.sleep(sleep_time)
                continue

            if confidence < required_threshold:
                print(f"Signal rejected - confidence too low: {confidence}% (required {required_threshold}%)")
                # 5) Run external signal processing after auto bot.
                run_external_signal_engine(df, cached_minute_df)
                time.sleep(sleep_time)
                continue

            fixed = get_fixed_signal(df)

            if fixed:
                now = pd.Timestamp.now(tz="Asia/Kolkata")

                if last_trade_time is not None:
                    cooldown_minutes = (now - last_trade_time).total_seconds() / 60

                    if cooldown_minutes < TRADE_COOLDOWN_MINUTES:
                        remaining = TRADE_COOLDOWN_MINUTES - cooldown_minutes
                        print(f"Trade cooldown active - {remaining:.1f} min remaining")
                        run_external_signal_engine(df, cached_minute_df)
                        time.sleep(sleep_time)
                        continue

                news_blocked, news_time = is_high_impact_news_window()

                if news_blocked:
                    print(f"Trade blocked due to high-impact news at {news_time:%H:%M}")
                    run_external_signal_engine(df, cached_minute_df)
                    time.sleep(sleep_time)
                    continue

                # Market Safety Checks
                session_ok, session_reason = is_optimal_session()
                if not session_ok:
                    print(f"Trade blocked: {session_reason}")
                    run_external_signal_engine(df, cached_minute_df)
                    time.sleep(sleep_time)
                    continue

                # Comprehensive market condition checks
                market_ok, market_reason = check_market_safety(df, fixed['signal'])
                if not market_ok:
                    print(f"Trade blocked - Market safety: {market_reason}")
                    run_external_signal_engine(df, cached_minute_df)
                    time.sleep(sleep_time)
                    continue

                current_candle_time = df.iloc[-1].get("CandleTime", df.index[-1])

                if current_candle_time == last_signal_time:
                    print("Duplicate signal skipped")
                    run_external_signal_engine(df, cached_minute_df)
                    time.sleep(sleep_time)
                    continue

                print("Score:", confidence, grade)

                # Get current session for display
                current_session = get_current_session()
                
                # Send pre-signal using new formatter
                pre_signal_msg = TelegramFormatter.format_pre_signal(
                    pair="EUR/USD",
                    direction=fixed['signal'],
                    confidence=confidence,
                    entry=str(fixed['entry']),
                    time=f"{fixed['seconds_left']//60} min",
                    source="auto_trade",
                )
                send_telegram_queued(MessageType.PRE_SIGNAL, pre_signal_msg, priority=3)

                forex = get_forex_signal(df, fixed["signal"], confidence)

                confirm = fixed

                # Send confirmed signal using new formatter
                confirmed_msg = TelegramFormatter.format_confirmed_signal(
                    pair="EUR/USD",
                    direction=forex['direction'],
                    confidence=confidence,
                    entry=str(confirm['entry']),
                    expiry=str(confirm['expiry']),
                    tp=str(forex['tp']),
                    sl=str(forex['sl']),
                    source="auto_trade",
                )
                send_telegram_queued(MessageType.CONFIRMED, confirmed_msg, priority=2)
                
                try:
                    # Record trade opening in risk manager
                    risk_manager.record_trade_open(confidence)
                    
                    entry_price = float(df.iloc[-1]["Close"])
                    expiry_time = pd.Timestamp.now(tz="Asia/Kolkata") + pd.Timedelta(minutes=5)
                    signal_time = pd.Timestamp.now(tz="Asia/Kolkata")

                    
                    # Create smart signal with full lifecycle management
                    signal = create_signal(
                        pair="EURUSD",
                        direction=forex["direction"],
                        signal_time=signal_time,
                        expiry_time=expiry_time,
                        entry_price=entry_price,
                        stop_loss=forex['sl'],
                        take_profit=forex['tp'],
                        confidence=confidence,
                        rsi=float(df.iloc[-1]["RSI"]) if "RSI" in df.columns else None,
                        atr=float(df.iloc[-1]["ATR"]) if "ATR" in df.columns else None,
                        source="auto_trade",
                    )
                    print(f"Smart Signal created: {signal.id} - Status: {signal.status.value}")
                    
                except Exception as e:
                    print("Tracking error:", e)

                last_signal_time = current_candle_time
                last_trade_time = pd.Timestamp.now(tz="Asia/Kolkata")

            else:
                print("No signal")

            # 5) Process external signal list with the same df.
            run_external_signal_engine(df, cached_minute_df)
            
            # 6) Process smart signal lifecycle - check entries and exits
            try:
                current_price = float(df.iloc[-1]["Close"])
                now = pd.Timestamp.now(tz="Asia/Kolkata")
                closed_signals = process_signals(current_price, now)
                
                # Report closed signals and record in risk manager
                for signal in closed_signals:
                    result_emoji = "✅" if signal.result.value == "win" else "❌"
                    print(f"{result_emoji} Signal {signal.id[:20]}... closed: {signal.result.value}")
                    
                    # Record trade close in risk manager
                    if signal.exit and signal.entry:
                        profit_pips = signal.exit.profit_pips
                        original_confidence = signal.confidence
                        
                        # Calculate profit/loss in pips
                        risk_manager.record_trade_close(profit_pips, original_confidence)
                        
                        # Check if safe mode should be activated
                        risk_status = risk_manager.get_status()
                        if risk_status['consecutive_losses'] >= 2 and not (risk_manager.trading_mode.value == 'safe_mode'):
                            risk_manager.activate_safe_mode(f"Consecutive losses: {risk_status['consecutive_losses']}")
                    
                    # Send result notification using new formatter
                    if signal.exit:
                        result_msg = TelegramFormatter.format_result(
                            pair=signal.pair,
                            direction=signal.direction,
                            result=signal.result.value,
                            entry=str(signal.entry.entry_price if signal.entry else 'N/A'),
                            exit=str(signal.exit.exit_price),
                            profit_pips=signal.exit.profit_pips,
                            reason=signal.exit.exit_reason,
                        )
                        send_telegram_queued(MessageType.RESULT, result_msg, priority=1)
                
                # Print active signals summary and risk status
                stats = get_signal_stats()
                if stats['active_count'] > 0:
                    print(f"Active signals: {stats['active_count']}, Total: {stats['total_signals']}")
                
                # Print risk manager status
                risk_status = risk_manager.get_status()
                print(f"[Risk Status] {risk_status['daily_trades']}/{risk_manager.config.max_daily_trades} trades | "
                      f"W/L: {risk_status['daily_wins']}/{risk_status['daily_losses']} | "
                      f"Mode: {risk_status['mode']} | Net P/L: {risk_status['net_profit_pips']}")
            except Exception as e:
                print(f"Signal processing error: {e}")
            
            time.sleep(sleep_time)

        except Exception as e:
            print("Error:", e)
            time.sleep(60)


if __name__ == "__main__":
    run()
