"""
Telegram Queue System
Fast and clean message delivery with queue management.
"""

import os
import time
import threading
import requests
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, List
from dataclasses import dataclass, field
from queue import PriorityQueue, Empty


class MessageType(Enum):
    """Allowed message types for clean communication."""
    PRE_SIGNAL = "pre_signal"       # Pre-signal alert
    CONFIRMED = "confirmed"         # Confirmed signal
    SKIPPED = "skipped"             # Signal skipped
    RESULT = "result"               # Signal result
    DAILY_REPORT = "daily_report"   # Daily performance report
    DIAGNOSTIC = "diagnostic"       # System diagnostics
    ALERT = "alert"                 # Important alerts


class SignalSource(Enum):
    """Signal source labels."""
    INTERNAL_GENERATOR = "Internal Generator 🤖"
    EXTERNAL_PROVIDER = "External Provider 📡"
    MERGED_SIGNAL = "Merged Signal 🔀"


class SignalQuality(Enum):
    """Signal quality labels based on real confidence."""
    S_TIER = "S-TIER 🔥"      # >= 85%
    A_PLUS = "A+ ✅"          # >= 75%
    A = "A"                    # >= 65%
    BELOW_A = "B"              # < 65% (won't be sent)


@dataclass
class TelegramMessage:
    """Telegram message with metadata."""
    msg_type: MessageType
    text: str
    priority: int = 5           # 1 = highest, 10 = lowest
    timestamp: datetime = field(default_factory=datetime.now)
    retries: int = 0
    max_retries: int = 3


class TelegramQueue:
    """
    Priority-based Telegram message sender.
    Ensures fast, clean, ordered message delivery with health monitoring.
    """
    
    PRIORITY_MAP = {
        MessageType.ALERT: 1,              # 1 = emergency alerts
        MessageType.CONFIRMED: 2,           # 2 = confirmed trades
        MessageType.SKIPPED: 3,             # 3 = skipped important trades
        MessageType.RESULT: 4,               # 4 = results
        MessageType.DAILY_REPORT: 5,         # 5 = reports
        MessageType.PRE_SIGNAL: 6,          # 6 = pre-signals (lowest priority)
        MessageType.DIAGNOSTIC: 7,          # 7 = diagnostics (lowest)
    }
    
    def __init__(self, bot_token: Optional[str] = None, chat_id: Optional[str] = None):
        self.bot_token = bot_token or os.getenv("BOT_TOKEN") or self._load_config()
        self.chat_id = chat_id or os.getenv("CHAT_ID")
        
        self.queue: PriorityQueue = PriorityQueue()
        self.sent_count = 0
        self.error_count = 0
        self.last_send_time = 0
        self.min_interval = 0.1     # Min 100ms between messages
        
        self._running = False
        self._worker_thread: Optional[threading.Thread] = None
        self._last_health_check = 0
        self._health_check_interval = 30  # Check worker health every 30 seconds
        self._consecutive_empty_polls = 0
        
        # Message filtering
        self.allowed_types = {
            MessageType.PRE_SIGNAL,
            MessageType.CONFIRMED,
            MessageType.SKIPPED,
            MessageType.RESULT,
            MessageType.DAILY_REPORT,
            MessageType.DIAGNOSTIC,
            MessageType.ALERT,
        }
        
        # Rate limiting
        self.rate_limit_count = 0
        self.rate_limit_reset = datetime.now()
    
    def _load_config(self) -> str:
        """Load config based on environment."""
        try:
            if os.getenv("RAILWAY_ENVIRONMENT"):
                from config_prod import BOT_TOKEN
                return BOT_TOKEN
            else:
                from config_local import BOT_TOKEN
                return BOT_TOKEN
        except:
            return ""
    
    def start(self) -> None:
        """Start the queue worker thread."""
        if self._running:
            return
        
        self._running = True
        self._worker_thread = threading.Thread(target=self._worker, daemon=True)
        self._worker_thread.start()
        print("Telegram queue started")
    
    def _check_worker_health(self) -> bool:
        """Check if worker thread is alive, restart if dead."""
        now = time.time()
        if now - self._last_health_check < self._health_check_interval:
            return True
        
        self._last_health_check = now
        
        if self._worker_thread is not None and not self._worker_thread.is_alive():
            print("Telegram queue worker stopped unexpectedly, restarting...")
            self._running = True
            self._worker_thread = threading.Thread(target=self._worker, daemon=True)
            self._worker_thread.start()
            self._consecutive_empty_polls = 0
            return False
        
        return True
    
    def stop(self) -> None:
        """Stop the queue worker."""
        self._running = False
        if self._worker_thread:
            self._worker_thread.join(timeout=5)
        print("Telegram queue stopped")
    
    def _worker(self) -> None:
        """Worker thread to process queue with priority ordering."""
        while self._running:
            try:
                # Check worker health periodically
                self._check_worker_health()
                
                # Get message with priority (lower number = higher priority)
                item = self.queue.get(timeout=1)
                priority, message = item
                
                self._send_message(message)
                self.queue.task_done()
                self._consecutive_empty_polls = 0
            except Empty:
                self._consecutive_empty_polls += 1
                # Log queue diagnostics every 60 empty polls (about 1 minute)
                if self._consecutive_empty_polls == 60:
                    stats = self.get_stats()
                    print(f"Telegram queue idle: {stats['queued']} queued, {stats['sent']} sent, {stats['errors']} errors")
                continue
            except Exception as e:
                print(f"Queue worker error: {e}")
    
    def _send_message(self, message: TelegramMessage) -> bool:
        """Send a single message with rate limiting and retries."""
        if not self.bot_token or not self.chat_id:
            print("Telegram credentials not configured")
            return False
        
        # Rate limiting
        now = time.time()
        time_since_last = now - self.last_send_time
        if time_since_last < self.min_interval:
            time.sleep(self.min_interval - time_since_last)
        
        # Telegram API rate limit (20 messages per minute)
        self._check_rate_limit()
        
        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message.text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        
        try:
            response = requests.post(url, json=payload, timeout=10)
            self.last_send_time = time.time()
            
            if response.status_code == 200:
                self.sent_count += 1
                self.rate_limit_count += 1
                return True
            elif response.status_code == 429:
                # Rate limited by Telegram
                retry_after = int(response.headers.get("Retry-After", 30))
                print(f"Telegram rate limit hit. Waiting {retry_after}s...")
                time.sleep(retry_after)
                return self._retry_message(message)
            else:
                print(f"Telegram error {response.status_code}: {response.text}")
                return self._retry_message(message)
                
        except Exception as e:
            print(f"Send error: {e}")
            return self._retry_message(message)
    
    def _check_rate_limit(self) -> None:
        """Check and reset rate limit counter."""
        now = datetime.now()
        if (now - self.rate_limit_reset).total_seconds() >= 60:
            self.rate_limit_count = 0
            self.rate_limit_reset = now
        
        # If approaching limit, pause
        if self.rate_limit_count >= 18:
            print("Approaching Telegram rate limit, pausing...")
            time.sleep(5)
    
    def _retry_message(self, message: TelegramMessage) -> bool:
        """Retry failed message."""
        if message.retries < message.max_retries:
            message.retries += 1
            time.sleep(2 ** message.retries)  # Exponential backoff
            return self._send_message(message)
        else:
            self.error_count += 1
            print(f"Message failed after {message.max_retries} retries")
            return False
    
    def enqueue(self, msg_type: MessageType, text: str, priority: int = 5) -> bool:
        """
        Add message to queue with priority ordering.
        Returns True if queued, False if filtered out.
        """
        # Filter message types
        if msg_type not in self.allowed_types:
            print(f"Message type {msg_type.value} filtered out")
            return False
        
        # Get message type priority (1 = highest)
        type_priority = self.PRIORITY_MAP.get(msg_type, 5)
        
        # Allow user to override priority but keep within sensible bounds
        final_priority = min(max(priority, 1), 10)
        
        # Create message object
        message = TelegramMessage(
            msg_type=msg_type,
            text=text,
            priority=final_priority
        )
        
        # PriorityQueue stores (priority, message), lower priority number = higher priority
        self.queue.put((type_priority, message))
        return True
    
    def send_immediate(self, msg_type: MessageType, text: str) -> bool:
        """
        Send message immediately (bypass queue).
        Use sparingly for critical alerts only.
        """
        if msg_type not in self.allowed_types:
            return False
        
        message = TelegramMessage(
            msg_type=msg_type,
            text=text,
            priority=1
        )
        return self._send_message(message)
    
    def get_stats(self) -> Dict:
        """Get queue statistics."""
        return {
            "queued": self.queue.qsize(),
            "sent": self.sent_count,
            "errors": self.error_count,
            "running": self._running,
        }
    
    def flush(self) -> None:
        """Wait for all queued messages to be sent."""
        self.queue.join()


class TelegramFormatter:
    """
    Formats Telegram messages with consistent structure.
    """
    
    @staticmethod
    def get_quality_label(confidence: int) -> str:
        """Get quality label based on real confidence."""
        if confidence >= 85:
            return SignalQuality.S_TIER.value
        elif confidence >= 75:
            return SignalQuality.A_PLUS.value
        elif confidence >= 65:
            return SignalQuality.A.value
        else:
            return SignalQuality.BELOW_A.value
    
    @staticmethod
    def get_source_label(source: str) -> str:
        """Get source label string."""
        source_map = {
            "auto": SignalSource.INTERNAL_GENERATOR.value,
            "auto_trade": SignalSource.INTERNAL_GENERATOR.value,
            "internal": SignalSource.INTERNAL_GENERATOR.value,
            "direct": SignalSource.EXTERNAL_PROVIDER.value,
            "external": SignalSource.EXTERNAL_PROVIDER.value,
            "merged": SignalSource.MERGED_SIGNAL.value,
            "martingale": SignalSource.INTERNAL_GENERATOR.value,
        }
        return source_map.get(source.lower(), SignalSource.EXTERNAL_PROVIDER.value)
    
    @staticmethod
    def format_pre_signal(
        pair: str,
        direction: str,
        confidence: int,
        entry: str,
        time: str,
        source: str = "internal",
        ema_trend: Optional[str] = None,
        rsi: Optional[float] = None,
        atr: Optional[str] = None,
    ) -> str:
        """Format pre-signal message."""
        quality = TelegramFormatter.get_quality_label(confidence)
        source_label = TelegramFormatter.get_source_label(source)
        
        msg = f"""📊 *PRE SIGNAL*

*Pair:* {pair}
*Direction:* {direction}
*Quality:* {quality}
*Source:* {source_label}

*Setup*
Entry: {entry}
Time: {time}
Confidence: {confidence}%
"""
        
        if ema_trend:
            msg += f"Trend: {ema_trend}\n"
        if rsi is not None:
            msg += f"RSI: {rsi:.0f}\n"
        if atr:
            msg += f"ATR: {atr}\n"
        
        msg += "\n⏳ Awaiting confirmation..."
        return msg
    
    @staticmethod
    def format_confirmed_signal(
        pair: str,
        direction: str,
        confidence: int,
        entry: str,
        expiry: str,
        tp: str,
        sl: str,
        source: str = "internal",
    ) -> str:
        """Format confirmed signal message."""
        quality = TelegramFormatter.get_quality_label(confidence)
        source_label = TelegramFormatter.get_source_label(source)
        
        return f"""✅ *CONFIRMED SIGNAL*

*Pair:* {pair}
*Direction:* {direction}
*Quality:* {quality}
*Source:* {source_label}

*Trade Setup*
Entry: {entry}
Expiry: {expiry}

*Risk Management*
TP: {tp}
SL: {sl}
Confidence: {confidence}%

🎯 Signal is ACTIVE
"""
    
    @staticmethod
    def format_skipped_signal(
        pair: str,
        direction: str,
        time: str,
        reason: str,
        confidence: Optional[int] = None,
    ) -> str:
        """Format skipped signal message with clear reason."""
        msg = f"""⛔ *SKIPPED*

*Pair:* {pair}
*Direction:* {direction}
*Time:* {time}
"""
        if confidence:
            msg += f"*Confidence:* {confidence}%\n"
        
        msg += f"\n*Reason:* {reason}\n"
        msg += "\n⚠️ Signal cancelled - conditions unfavorable"
        return msg
    
    @staticmethod
    def format_result(
        pair: str,
        direction: str,
        result: str,  # WIN or LOSS
        entry: str,
        exit: str,
        profit_pips: float,
        reason: str,
    ) -> str:
        """Format result message."""
        emoji = "✅" if result.upper() == "WIN" else "❌"
        
        return f"""{emoji} *RESULT*

*Pair:* {pair}
*Direction:* {direction}
*Outcome:* {result.upper()}

*Trade Details*
Entry: {entry}
Exit: {exit}
P/L: {profit_pips:+.5f} pips

*Exit Reason:* {reason}
"""
    
    @staticmethod
    def format_daily_report(
        date: str,
        total: int,
        wins: int,
        losses: int,
        win_rate: float,
        safety_status: str,
    ) -> str:
        """Format daily performance report."""
        return f"""📊 *DAILY REPORT* - {date}

*Performance*
Total Trades: {total}
Wins: {wins} ✅
Losses: {losses} ❌
Win Rate: {win_rate:.1f}%

*System Status*
🛡️ {safety_status}

— End of Day —
"""
    
    @staticmethod
    def format_startup_diagnostics(
        bot_online: bool,
        cache_active: bool,
        learning_active: bool,
        signals_loaded: int = 0,
    ) -> str:
        """Format startup diagnostics message."""
        bot_status = "🟢 Online" if bot_online else "🔴 Offline"
        cache_status = "🟢 Active" if cache_active else "🔴 Inactive"
        learning_status = "🟢 Active" if learning_active else "🔴 Inactive"
        
        return f"""🤖 *BOT STARTED*

*System Diagnostics*
Bot: {bot_status}
Cache: {cache_status}
Learning: {learning_status}

*Signal Queue*
Loaded: {signals_loaded} signals

⏰ Ready for trading
"""
    
    @staticmethod
    def format_alert(title: str, message: str, severity: str = "info") -> str:
        """Format alert message."""
        emoji_map = {
            "info": "ℹ️",
            "warning": "⚠️",
            "error": "🚨",
            "success": "✅",
        }
        emoji = emoji_map.get(severity, "ℹ️")
        
        return f"""{emoji} *{title}*

{message}
"""


# Global queue instance
_telegram_queue: Optional[TelegramQueue] = None


def get_telegram_queue() -> TelegramQueue:
    """Get or create global Telegram queue."""
    global _telegram_queue
    if _telegram_queue is None:
        _telegram_queue = TelegramQueue()
        _telegram_queue.start()
    return _telegram_queue


def send_telegram_queued(msg_type: MessageType, text: str, priority: int = 5) -> bool:
    """Convenience function to send queued message."""
    queue = get_telegram_queue()
    return queue.enqueue(msg_type, text, priority)


def send_telegram_immediate(msg_type: MessageType, text: str) -> bool:
    """Convenience function for immediate send."""
    queue = get_telegram_queue()
    return queue.send_immediate(msg_type, text)


def stop_telegram_queue() -> None:
    """Stop the Telegram queue."""
    global _telegram_queue
    if _telegram_queue:
        _telegram_queue.stop()
        _telegram_queue = None
