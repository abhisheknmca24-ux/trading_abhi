"""
Smart Signal Manager
Handles complete signal lifecycle: entry, exit, and state management.
"""

import os
import json
import uuid
import time
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Optional, Dict, List, Tuple, Callable
from dataclasses import dataclass, field, asdict


class SignalStatus(Enum):
    """Signal lifecycle states."""
    CREATE = "create"           # Signal just created
    PENDING = "pending"         # Waiting for entry window
    ACTIVE = "active"           # Entry triggered, trade open
    TP_HIT = "tp_hit"          # Take profit reached
    SL_HIT = "sl_hit"          # Stop loss hit
    EXPIRED = "expired"        # Time expired
    CLOSED = "closed"          # Manually closed
    END = "end"                # Final state, archived


class SignalResult(Enum):
    """Signal outcome."""
    NONE = None
    WIN = "win"
    LOSS = "loss"
    BREAKEVEN = "breakeven"
    EXPIRED = "expired"
    CANCELLED = "cancelled"


@dataclass
class SignalEntry:
    """Signal entry parameters."""
    entry_price: float
    stop_loss: float
    take_profit: float
    entry_window_start: datetime
    entry_window_end: datetime
    max_slippage_pips: float = 0.0005  # 5 pips for EUR/USD


@dataclass
class SignalExit:
    """Signal exit details."""
    exit_price: Optional[float] = None
    exit_time: Optional[datetime] = None
    exit_reason: Optional[str] = None
    profit_pips: float = 0.0
    result: SignalResult = SignalResult.NONE


@dataclass
class Signal:
    """Complete signal data structure."""
    # Identification
    id: str
    pair: str
    direction: str  # CALL/PUT
    
    # Timing
    signal_time: datetime
    expiry_time: datetime
    created_at: datetime = field(default_factory=datetime.now)
    
    # Status
    status: SignalStatus = SignalStatus.CREATE
    result: SignalResult = SignalResult.NONE
    
    # Entry/Exit
    entry: Optional[SignalEntry] = None
    exit: SignalExit = field(default_factory=SignalExit)
    
    # Market data at creation
    confidence: int = 0
    rsi: Optional[float] = None
    atr: Optional[float] = None
    ema_trend: Optional[str] = None
    
    # Updates
    updates: List[Dict] = field(default_factory=list)
    last_update_time: Optional[datetime] = None
    
    # Source
    source: str = "auto"  # auto, external, manual
    
    # Logic flags
    auto_trade: bool = False
    is_martingale: bool = False
    
    def to_dict(self) -> Dict:
        """Convert signal to dictionary for serialization."""
        data = asdict(self)
        # Convert enums to strings
        data['status'] = self.status.value
        data['result'] = self.result.value if self.result else None
        return data
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Signal':
        """Create signal from dictionary."""
        # Convert string enums back
        data['status'] = SignalStatus(data.get('status', 'create'))
        result_val = data.get('result')
        data['result'] = SignalResult(result_val) if result_val else SignalResult.NONE
        
        # Reconstruct nested dataclasses
        if data.get('entry'):
            data['entry'] = SignalEntry(**data['entry'])
        if data.get('exit'):
            data['exit'] = SignalExit(**data['exit'])
        
        return cls(**data)


class SmartSignalManager:
    """
    Manages complete signal lifecycle from creation to result.
    """
    
    SIGNALS_FILE = "smart_signals.json"
    MAX_COMPLETED_SIGNALS = 200   # Hard cap for Railway disk efficiency
    RETENTION_DAYS = 14           # Rolling 14-day window as requested
    SAVE_DEBOUNCE_SECONDS = 30    # Minimum time between disk saves
    MAX_UPDATES_PER_SIGNAL = 20   # Memory growth limit for updates list
    
    def __init__(self):
        self.signals: Dict[str, Signal] = {}
        self.active_signals: Dict[str, Signal] = {}
        self.completed_signals: List[Signal] = []
        self._callbacks: Dict[str, List[Callable]] = {
            'on_create': [],
            'on_entry': [],
            'on_exit': [],
            'on_result': [],
        }
        
        # Debouncing state
        self._last_save_time = 0
        self._pending_updates = 0
        
        self._load_signals()
    
    def _now(self) -> datetime:
        """Get current time in Asia/Kolkata timezone."""
        try:
            from zoneinfo import ZoneInfo
            return datetime.now(ZoneInfo("Asia/Kolkata"))
        except ImportError:
            return datetime.now()
    
    def _generate_id(self, pair: str, signal_time: datetime, direction: str) -> str:
        """Generate stable unique signal ID using UUID5."""
        # Use a consistent namespace for our signals
        namespace = uuid.NAMESPACE_DNS
        name = f"{pair}_{signal_time.isoformat()}_{direction}"
        return str(uuid.uuid5(namespace, name)).replace("-", "")[:12]
    
    def _load_signals(self) -> None:
        """Load signals from file."""
        try:
            if os.path.exists(self.SIGNALS_FILE):
                with open(self.SIGNALS_FILE, 'r') as f:
                    data = json.load(f)
                    for sig_data in data.get('active', []):
                        sig = Signal.from_dict(sig_data)
                        self.signals[sig.id] = sig
                        if sig.status not in [SignalStatus.END, SignalStatus.EXPIRED, 
                                              SignalStatus.TP_HIT, SignalStatus.SL_HIT]:
                            self.active_signals[sig.id] = sig
                    for sig_data in data.get('completed', []):
                        sig = Signal.from_dict(sig_data)
                        self.completed_signals.append(sig)
        except Exception as e:
            print(f"Error loading signals: {e}")
    
    def _save_signals(self, force: bool = False) -> None:
        """Save signals to file, enforcing retention cap and debouncing."""
        now_ts = time.time()
        self._pending_updates += 1
        
        # Debounce: save if forced, or enough updates, or enough time passed
        if not force and self._pending_updates < 5 and (now_ts - self._last_save_time) < self.SAVE_DEBOUNCE_SECONDS:
            return

        try:
            # Prune completed signals: last 30 days and max 200
            cutoff = self._now() - timedelta(days=self.RETENTION_DAYS)
            fresh = [
                sig for sig in self.completed_signals
                if (
                    sig.created_at if isinstance(sig.created_at, datetime)
                    else datetime.fromisoformat(str(sig.created_at))
                ) >= cutoff
            ]
            # Also enforce hard cap — keep most recent
            if len(fresh) > self.MAX_COMPLETED_SIGNALS:
                fresh = fresh[-self.MAX_COMPLETED_SIGNALS:]
            self.completed_signals = fresh

            data = {
                'active': [sig.to_dict() for sig in self.active_signals.values()],
                'completed': [sig.to_dict() for sig in self.completed_signals],
                'saved_at': self._now().isoformat()
            }
            with open(self.SIGNALS_FILE, 'w') as f:
                json.dump(data, f, default=str, indent=2)
            
            self._last_save_time = now_ts
            self._pending_updates = 0
        except Exception as e:
            print(f"Error saving signals: {e}")

    def prune_old_signals(self) -> int:
        """Public cleanup: remove signals older than RETENTION_DAYS. Returns count removed."""
        cutoff = self._now() - timedelta(days=self.RETENTION_DAYS)
        before = len(self.completed_signals)
        self.completed_signals = [
            sig for sig in self.completed_signals
            if (
                sig.created_at if isinstance(sig.created_at, datetime)
                else datetime.fromisoformat(str(sig.created_at))
            ) >= cutoff
        ]
        if len(self.completed_signals) > self.MAX_COMPLETED_SIGNALS:
            self.completed_signals = self.completed_signals[-self.MAX_COMPLETED_SIGNALS:]
        removed = before - len(self.completed_signals)
        if removed > 0:
            self._save_signals()
            print(f"  ✂ SmartSignalManager: pruned {removed} old signals ({len(self.completed_signals)} kept)")
        return removed
    
    def create_signal(
        self,
        pair: str,
        direction: str,
        signal_time: datetime,
        expiry_time: datetime,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        confidence: int = 0,
        rsi: Optional[float] = None,
        atr: Optional[float] = None,
        ema_trend: Optional[str] = None,
        source: str = "auto",
        auto_trade: bool = False,
        is_martingale: bool = False
    ) -> Signal:
        """
        Create a new signal with entry parameters.
        Status: CREATE → PENDING
        """
        signal_id = self._generate_id(pair, signal_time, direction)
        
        entry_window_start = signal_time - timedelta(minutes=1)
        entry_window_end = signal_time + timedelta(minutes=2)
        
        entry = SignalEntry(
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            entry_window_start=entry_window_start,
            entry_window_end=entry_window_end,
        )
        
        signal = Signal(
            id=signal_id,
            pair=pair,
            direction=direction,
            signal_time=signal_time,
            expiry_time=expiry_time,
            entry=entry,
            confidence=confidence,
            rsi=rsi,
            atr=atr,
            ema_trend=ema_trend,
            source=source,
            auto_trade=auto_trade,
            is_martingale=is_martingale,
            status=SignalStatus.PENDING
        )
        
        self.signals[signal_id] = signal
        self.active_signals[signal_id] = signal
        
        self._trigger_callbacks('on_create', signal)
        self._save_signals()
        
        print(f"📊 Signal {signal_id}: {SignalStatus.CREATE.value} → {SignalStatus.PENDING.value}")
        return signal
    
    def check_entry(self, signal_id: str, current_price: float, current_time: datetime) -> bool:
        """
        Check if signal entry conditions are met.
        Status: PENDING → ACTIVE
        """
        if signal_id not in self.active_signals:
            return False
        
        signal = self.active_signals[signal_id]
        
        if signal.status != SignalStatus.PENDING:
            return False
        
        if not signal.entry:
            return False
        
        # Check entry window
        if current_time < signal.entry.entry_window_start:
            return False
        
        if current_time > signal.entry.entry_window_end:
            # Entry window expired
            signal.status = SignalStatus.EXPIRED
            signal.exit.exit_reason = "Entry window expired"
            signal.exit.exit_time = current_time
            signal.result = SignalResult.CANCELLED
            self._move_to_completed(signal)
            return False
        
        # Check price reached entry
        entry = signal.entry.entry_price
        slippage = signal.entry.max_slippage_pips
        
        if signal.direction.upper() in ['CALL', 'BUY']:
            # For CALL: price should touch or cross entry from below
            if current_price >= entry - slippage:
                signal.status = SignalStatus.ACTIVE
                signal.updates.append({
                    'time': current_time.isoformat(),
                    'price': current_price,
                    'event': 'entry_triggered'
                })
                self._trigger_callbacks('on_entry', signal)
                self._save_signals()
                print(f"🎯 Signal {signal_id}: {SignalStatus.PENDING.value} → {SignalStatus.ACTIVE.value} @ {current_price}")
                return True
        else:
            # For PUT: price should touch or cross entry from above
            if current_price <= entry + slippage:
                signal.status = SignalStatus.ACTIVE
                signal.updates.append({
                    'time': current_time.isoformat(),
                    'price': current_price,
                    'event': 'entry_triggered'
                })
                # Limit updates list
                if len(signal.updates) > self.MAX_UPDATES_PER_SIGNAL:
                    signal.updates = signal.updates[-self.MAX_UPDATES_PER_SIGNAL:]

                self._trigger_callbacks('on_entry', signal)
                self._save_signals()
                print(f"🎯 Signal {signal_id}: {SignalStatus.PENDING.value} → {SignalStatus.ACTIVE.value} @ {current_price}")
                return True
        
        return False
    
    def check_exit(self, signal_id: str, current_price: float, current_time: datetime) -> Optional[SignalResult]:
        """
        Check if signal exit conditions are met (TP, SL, Expiry).
        Status: ACTIVE → TP_HIT/SL_HIT/EXPIRED
        """
        if signal_id not in self.active_signals:
            return None
        
        signal = self.active_signals[signal_id]
        
        if signal.status != SignalStatus.ACTIVE:
            return None
        
        if not signal.entry:
            return None
        
        entry_price = signal.entry.entry_price
        tp = signal.entry.take_profit
        sl = signal.entry.stop_loss
        
        result = None
        exit_price = None
        exit_reason = None
        
        # Check TP hit
        if signal.direction.upper() in ['CALL', 'BUY']:
            if current_price >= tp:
                result = SignalResult.WIN
                exit_price = current_price
                exit_reason = "Take profit hit"
                signal.status = SignalStatus.TP_HIT
            elif current_price <= sl:
                result = SignalResult.LOSS
                exit_price = current_price
                exit_reason = "Stop loss hit"
                signal.status = SignalStatus.SL_HIT
        else:
            if current_price <= tp:
                result = SignalResult.WIN
                exit_price = current_price
                exit_reason = "Take profit hit"
                signal.status = SignalStatus.TP_HIT
            elif current_price >= sl:
                result = SignalResult.LOSS
                exit_price = current_price
                exit_reason = "Stop loss hit"
                signal.status = SignalStatus.SL_HIT
        
        # Check expiry (for fixed time trades)
        if result is None and current_time >= signal.expiry_time:
            # Determine result based on price vs entry
            if signal.direction.upper() in ['CALL', 'BUY']:
                if current_price > entry_price:
                    result = SignalResult.WIN
                elif current_price < entry_price:
                    result = SignalResult.LOSS
                else:
                    result = SignalResult.BREAKEVEN
            else:
                if current_price < entry_price:
                    result = SignalResult.WIN
                elif current_price > entry_price:
                    result = SignalResult.LOSS
                else:
                    result = SignalResult.BREAKEVEN
            
            exit_price = current_price
            exit_reason = f"Expired - {result.value}"
            signal.status = SignalStatus.EXPIRED
        
        if result:
            # Calculate profit in pips
            profit_pips = abs(current_price - entry_price)
            if result == SignalResult.LOSS:
                profit_pips = -profit_pips
            
            signal.exit = SignalExit(
                exit_price=exit_price,
                exit_time=current_time,
                exit_reason=exit_reason,
                profit_pips=profit_pips,
                result=result
            )
            signal.result = result
            
            self._trigger_callbacks('on_exit', signal)
            self._trigger_callbacks('on_result', signal)
            self._move_to_completed(signal)
            self._save_signals()
            
            print(f"🏁 Signal {signal_id}: EXIT {result.value.upper()} @ {exit_price} ({exit_reason})")
        
        return result
    
    def update_signal(self, signal_id: str, current_price: float, current_time: datetime) -> None:
        """
        Update active signal with current price.
        Records price movement during trade.
        """
        if signal_id not in self.active_signals:
            return
        
        signal = self.active_signals[signal_id]
        
        if signal.status != SignalStatus.ACTIVE:
            return
        
        # Record price update (throttle to avoid too many updates)
        if signal.last_update_time:
            time_since_last = (current_time - signal.last_update_time).total_seconds()
            if time_since_last < 30:  # Max 1 update per 30 seconds
                return
        
        signal.updates.append({
            'time': current_time.isoformat(),
            'price': current_price,
            'event': 'price_update'
        })
        # Limit updates list
        if len(signal.updates) > self.MAX_UPDATES_PER_SIGNAL:
            signal.updates = signal.updates[-self.MAX_UPDATES_PER_SIGNAL:]
            
        signal.last_update_time = current_time
    
    def close_signal(self, signal_id: str, current_price: float, current_time: datetime, 
                     reason: str = "manual") -> None:
        """
        Manually close a signal.
        Status: ACTIVE → CLOSED
        """
        if signal_id not in self.active_signals:
            return
        
        signal = self.active_signals[signal_id]
        
        if signal.status not in [SignalStatus.ACTIVE, SignalStatus.PENDING]:
            return
        
        entry_price = signal.entry.entry_price if signal.entry else current_price
        
        # Determine result
        if signal.direction.upper() in ['CALL', 'BUY']:
            if current_price > entry_price:
                result = SignalResult.WIN
            elif current_price < entry_price:
                result = SignalResult.LOSS
            else:
                result = SignalResult.BREAKEVEN
        else:
            if current_price < entry_price:
                result = SignalResult.WIN
            elif current_price > entry_price:
                result = SignalResult.LOSS
            else:
                result = SignalResult.BREAKEVEN
        
        profit_pips = abs(current_price - entry_price)
        if result == SignalResult.LOSS:
            profit_pips = -profit_pips
        
        signal.status = SignalStatus.CLOSED
        signal.result = result
        signal.exit = SignalExit(
            exit_price=current_price,
            exit_time=current_time,
            exit_reason=f"Manual close: {reason}",
            profit_pips=profit_pips,
            result=result
        )
        
        self._trigger_callbacks('on_exit', signal)
        self._trigger_callbacks('on_result', signal)
        self._move_to_completed(signal)
        self._save_signals()
        
        print(f"🏁 Signal {signal_id}: CLOSED {result.value.upper()} @ {current_price} (Manual)")
    
    def process_all_active(self, current_price: float, current_time: datetime) -> List[Signal]:
        """
        Process all active signals - check entries and exits.
        Returns list of signals that closed this cycle.
        """
        closed_signals = []
        
        for signal_id in list(self.active_signals.keys()):
            signal = self.active_signals.get(signal_id)
            if not signal:
                continue
            
            # Check entry if pending
            if signal.status == SignalStatus.PENDING:
                self.check_entry(signal_id, current_price, current_time)
            
            # Check exit if active
            elif signal.status == SignalStatus.ACTIVE:
                result = self.check_exit(signal_id, current_price, current_time)
                if result:
                    closed_signals.append(signal)
                else:
                    self.update_signal(signal_id, current_price, current_time)
        
        return closed_signals
    
    def _move_to_completed(self, signal: Signal) -> None:
        """Move signal from active to completed."""
        if signal.id in self.active_signals:
            del self.active_signals[signal.id]
        signal.status = SignalStatus.END
        self.completed_signals.append(signal)
    
    def register_callback(self, event: str, callback: Callable) -> None:
        """Register a callback for signal events."""
        if event in self._callbacks:
            self._callbacks[event].append(callback)
    
    def _trigger_callbacks(self, event: str, signal: Signal) -> None:
        """Trigger all callbacks for an event."""
        for callback in self._callbacks.get(event, []):
            try:
                callback(signal)
            except Exception as e:
                print(f"Callback error: {e}")
    
    def get_active_signals(self) -> List[Signal]:
        """Get all currently active signals."""
        return list(self.active_signals.values())
    
    def get_signal(self, signal_id: str) -> Optional[Signal]:
        """Get a specific signal by ID."""
        return self.signals.get(signal_id)
    
    def get_statistics(self) -> Dict:
        """Get signal statistics with confidence buckets."""
        total = len(self.completed_signals)
        wins = sum(1 for s in self.completed_signals if s.result == SignalResult.WIN)
        losses = sum(1 for s in self.completed_signals if s.result == SignalResult.LOSS)
        
        # Confidence buckets as expected by signal_list.py
        buckets = {
            '>=80': {'total': 0, 'wins': 0, 'win_rate': 0.0},
            '70-79': {'total': 0, 'wins': 0, 'win_rate': 0.0},
            '60-69': {'total': 0, 'wins': 0, 'win_rate': 0.0}
        }
        
        for sig in self.completed_signals:
            conf = sig.confidence
            is_win = 1 if sig.result == SignalResult.WIN else 0
            
            if conf >= 80:
                buckets['>=80']['total'] += 1
                buckets['>=80']['wins'] += is_win
            elif 70 <= conf <= 79:
                buckets['70-79']['total'] += 1
                buckets['70-79']['wins'] += is_win
            elif 60 <= conf <= 69:
                buckets['60-69']['total'] += 1
                buckets['60-69']['wins'] += is_win
        
        for k in buckets:
            if buckets[k]['total'] > 0:
                buckets[k]['win_rate'] = (buckets[k]['wins'] / buckets[k]['total']) * 100

        return {
            'total_trades': total,
            'wins': wins,
            'losses': losses,
            'win_rate': (wins / total * 100) if total > 0 else 0,
            'active_count': len(self.active_signals),
            'total_signals': len(self.signals),
            'confidence_buckets': buckets
        }


# Global manager instance
_signal_manager: Optional[SmartSignalManager] = None


def get_signal_manager() -> SmartSignalManager:
    """Get or create the global signal manager."""
    global _signal_manager
    if _signal_manager is None:
        _signal_manager = SmartSignalManager()
    return _signal_manager


def create_signal(
    pair: str,
    direction: str,
    signal_time: datetime,
    expiry_time: datetime,
    entry_price: float,
    stop_loss: float,
    take_profit: float,
    confidence: int = 0,
    **kwargs
) -> Signal:
    """Convenience function to create a signal."""
    manager = get_signal_manager()
    return manager.create_signal(
        pair=pair,
        direction=direction,
        signal_time=signal_time,
        expiry_time=expiry_time,
        entry_price=entry_price,
        stop_loss=stop_loss,
        take_profit=take_profit,
        confidence=confidence,
        **kwargs
    )


def process_signals(current_price: float, current_time: Optional[datetime] = None) -> List[Signal]:
    """Convenience function to process all active signals."""
    manager = get_signal_manager()
    if current_time is None:
        current_time = datetime.now()
    return manager.process_all_active(current_price, current_time)


def get_signal_stats() -> Dict:
    """Get signal statistics."""
    manager = get_signal_manager()
    return manager.get_statistics()
