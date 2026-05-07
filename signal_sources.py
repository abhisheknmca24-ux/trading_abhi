import pandas as pd
from typing import List, Dict, Optional

# Signal source priorities
PRIORITIES = {
    "internal_generator": 3,  # HIGH
    "manual_signal_input": 3,  # HIGH
    "telegram_provider": 2,    # MEDIUM
}


class SignalSourceManager:
    def __init__(self):
        # Active signals stored as list of dicts
        self.active_signals: List[Dict] = []

    def _make_key(self, time, direction):
        # Normalize time to ISO minute resolution if timestamp-like
        if isinstance(time, (pd.Timestamp,)):
            t = time.floor("min")
        else:
            try:
                t = pd.to_datetime(time).floor("min")
            except Exception:
                t = str(time)

        return f"{t}_{direction}"

    def add_signal(self, *, time, direction: str, confidence: float, source: str, meta: Optional[Dict] = None):
        """Add a signal from a source into the active queue.

        - Prevents exact duplicates (same time+direction+source).
        - Merges same signal coming from different sources (merged confidence bonus).
        - Resolves opposite-direction conflicts occurring at same minute.
        """
        if meta is None:
            meta = {}

        direction = direction.upper()
        sig_time = pd.to_datetime(time)
        key = self._make_key(sig_time, direction)
        priority = PRIORITIES.get(source, 1)

        # Duplicate detection: exact same key and source
        for s in self.active_signals:
            if s["signal_key"] == key and s["source"] == source:
                # duplicate spam — ignore
                return None

        # If same signal (time+direction) exists from other source(s) -> merge
        for s in list(self.active_signals):
            if s["signal_key"] == key and s["direction"] == direction:
                # Merge sources and boost confidence slightly
                merged_sources = list(set([s["source"], source]))
                avg_conf = (s["confidence"] + confidence) / 2.0
                # Add small bonus for corroboration
                merged_conf = min(100.0, round(avg_conf + 5.0, 2))
                s["confidence"] = merged_conf
                s["source"] = ",".join(sorted(merged_sources))
                # keep highest priority
                s["priority"] = max(s.get("priority", 1), priority)
                s.setdefault("meta", {}).update(meta)
                return s

        # Opposite-direction conflict resolver: same minute, opposite direction
        opposite_key = self._make_key(sig_time, "CALL" if direction == "PUT" else "PUT")
        for s in list(self.active_signals):
            if s["signal_key"] == opposite_key:
                # Found conflict at same minute
                # Resolve by priority first, then confidence; if tie, skip both
                if priority > s.get("priority", 1):
                    # replace the existing lower-priority signal with this one
                    self.active_signals = [x for x in self.active_signals if x["signal_key"] != opposite_key]
                    break
                elif priority < s.get("priority", 1):
                    # keep existing, drop this incoming
                    return None
                else:
                    # equal priority -> compare confidence
                    if confidence > s.get("confidence", 0):
                        # remove existing lower-confidence signal
                        self.active_signals = [x for x in self.active_signals if x["signal_key"] != opposite_key]
                        break
                    elif confidence < s.get("confidence", 0):
                        return None
                    else:
                        # exact tie -> skip both
                        self.active_signals = [x for x in self.active_signals if x["signal_key"] != opposite_key]
                        return None

        # If we reached here, safe to append new signal
        signal = {
            "signal_key": key,
            "time": sig_time,
            "direction": direction,
            "confidence": float(confidence),
            "source": source,
            "priority": priority,
            "meta": meta,
            "status": "active",
        }

        self.active_signals.append(signal)
        return signal

    def get_active_signals(self) -> List[Dict]:
        return list(self.active_signals)

    def pop_next_signal(self) -> Optional[Dict]:
        if not self.active_signals:
            return None
        # pop highest priority then highest confidence
        self.active_signals.sort(key=lambda s: (s.get("priority", 0), s.get("confidence", 0)), reverse=True)
        return self.active_signals.pop(0)

    def clear_signals(self):
        self.active_signals.clear()

    def remove_signal_by_key(self, signal_key: str):
        self.active_signals = [s for s in self.active_signals if s["signal_key"] != signal_key]


# Module-level default manager for convenient imports
default_manager = SignalSourceManager()

__all__ = ["SignalSourceManager", "default_manager"]
