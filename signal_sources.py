import re
from datetime import datetime
from typing import List, Dict, Optional
try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

TIMEZONE_NAME = "Asia/Kolkata"
SIGNAL_PATTERN = re.compile(r"^(?P<hour>\d{2}):(?P<minute>\d{2})\s+EURUSD\s+(?P<signal>CALL|PUT|SKIP)(?:\s+BOOST=(?P<boost>\d+))?(?:\s+SKIP=(?P<skip>TRUE|FALSE))?$", re.IGNORECASE)

class SignalSource:
    INTERNAL = "INTERNAL"
    MANUAL = "MANUAL"
    EXTERNAL = "EXTERNAL"

    @classmethod
    def get_priority(cls, source: str) -> int:
        if source in [cls.INTERNAL, cls.MANUAL]:
            return 2 # HIGH
        return 1 # MEDIUM

class SignalManager:
    def __init__(self):
        # Maps time_key (HH:MM) to another dict of directions and their sources
        # e.g. {"15:20": {"CALL": {"EXTERNAL": {"priority": 1, "boost": 0, "skip": False}}, "PUT": {}, "SKIP": {}}}
        self.raw_signals = {}
        self.last_date = None

    def _now(self):
        tz = ZoneInfo(TIMEZONE_NAME) if ZoneInfo else None
        return datetime.now(tz) if tz else datetime.now()

    def _reset_if_new_day(self):
        current_date = self._now().date()
        if self.last_date != current_date:
            self.raw_signals = {}
            self.last_date = current_date

    def add_signals(self, signal_lines: List[str], source: str):
        self._reset_if_new_day()
        for line in signal_lines:
            line = line.strip().upper()
            match = SIGNAL_PATTERN.match(line)
            if not match:
                continue
            
            hour = match.group("hour")
            minute = match.group("minute")
            direction = match.group("signal")
            
            boost_str = match.group("boost")
            skip_str = match.group("skip")
            
            boost_val = int(boost_str) if boost_str else 0
            skip_val = True if skip_str == "TRUE" else False
            
            time_key = f"{hour}:{minute}"
            
            if time_key not in self.raw_signals:
                self.raw_signals[time_key] = {"CALL": {}, "PUT": {}, "SKIP": {}}
                
            self.raw_signals[time_key][direction][source] = {
                "priority": SignalSource.get_priority(source),
                "boost": boost_val,
                "skip": skip_val
            }

    def get_merged_signals(self) -> List[dict]:
        """
        Returns merged signals with conflict resolution, duplicate removal, and confidence boosting.
        """
        self._reset_if_new_day()
        merged = []
        for time_key, directions in self.raw_signals.items():
            call_sources = directions.get("CALL", {})
            put_sources = directions.get("PUT", {})
            skip_sources = directions.get("SKIP", {})
            
            # Check explicit skip or skip flag on any source
            explicit_skip = bool(skip_sources)
            for src_dict in list(call_sources.values()) + list(put_sources.values()):
                if src_dict.get("skip"):
                    explicit_skip = True
                    break
            
            # Conflict resolution: opposite signals
            conflict = bool(call_sources and put_sources)
            
            if explicit_skip or conflict:
                merged.append({
                    "line": f"{time_key} EURUSD SKIP",
                    "direction": "SKIP",
                    "boost": 0,
                    "skip": True
                })
                continue
                
            active_dir = "CALL" if call_sources else ("PUT" if put_sources else None)
            if not active_dir:
                continue
                
            sources = call_sources if active_dir == "CALL" else put_sources
            
            # Calculate total boost
            boost = sum(src_dict.get("boost", 0) for src_dict in sources.values())
            
            # If multiple sources agree, add an extra 10 boost
            if len(sources) > 1:
                boost += 10
            
            merged.append({
                "line": f"{time_key} EURUSD {active_dir}",
                "direction": active_dir,
                "boost": boost,
                "skip": False
            })
            
        # Return sorted by time
        return sorted(merged, key=lambda x: x["line"])

# Global manager instance
global_signal_manager = SignalManager()
