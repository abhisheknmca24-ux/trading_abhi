"""
Major Forex News Filter
Blocks trading 15 minutes before/after high-impact news events.
Prevents trading during FOMC, CPI, NFP, ECB, and Interest Rate releases.
"""

from datetime import datetime, timedelta, time
from typing import Optional, List, Dict, Tuple
import json
import os

try:
    from zoneinfo import ZoneInfo
    _IST = ZoneInfo("Asia/Kolkata")
    _UTC = ZoneInfo("UTC")
except ImportError:
    _IST = None
    _UTC = None

# Blackout window around major news (minutes)
NEWS_BLACKOUT_MINUTES = 15

# Known upcoming major news events — format: (IST time string "HH:MM", event name)
# These are approximate recurrence patterns. The bot will also load a dynamic file.
STATIC_WEEKLY_SCHEDULE: List[Dict] = [
    # NFP — first Friday of each month, ~18:30 IST
    {"weekday": 4, "hour": 18, "minute": 30, "name": "NFP (Non-Farm Payrolls)", "recurring": "first_friday"},
    # FOMC — various; typically Wednesday ~23:00 IST
    {"weekday": 2, "hour": 23, "minute": 0, "name": "FOMC Statement", "recurring": "bimonthly"},
    # CPI (US) — typically Tuesday/Wednesday ~18:30 IST
    {"weekday": 1, "hour": 18, "minute": 30, "name": "US CPI", "recurring": "monthly"},
    # ECB — typically Thursday ~17:15 IST
    {"weekday": 3, "hour": 17, "minute": 15, "name": "ECB Rate Decision", "recurring": "bimonthly"},
    # US Interest Rate Decision — aligned with FOMC
    {"weekday": 2, "hour": 23, "minute": 30, "name": "US Interest Rate Decision", "recurring": "bimonthly"},
]

# Dynamic override file: list of {"datetime_ist": "YYYY-MM-DD HH:MM", "name": "..."}
DYNAMIC_NEWS_FILE = "news_events.json"


def _now_ist() -> datetime:
    """Get current time in IST."""
    if _IST:
        return datetime.now(_IST)
    return datetime.now()


def _load_dynamic_events() -> List[Dict]:
    """Load upcoming news events from the dynamic override file."""
    if not os.path.exists(DYNAMIC_NEWS_FILE):
        return []
    try:
        with open(DYNAMIC_NEWS_FILE, "r") as f:
            return json.load(f)
    except Exception:
        return []


def _save_dynamic_events(events: List[Dict]) -> None:
    """Save dynamic events to file."""
    try:
        with open(DYNAMIC_NEWS_FILE, "w") as f:
            json.dump(events, f, indent=2)
    except Exception:
        pass


def add_news_event(event_datetime_ist: str, event_name: str) -> None:
    """
    Add a specific upcoming news event.
    event_datetime_ist: "YYYY-MM-DD HH:MM" in IST
    event_name: descriptive name like "US CPI"
    """
    events = _load_dynamic_events()
    events.append({
        "datetime_ist": event_datetime_ist,
        "name": event_name
    })
    _save_dynamic_events(events)


def _clean_old_dynamic_events() -> None:
    """Remove past events from the dynamic file."""
    now = _now_ist()
    events = _load_dynamic_events()
    active = []
    for e in events:
        try:
            dt_str = e.get("datetime_ist", "")
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
            if _IST:
                dt = dt.replace(tzinfo=_IST)
            if dt + timedelta(hours=2) >= now:
                active.append(e)
        except Exception:
            continue
    _save_dynamic_events(active)


def _get_active_dynamic_events(now: datetime, window_minutes: int) -> List[str]:
    """Check dynamic events for active blackout."""
    events = _load_dynamic_events()
    blackout_names = []
    window = timedelta(minutes=window_minutes)

    for e in events:
        try:
            dt_str = e.get("datetime_ist", "")
            dt = datetime.strptime(dt_str, "%Y-%m-%d %H:%M")
            if _IST:
                dt = dt.replace(tzinfo=_IST)
            if abs((now - dt).total_seconds()) <= window.total_seconds():
                blackout_names.append(e.get("name", "Unknown News"))
        except Exception:
            continue
    return blackout_names


def _is_first_friday_of_month(now: datetime) -> bool:
    """Check if today is the first Friday of the month."""
    if now.weekday() != 4:  # Not Friday
        return False
    return now.day <= 7


def _get_active_static_events(now: datetime, window_minutes: int) -> List[str]:
    """Check static schedule for active blackout based on current time."""
    blackout_names = []
    window = timedelta(minutes=window_minutes)
    now_naive = now.replace(tzinfo=None)

    for rule in STATIC_WEEKLY_SCHEDULE:
        weekday = rule["weekday"]
        hour = rule["hour"]
        minute = rule["minute"]
        name = rule["name"]
        recurring = rule.get("recurring", "weekly")

        # Only check if it's the right weekday
        if now.weekday() != weekday:
            continue

        # Create event time for today
        event_time = now_naive.replace(hour=hour, minute=minute, second=0, microsecond=0)

        # Check if within window
        if abs((now_naive - event_time).total_seconds()) <= window.total_seconds():
            # Additional recurrence checks
            if recurring == "first_friday" and not _is_first_friday_of_month(now):
                continue  # NFP only on first Friday
            blackout_names.append(name)

    return blackout_names


def is_news_blackout(now: Optional[datetime] = None) -> Tuple[bool, str]:
    """
    Check if current time is within a major news blackout window.

    Returns:
        (is_blackout, reason_message)
    """
    if now is None:
        now = _now_ist()

    active_events: List[str] = []

    # Check dynamic events (specific dates loaded from file)
    dynamic = _get_active_dynamic_events(now, NEWS_BLACKOUT_MINUTES)
    active_events.extend(dynamic)

    # Check static recurring schedule
    static = _get_active_static_events(now, NEWS_BLACKOUT_MINUTES)
    active_events.extend(static)

    if active_events:
        event_list = " | ".join(active_events)
        return True, f"⚠️ NEWS BLACKOUT: {event_list} (±{NEWS_BLACKOUT_MINUTES} min window)"

    return False, ""


def get_news_warning_message(reason: str) -> str:
    """Format a Telegram-ready news warning message."""
    return (
        f"🚨 *News Blackout Active*\n\n"
        f"{reason}\n\n"
        f"⛔ Trading suspended for safety.\n"
        f"Will resume automatically after event window."
    )


def cleanup_old_events() -> None:
    """Periodic cleanup of stale dynamic events."""
    _clean_old_dynamic_events()
