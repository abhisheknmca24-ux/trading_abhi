from __future__ import annotations

from dataclasses import dataclass, field
from collections import deque
from typing import Optional

import pandas as pd


@dataclass
class CandleCacheManager:
    cached_5m_dataframe: Optional[pd.DataFrame] = None
    cached_1m_dataframe: Optional[pd.DataFrame] = None
    processed_dataframe: Optional[pd.DataFrame] = None
    candle_keys: set[str] = field(default_factory=set)
    _ordered_candle_keys: deque[str] = field(default_factory=deque)
    max_candle_keys: int = 500

    def should_refresh(self, candle_key: str) -> bool:
        if candle_key in self.candle_keys:
            return False
        self.candle_keys.add(candle_key)
        self._ordered_candle_keys.append(candle_key)
        if len(self._ordered_candle_keys) > self.max_candle_keys:
            oldest_key = self._ordered_candle_keys.popleft()
            self.candle_keys.discard(oldest_key)
        return True

    def store_5m_dataframe(self, dataframe: Optional[pd.DataFrame]) -> None:
        self.cached_5m_dataframe = dataframe

    def store_1m_dataframe(self, dataframe: Optional[pd.DataFrame]) -> None:
        self.cached_1m_dataframe = dataframe

    def store_processed_dataframe(self, dataframe: Optional[pd.DataFrame]) -> None:
        self.processed_dataframe = dataframe

    def reset(self) -> None:
        self.cached_5m_dataframe = None
        self.cached_1m_dataframe = None
        self.processed_dataframe = None
        self.candle_keys.clear()
        self._ordered_candle_keys.clear()


cache_manager = CandleCacheManager()
