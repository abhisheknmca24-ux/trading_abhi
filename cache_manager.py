from logger import logger
import pandas as pd

class CacheManager:
    def __init__(self):
        self.cached_df = None
        self.cached_interval = None
        self.cached_candle_key = None
        
        self.cached_minute_df = None
        self.cached_minute_time = None
        
        self.processed_1min_df = None
        self.processed_5min_df = None

    def get_candle_key(self, interval):
        now = pd.Timestamp.now(tz="Asia/Kolkata")
        if interval == "1min":
            return now.floor("min")
        if interval == "5min":
            return now.floor("5min")
        return now.floor("min")

    def get_dataframe(self, interval, fetch_func):
        current_candle_key = self.get_candle_key(interval)
        
        if interval == "1min":
            if self.cached_minute_df is not None and self.cached_minute_time == current_candle_key:
                return self.cached_minute_df
            else:
                df = fetch_func(interval)
                if df is not None:
                    self.cached_minute_df = df
                    self.cached_minute_time = current_candle_key
                    # Reset only 1min processed cache
                    self.processed_1min_df = None
                return df
                
        else:
            if (
                self.cached_df is not None
                and self.cached_interval == interval
                and self.cached_candle_key == current_candle_key
            ):
                logger.debug(f"Using cached {interval} data")
                return self.cached_df
            else:
                df = fetch_func(interval)
                if df is not None:
                    self.cached_interval = interval
                    self.cached_candle_key = current_candle_key
                    self.cached_df = df
                    # Reset only 5min processed cache
                    self.processed_5min_df = None
                return df

    def get_processed_dataframe(self, interval, fetch_func, process_func):
        df = self.get_dataframe(interval, fetch_func)
        if df is None:
            return None
            
        if interval == "1min":
            if self.processed_1min_df is not None:
                return self.processed_1min_df
            # Indicators are safe to add in-place; removing .copy() to save RAM
            self.processed_1min_df = process_func(df)
            return self.processed_1min_df
        else:
            if self.processed_5min_df is not None:
                return self.processed_5min_df
            # Indicators are safe to add in-place; removing .copy() to save RAM
            self.processed_5min_df = process_func(df)
            return self.processed_5min_df

cache = CacheManager()
