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

    def cleanup_stale_cache(self):
        """Clear cache if older than 15 minutes to avoid indefinite RAM usage."""
        now = pd.Timestamp.now(tz="Asia/Kolkata")
        
        if self.cached_minute_time is not None:
            if (now - self.cached_minute_time).total_seconds() > 900:
                logger.debug("Clearing stale 1min cache (>15m)")
                self.cached_minute_df = None
                self.cached_minute_time = None
                self.processed_1min_df = None
                
        if self.cached_candle_key is not None:
            if (now - self.cached_candle_key).total_seconds() > 900:
                logger.debug("Clearing stale interval cache (>15m)")
                self.cached_df = None
                self.cached_candle_key = None
                self.cached_interval = None
                self.processed_5min_df = None

    def get_candle_key(self, interval):
        now = pd.Timestamp.now(tz="Asia/Kolkata")
        if interval == "1min":
            return now.floor("min")
        if interval == "5min":
            return now.floor("5min")
        return now.floor("min")

    def get_dataframe(self, interval, fetch_func):
        self.cleanup_stale_cache()
        current_candle_key = self.get_candle_key(interval)
        
        if interval == "1min":
            if self.cached_minute_df is not None and self.cached_minute_time == current_candle_key:
                logger.debug(f"Cache HIT for 1min | Key: {current_candle_key}")
                return self.cached_minute_df.copy(deep=True)
            else:
                logger.debug(f"Cache MISS for 1min | Key: {current_candle_key} | Refreshing from API")
                df = fetch_func(interval)
                if df is not None:
                    self.cached_minute_df = df.copy(deep=True)
                    self.cached_minute_time = current_candle_key
                    # Reset only 1min processed cache
                    logger.debug("Resetting processed 1min cache due to fresh candle data")
                    self.processed_1min_df = None
                return df.copy(deep=True) if df is not None else None
                
        else:
            if (
                self.cached_df is not None
                and self.cached_interval == interval
                and self.cached_candle_key == current_candle_key
            ):
                logger.debug(f"Cache HIT for {interval} | Key: {current_candle_key}")
                return self.cached_df.copy(deep=True)
            else:
                logger.debug(f"Cache MISS for {interval} | Key: {current_candle_key} | Refreshing from API")
                df = fetch_func(interval)
                if df is not None:
                    self.cached_interval = interval
                    self.cached_candle_key = current_candle_key
                    self.cached_df = df.copy(deep=True)
                    # Reset only 5min processed cache
                    logger.debug(f"Resetting processed {interval} cache due to fresh candle data")
                    self.processed_5min_df = None
                return df.copy(deep=True) if df is not None else None

    def get_processed_dataframe(self, interval, fetch_func, process_func):
        df = self.get_dataframe(interval, fetch_func)
        if df is None:
            return None
            
        if interval == "1min":
            if self.processed_1min_df is not None:
                logger.debug("Processed cache HIT for 1min")
                return self.processed_1min_df.copy(deep=True)
            # Use deep copy to protect raw cache
            self.processed_1min_df = process_func(df.copy(deep=True))
            return self.processed_1min_df.copy(deep=True)
        else:
            if self.processed_5min_df is not None:
                logger.debug(f"Processed cache HIT for {interval}")
                return self.processed_5min_df.copy(deep=True)
            # Use deep copy to protect raw cache
            self.processed_5min_df = process_func(df.copy(deep=True))
            return self.processed_5min_df.copy(deep=True)

cache = CacheManager()

