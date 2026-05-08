"""
Data Compression and Storage Utilities
Optimizes JSON storage and data compression for efficient memory usage.
"""

import json
import gzip
import os
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date
from pathlib import Path


class CompressedJsonStorage:
    """
    Handles compressed JSON storage for efficient persistence.
    Automatically compresses large JSON files using gzip.
    """
    
    COMPRESSION_THRESHOLD = 10000  # Compress files larger than 10KB
    
    @staticmethod
    def _serialize_value(obj: Any) -> Any:
        """Convert non-JSON-serializable objects to serializable form."""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif isinstance(obj, set):
            return list(obj)
        elif hasattr(obj, '__dict__'):
            return obj.__dict__
        return obj
    
    @staticmethod
    def save(filepath: str, data: Union[Dict, List], compress: bool = True) -> bool:
        """
        Save data to JSON file, optionally compressing with gzip.
        
        Args:
            filepath: Path to save file
            data: Data to save (dict or list)
            compress: Whether to use gzip compression
        
        Returns:
            True if successful, False otherwise
        """
        try:
            json_str = json.dumps(
                data,
                default=CompressedJsonStorage._serialize_value,
                indent=2 if not compress else None
            )
            
            json_bytes = json_str.encode('utf-8')
            
            # Decide whether to compress
            should_compress = compress and len(json_bytes) > CompressedJsonStorage.COMPRESSION_THRESHOLD
            
            if should_compress:
                # Compress with gzip
                with gzip.open(f"{filepath}.gz", 'wb') as f:
                    f.write(json_bytes)
                
                # Remove uncompressed version if it exists
                if os.path.exists(filepath):
                    os.remove(filepath)
            else:
                # Save uncompressed
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(json_str)
                
                # Remove compressed version if it exists
                if os.path.exists(f"{filepath}.gz"):
                    os.remove(f"{filepath}.gz")
            
            return True
        
        except Exception as e:
            print(f"[Compression] Error saving {filepath}: {e}")
            return False
    
    @staticmethod
    def load(filepath: str) -> Optional[Union[Dict, List]]:
        """
        Load JSON data from file, handling both compressed and uncompressed formats.
        
        Args:
            filepath: Path to load file
        
        Returns:
            Loaded data or None if load fails
        """
        try:
            # Try compressed version first
            if os.path.exists(f"{filepath}.gz"):
                with gzip.open(f"{filepath}.gz", 'rb') as f:
                    json_bytes = f.read()
                    json_str = json_bytes.decode('utf-8')
                    return json.loads(json_str)
            
            # Try uncompressed version
            if os.path.exists(filepath):
                with open(filepath, 'r', encoding='utf-8') as f:
                    return json.load(f)
            
            return None
        
        except Exception as e:
            print(f"[Compression] Error loading {filepath}: {e}")
            return None
    
    @staticmethod
    def get_file_size(filepath: str) -> int:
        """Get file size in bytes, checking both compressed and uncompressed."""
        total_size = 0
        
        if os.path.exists(filepath):
            total_size += os.path.getsize(filepath)
        
        if os.path.exists(f"{filepath}.gz"):
            total_size += os.path.getsize(f"{filepath}.gz")
        
        return total_size


class TradeHistoryCompressor:
    """
    Compresses trade history by keeping only recent 200-500 trades in active storage.
    Archives older trades to compressed files.
    """
    
    ACTIVE_TRADE_LIMIT = 500
    ARCHIVE_TRADE_LIMIT = 1000
    
    @staticmethod
    def compress_trades(trades: List[Dict]) -> List[Dict]:
        """
        Keep only recent trades in active storage.
        
        Args:
            trades: List of trade records
        
        Returns:
            Compressed trades list (200-500 most recent)
        """
        if len(trades) <= TradeHistoryCompressor.ACTIVE_TRADE_LIMIT:
            return trades
        
        # Keep only the most recent ACTIVE_TRADE_LIMIT trades
        return trades[-TradeHistoryCompressor.ACTIVE_TRADE_LIMIT:]
    
    @staticmethod
    def archive_trades(trades: List[Dict], archive_path: str) -> Tuple[List[Dict], int]:
        """
        Archive old trades to compressed file.
        
        Args:
            trades: List of trade records
            archive_path: Path to archive file
        
        Returns:
            (remaining_trades, archived_count)
        """
        if len(trades) <= TradeHistoryCompressor.ACTIVE_TRADE_LIMIT:
            return trades, 0
        
        # Split trades
        archive_trades = trades[:-TradeHistoryCompressor.ACTIVE_TRADE_LIMIT]
        active_trades = trades[-TradeHistoryCompressor.ACTIVE_TRADE_LIMIT:]
        
        # Archive old trades
        try:
            existing_archive = CompressedJsonStorage.load(archive_path) or []
            if isinstance(existing_archive, list):
                all_archived = existing_archive + archive_trades
            else:
                all_archived = archive_trades
            
            CompressedJsonStorage.save(archive_path, all_archived, compress=True)
            return active_trades, len(archive_trades)
        
        except Exception as e:
            print(f"[TradeHistoryCompressor] Error archiving: {e}")
            return trades, 0


class CandleDataCompressor:
    """
    Optimizes candle data storage by keeping only recent 2000-3000 candles.
    """
    
    MIN_CANDLES = 2000
    MAX_CANDLES = 3000
    
    @staticmethod
    def compress_candles(df) -> object:
        """
        Compress candle dataframe to optimal size.
        
        Args:
            df: Candle dataframe
        
        Returns:
            Compressed dataframe (2000-3000 candles)
        """
        if df is None or len(df) == 0:
            return df
        
        if len(df) <= CandleDataCompressor.MAX_CANDLES:
            return df
        
        # Keep only most recent candles
        return df.tail(CandleDataCompressor.MAX_CANDLES).reset_index(drop=True)
    
    @staticmethod
    def get_compression_stats(df) -> Dict[str, Any]:
        """Get compression statistics for a dataframe."""
        if df is None:
            return {'candles': 0, 'compressed': False, 'memory_mb': 0}
        
        candle_count = len(df)
        needs_compression = candle_count > CandleDataCompressor.MAX_CANDLES
        
        # Estimate memory usage
        memory_bytes = df.memory_usage(deep=True).sum() if hasattr(df, 'memory_usage') else 0
        memory_mb = memory_bytes / (1024 * 1024)
        
        return {
            'candles': candle_count,
            'compressed': needs_compression,
            'memory_mb': round(memory_mb, 2),
            'compression_needed': needs_compression,
            'target_candles': f"{CandleDataCompressor.MIN_CANDLES}-{CandleDataCompressor.MAX_CANDLES}"
        }


class SignalDataCompressor:
    """
    Compresses signal data for efficient storage.
    """
    
    @staticmethod
    def minify_signal(signal: Dict) -> Dict:
        """
        Minify signal data by removing redundant fields.
        
        Args:
            signal: Signal dictionary
        
        Returns:
            Minified signal
        """
        minified = {
            'id': signal.get('id'),
            't': signal.get('time') or signal.get('signal_time'),  # time
            'd': signal.get('direction'),  # direction
            'c': signal.get('confidence'),  # confidence
            'r': signal.get('result'),  # result
            'p': signal.get('profit_pips'),  # profit_pips
        }
        
        # Remove None values
        return {k: v for k, v in minified.items() if v is not None}
    
    @staticmethod
    def minify_signals(signals: List[Dict]) -> List[Dict]:
        """Minify a list of signals."""
        return [SignalDataCompressor.minify_signal(s) for s in signals]
    
    @staticmethod
    def expand_signal(minified: Dict) -> Dict:
        """Expand minified signal back to full form."""
        return {
            'id': minified.get('id'),
            'time': minified.get('t'),
            'direction': minified.get('d'),
            'confidence': minified.get('c'),
            'result': minified.get('r'),
            'profit_pips': minified.get('p'),
        }


class StorageOptimizer:
    """
    Comprehensive storage optimization manager.
    """
    
    @staticmethod
    def optimize_risk_state(state_data: Dict) -> Dict:
        """Optimize risk management state for storage."""
        if not state_data:
            return state_data
        
        optimized = state_data.copy()
        
        # Keep only recent daily stats (90 days)
        if 'daily_stats' in optimized:
            from datetime import datetime, timedelta
            cutoff_date = (datetime.now().date() - timedelta(days=90)).isoformat()
            optimized['daily_stats'] = [
                s for s in optimized['daily_stats']
                if s.get('date', '') >= cutoff_date
            ]
        
        return optimized
    
    @staticmethod
    def get_storage_report() -> Dict[str, Any]:
        """Generate storage usage report."""
        report = {
            'timestamp': datetime.now().isoformat(),
            'files': {},
            'total_size_mb': 0,
        }
        
        # Check common state files
        state_files = [
            'risk_management_state.json',
            'day_reset_state.json',
            'daily_signals_state.json',
            'signal_manager_state.json',
        ]
        
        for filename in state_files:
            size = CompressedJsonStorage.get_file_size(filename)
            report['files'][filename] = {
                'size_kb': round(size / 1024, 2),
                'compressed': os.path.exists(f"{filename}.gz")
            }
            report['total_size_mb'] += size
        
        report['total_size_mb'] = round(report['total_size_mb'] / (1024 * 1024), 2)
        
        return report


from typing import Tuple

# Re-export for convenience
__all__ = [
    'CompressedJsonStorage',
    'TradeHistoryCompressor',
    'CandleDataCompressor',
    'SignalDataCompressor',
    'StorageOptimizer',
]
