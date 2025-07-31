"""
Local caching system for flight data using Parquet format
"""
import pandas as pd
import os
from pathlib import Path
from datetime import datetime, timedelta
import json


class FlightCache:
    """Manages local Parquet cache for flight data"""
    
    def __init__(self, cache_dir: str = "flight_cache"):
        """
        Initialize cache manager
        
        Args:
            cache_dir: Directory for storing cached data
        """
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        
        # Subdirectories for different data types
        self.opensky_dir = self.cache_dir / "opensky"
        self.adsb_dir = self.cache_dir / "adsb" 
        self.analysis_dir = self.cache_dir / "analysis"
        
        for dir_path in [self.opensky_dir, self.adsb_dir, self.analysis_dir]:
            dir_path.mkdir(exist_ok=True)
    
    def save_opensky_data(self, df: pd.DataFrame, data_type: str = "current") -> str:
        """
        Save OpenSky data to cache
        
        Args:
            df: DataFrame with flight data
            data_type: Type of data ("current" or "historical")
            
        Returns:
            Path to saved file
        """
        if df.empty:
            print("No data to cache")
            return None
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"opensky_{data_type}_{timestamp}.parquet"
        filepath = self.opensky_dir / filename
        
        # Add metadata
        df_with_meta = df.copy()
        df_with_meta['cache_timestamp'] = datetime.now()
        df_with_meta['data_source'] = 'opensky'
        df_with_meta['data_type'] = data_type
        
        df_with_meta.to_parquet(filepath, index=False)
        print(f"Cached {len(df)} records to {filepath}")
        
        # Save metadata file
        metadata = {
            'filename': filename,
            'timestamp': timestamp,
            'data_type': data_type,
            'record_count': len(df),
            'columns': list(df.columns),
            'date_range': {
                'min': df['data_time'].min().isoformat() if 'data_time' in df.columns else None,
                'max': df['data_time'].max().isoformat() if 'data_time' in df.columns else None
            }
        }
        
        metadata_file = self.opensky_dir / f"{filename}.meta.json"
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        return str(filepath)
    
    def load_opensky_data(self, hours_back: int = 24) -> pd.DataFrame:
        """
        Load recent OpenSky data from cache
        
        Args:
            hours_back: Load data from last N hours
            
        Returns:
            Combined DataFrame of cached data
        """
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        parquet_files = list(self.opensky_dir.glob("*.parquet"))
        recent_files = []
        
        for file in parquet_files:
            # Extract timestamp from filename
            try:
                timestamp_str = file.stem.split('_')[-2] + '_' + file.stem.split('_')[-1]
                file_time = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                if file_time >= cutoff_time:
                    recent_files.append(file)
            except (ValueError, IndexError):
                continue
        
        if not recent_files:
            print(f"No cached data found within {hours_back} hours")
            return pd.DataFrame()
        
        # Load and combine data
        dfs = []
        for file in recent_files:
            try:
                df = pd.read_parquet(file)
                dfs.append(df)
            except Exception as e:
                print(f"Error loading {file}: {e}")
                continue
        
        if not dfs:
            return pd.DataFrame()
            
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Remove duplicates based on icao24 and timestamp
        if 'icao24' in combined_df.columns and 'data_time' in combined_df.columns:
            combined_df = combined_df.drop_duplicates(subset=['icao24', 'data_time'])
        
        print(f"Loaded {len(combined_df)} records from {len(recent_files)} cache files")
        return combined_df
    
    def get_cache_stats(self) -> dict:
        """Get statistics about cached data"""
        stats = {
            'opensky_files': len(list(self.opensky_dir.glob("*.parquet"))),
            'adsb_files': len(list(self.adsb_dir.glob("*.parquet"))),
            'total_size_mb': 0,
            'oldest_file': None,
            'newest_file': None
        }
        
        all_files = list(self.cache_dir.rglob("*.parquet"))
        
        if all_files:
            # Calculate total size
            total_size = sum(f.stat().st_size for f in all_files)
            stats['total_size_mb'] = round(total_size / (1024 * 1024), 2)
            
            # Find oldest and newest files
            file_times = []
            for f in all_files:
                try:
                    timestamp_str = f.stem.split('_')[-2] + '_' + f.stem.split('_')[-1]
                    file_time = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                    file_times.append((file_time, f.name))
                except (ValueError, IndexError):
                    continue
            
            if file_times:
                file_times.sort()
                stats['oldest_file'] = file_times[0][1]
                stats['newest_file'] = file_times[-1][1]
        
        return stats
    
    def cleanup_old_files(self, days_old: int = 7):
        """Remove cache files older than specified days"""
        cutoff_time = datetime.now() - timedelta(days=days_old)
        
        all_files = list(self.cache_dir.rglob("*.parquet")) + list(self.cache_dir.rglob("*.json"))
        removed_count = 0
        
        for file in all_files:
            try:
                # Try to extract timestamp from filename
                if '_' in file.stem:
                    timestamp_str = file.stem.split('_')[-2] + '_' + file.stem.split('_')[-1]
                    timestamp_str = timestamp_str.replace('.meta', '')  # Handle metadata files
                    file_time = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                    
                    if file_time < cutoff_time:
                        file.unlink()
                        removed_count += 1
                        print(f"Removed old file: {file.name}")
            except (ValueError, IndexError):
                # If we can't parse the timestamp, check file modification time
                if datetime.fromtimestamp(file.stat().st_mtime) < cutoff_time:
                    file.unlink()
                    removed_count += 1
                    print(f"Removed old file: {file.name}")
                continue
        
        print(f"Cleaned up {removed_count} old files")
        return removed_count


if __name__ == "__main__":
    # Test the cache manager
    cache = FlightCache()
    
    print("Cache statistics:")
    stats = cache.get_cache_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Test with sample data
    sample_data = pd.DataFrame({
        'icao24': ['test123'],
        'callsign': ['TEST'],
        'data_time': [datetime.now()]
    })
    
    print("\nTesting cache save...")
    cache.save_opensky_data(sample_data, "test")
    
    print("\nUpdated cache statistics:")
    stats = cache.get_cache_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")