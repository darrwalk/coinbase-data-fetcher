"""Configuration for Coinbase data fetcher."""

import os
from pathlib import Path


class Config:
    """Configuration for the Coinbase data fetcher."""
    
    def __init__(self):
        self._cache_path = os.environ.get('COINBASE_CACHE_PATH', './var/coinbase_data/')
    
    @property
    def cache_path(self) -> str:
        """Get the cache path for storing fetched data."""
        return self._cache_path
    
    @cache_path.setter
    def cache_path(self, value: str):
        """Set the cache path."""
        self._cache_path = value
    
    def ensure_cache_dir(self) -> Path:
        """Ensure the cache directory exists and return Path object."""
        path = Path(self._cache_path)
        path.mkdir(parents=True, exist_ok=True)
        return path


# Global configuration instance
config = Config()