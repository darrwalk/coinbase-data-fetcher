# Migration Guide

This guide helps you migrate from the embedded `fetch_data.py` to the standalone `coinbase-data-fetcher` module.

## Module Structure

The original `fetch_data.py` has been split into:

- `models.py`: Data models (Coins, CoinInfo, CoinDataModel, CoinData)
- `fetcher.py`: Core fetching logic
- `utils.py`: Data processing utilities
- `progress.py`: Progress bar abstraction
- `config.py`: Configuration management

## Key Changes

### 1. Dependencies
- Removed dependency on project-specific `Model` base class
- `ProgressBar` is now included in the module with optional tqdm support
- Configuration via environment variable instead of `Settings` class

### 2. Imports
Instead of:
```python
from fetch_data import CoinData, fetch_prices
```

Use either:
```python
# Direct import (after installing the module)
from coinbase_data_fetcher import CoinData, fetch_prices

# Or use the adapter during migration
from fetch_data_adapter import CoinData, fetch_prices
```

### 3. Configuration
Set cache path via environment variable:
```bash
export COINBASE_CACHE_PATH=/path/to/cache
```

Or programmatically:
```python
from coinbase_data_fetcher.config import config
config.cache_path = '/your/cache/path'
```

### 4. Progress Bars
The module includes its own progress bar abstraction. To use tqdm:
```bash
pip install coinbase-data-fetcher[tqdm]
```

## Migration Steps

1. **Install the module** (if published):
   ```bash
   pip install coinbase-data-fetcher
   ```

   Or for development:
   ```bash
   cd coinbase-data-fetcher
   pip install -e .
   ```

2. **Run migration script** (for TradeOptimizer):
   ```bash
   # Dry run to see what would change
   python migrate_to_fetcher_module.py
   
   # Apply changes
   python migrate_to_fetcher_module.py --apply
   ```

3. **Update imports manually** if needed

4. **Remove old fetch_data.py** once migration is complete

## Testing

Run tests to ensure everything works:
```bash
cd coinbase-data-fetcher
pytest tests/
```

## Backward Compatibility

The `fetch_data_adapter.py` provides backward compatibility during migration. Once all code is updated, you can remove this adapter and the original `fetch_data.py`.