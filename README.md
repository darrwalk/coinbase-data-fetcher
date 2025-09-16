# Coinbase Data Fetcher

A Python library for fetching historical cryptocurrency data from Coinbase with caching support.

## Features

- Fetch historical price data for multiple cryptocurrencies
- Built-in rate limiting and retry logic
- Local caching to minimize API calls
- Support for multiple time granularities (1min, 5min, 15min, 1hour)
- Optional candlestick high/low interpolation
- Progress bar support

## Installation

```bash
pip install coinbase-data-fetcher
```

For progress bar support:
```bash
pip install coinbase-data-fetcher[tqdm]
```

## Usage

```python
from coinbase_data_fetcher import CoinDataModel, CoinData, Coins
import pandas as pd

# Create a model for Bitcoin data
model = CoinDataModel(
    coin=Coins.BITCOIN,
    data_granularity=3600,  # 1 hour
    start_date=pd.Timestamp('2023-01-01'),
    end_date=pd.Timestamp('2023-12-31'),
    price_interpolation='mean'
)

# Create data fetcher
coin_data = CoinData(model)

# Fetch prices
df = coin_data.fetch_prices()
```

### Direct API Usage

```python
from coinbase_data_fetcher import fetch_prices, Coins

df = fetch_prices(
    coin=Coins.ETHEREUM,
    start_time='2023-06-01',
    end_time='2023-06-30',
    granularity=300,  # 5 minutes
    use_candle_hi_lo=True
)
```

## Configuration

Set the cache directory using environment variable:
```bash
export COINBASE_CACHE_PATH=/path/to/cache
```

Or programmatically:
```python
from coinbase_data_fetcher.config import config
config.cache_path = '/path/to/cache'
```

## Available Coins

- Bitcoin (BTC-USD) - Available from 2015-07-20
- Ethereum (ETH-USD) - Available from 2016-07-21
- Solana (SOL-USD) - Available from 2021-05-24
- Litecoin (LTC-USD) - Available from 2017-05-03
- Dogecoin (DOGE-USD) - Available from 2021-06-03
- dogwifhat (WIF-USD) - Available from 2024-11-13

## Development

### Setup

1. Clone the repository
2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. Install development dependencies:
   ```bash
   pip install -e ".[dev]"
   ```

### Testing

Run tests with pytest:
```bash
source .venv/bin/activate
pytest
```

Run tests with coverage:
```bash
pytest --cov=coinbase_data_fetcher
```

### Code Quality

The project uses Ruff for linting and Pyright for type checking:
```bash
ruff check src tests
pyright
```

### Requirements

- Python 3.9+
- Dependencies managed via pyproject.toml

## API Rate Limiting

The library includes built-in rate limiting (10 calls per second) and automatic retry logic with exponential backoff to handle Coinbase API limits gracefully.

## Caching

Fetched data is automatically cached locally to minimize API calls. The cache directory can be configured via the `COINBASE_CACHE_PATH` environment variable.

## License

MIT License