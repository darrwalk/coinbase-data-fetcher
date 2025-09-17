# Coinbase Data Fetcher

A Python CLI and library for fetching historical crypto price data from Coinbase with caching support.

## Features

- Fetch historical price data for multiple cryptocurrencies
- Built-in rate limiting and retry logic
- Local caching to minimize API calls
- Support for multiple time granularities (1min, 5min, 15min, 1hour, 6hour, 1day)
- Optional candlestick high/low interpolation
- Progress bar support
- Command-line tool for batch data fetching

## Installation

```bash
pip install coinbase-data-fetcher
```

## Command Line Tool

After installation, you can use the `coinbase-fetch` command to fetch data:

```bash
# List all supported coins
coinbase-fetch --list-coins

# Pre-fetch all data for all coins and granularities
coinbase-fetch

# Pre-fetch all data with custom date range
coinbase-fetch --start-date 2024-01-01 --end-date 2024-01-31

# Pre-fetch specific coin (all granularities)
coinbase-fetch --coin bitcoin

# Pre-fetch XRP (Ripple) data
coinbase-fetch --coin xrp --granularity 3600

# Pre-fetch various cryptocurrencies
coinbase-fetch --coin avalanche --granularity 3600
coinbase-fetch --coin polygon --granularity 300
coinbase-fetch --coin uniswap --granularity 900

# Pre-fetch specific coin and granularity
coinbase-fetch --coin bitcoin --granularity 3600

# Pre-fetch with custom date range
coinbase-fetch --coin bitcoin --granularity 3600 --start-date 2023-01-01 --end-date 2023-12-31

# Use custom cache directory
coinbase-fetch --cache-path /custom/cache/path

# Don't save CSV files (cache only)
coinbase-fetch --no-csv

# Enable price interpolation using candlestick hi/lo data
coinbase-fetch --coin bitcoin --granularity 3600 --interpolate-price
```

### Command Line Options

- `--list-coins`: List all supported coins with their start dates
- `--coin`: Specific coin to fetch (e.g., bitcoin, ethereum, xrp, ada)
- `--granularity`: Time granularity in seconds (60, 300, 900, 3600, 21600, 86400)
- `--start-date`: Start date for fetching (e.g., 2023-01-01). Default: earliest available date for the coin
- `--end-date`: End date for fetching (e.g., 2023-12-31). Default: yesterday
- `--interpolate-price`: Enable price interpolation using candlestick hi/lo data (default: disabled)
- `--cache-path`: Override default cache directory
- `--no-csv`: Don't save CSV files, only cache JSON data

## Python API Usage

### Object-Oriented Interface

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

### Programmatic Pre-fetching

```python
from coinbase_data_fetcher import fetch_data_for_coin

# Pre-fetch specific coin and granularity
fetch_data_for_coin('bitcoin', 3600)  # Bitcoin, 1 hour granularity

# Pre-fetch with custom parameters
fetch_data_for_coin(
    'ethereum', 
    300,  # 5 minute granularity
    start_date='2024-01-01',
    end_date='2024-01-31',
    save_csv=False,  # Only cache, don't save CSVs
    interpolate_price=False  # Raw candlestick data
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

The library supports 57 cryptocurrencies available on Coinbase, including:

### Major Cryptocurrencies
- Bitcoin (BTC-USD) - Since 2015-07-20
- Ethereum (ETH-USD) - Since 2016-07-21

### Top Market Cap Coins
- Solana (SOL-USD), Ripple (XRP-USD), Cardano (ADA-USD)
- Avalanche (AVAX-USD), Polkadot (DOT-USD), Polygon (MATIC-USD)
- Chainlink (LINK-USD), Dogecoin (DOGE-USD)

### DeFi Tokens
- Uniswap (UNI-USD), Aave (AAVE-USD), Curve (CRV-USD)
- Maker (MKR-USD), Compound (COMP-USD), SushiSwap (SUSHI-USD)

### Layer 2 Solutions
- Arbitrum (ARB-USD), Optimism (OP-USD)

### Gaming & Metaverse
- Sandbox (SAND-USD), Decentraland (MANA-USD), ApeCoin (APE-USD)
- Axie Infinity (AXS-USD), ImmutableX (IMX-USD)

### And many more...
Including Cosmos (ATOM), Filecoin (FIL), The Graph (GRT), Algorand (ALGO), and others.

Use `coinbase-fetch --coin <name>` with the lowercase name (e.g., bitcoin, ethereum, avalanche)

To see all available coins with their start dates, run:
```bash
coinbase-fetch --list-coins
```

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