"""Coinbase data fetcher for cryptocurrency price data."""

from coinbase_data_fetcher.fetcher import fetch_prices
from coinbase_data_fetcher.models import CoinData, CoinDataModel, Coins, COIN_INFO
from coinbase_data_fetcher.config_loader import CoinInfo
from coinbase_data_fetcher.prefetch import fetch_data_for_coin

__version__ = "0.2.0"
__all__ = [
    "fetch_prices",
    "Coins",
    "CoinInfo",
    "COIN_INFO",
    "CoinDataModel",
    "CoinData",
    "fetch_data_for_coin",
]