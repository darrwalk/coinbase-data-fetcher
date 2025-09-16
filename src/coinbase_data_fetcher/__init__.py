"""Coinbase data fetcher for cryptocurrency price data."""

from coinbase_data_fetcher.fetcher import fetch_prices
from coinbase_data_fetcher.models import CoinData, CoinDataModel, CoinInfo, Coins, COIN_INFO
from coinbase_data_fetcher.prefetch import fetch_data_for_coin, prefetch_all_data

__version__ = "0.1.0"
__all__ = [
    "fetch_prices",
    "Coins",
    "CoinInfo",
    "COIN_INFO",
    "CoinDataModel",
    "CoinData",
    "fetch_data_for_coin",
    "prefetch_all_data",
]