#!/usr/bin/env python3
"""Pre-fetch cryptocurrency data to warm the cache."""

import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from coinbase_data_fetcher.config import config
from coinbase_data_fetcher.fetcher import fetch_prices
from coinbase_data_fetcher.models import COIN_INFO, CoinDataModel
from coinbase_data_fetcher.progress import TqdmProgressBar, NullProgressBar


def fetch_data_for_coin(coin, granularity, save_csv: bool = True, progress_bar_desc: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None, interpolate_price: bool = True):
    """Fetch data for a specific coin and granularity."""
    try:
        from coinbase_data_fetcher.progress import TqdmProgressBar
        progress_bar = TqdmProgressBar(
            total=100, 
            desc=progress_bar_desc or f"{coin.upper()}-{int(granularity/60)}m"
        )
    except ImportError:
        progress_bar = NullProgressBar()
    
    # Use provided dates or defaults
    start_time = pd.Timestamp(start_date) if start_date else COIN_INFO[coin].start_date
    yesterday = pd.Timestamp.now().date() - pd.Timedelta(days=1)
    
    if end_date:
        end_time = pd.Timestamp(end_date)
        # Ensure end date is not later than yesterday
        if end_time.date() > yesterday:
            print(f"Warning: End date {end_date} is in the future. Using yesterday ({yesterday}) instead.")
            end_time = yesterday
    else:
        end_time = yesterday
    
    df = fetch_prices(
        coin,
        start_time=start_time,
        end_time=end_time,
        granularity=granularity,
        progress_bar=progress_bar,
        leave_pure=not interpolate_price,
        use_candle_hi_lo=interpolate_price
    )
    
    if save_csv:
        # Write to CSV
        start_date = df.index[0].date().strftime('%Y-%m-%d')
        end_date = df.index[-1].date().strftime('%Y-%m-%d')
        
        # Create cache folder if not exists
        cache_path = config.cache_path
        if not os.path.exists(cache_path):
            os.makedirs(cache_path)
        
        csv_path = f'{cache_path}/{coin}_{granularity}_{start_date}_{end_date}.csv'
        df.to_csv(csv_path)
        print(f"Saved: {csv_path}")
    
    return df



def main():
    """CLI entry point for prefetching data."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Pre-fetch cryptocurrency data")
    parser.add_argument('--coin', help="Specific coin to fetch (e.g., bitcoin)")
    parser.add_argument('--granularity', type=int, help="Specific granularity in seconds (e.g., 3600)")
    parser.add_argument('--start-date', help="Start date for fetching (e.g., 2023-01-01)")
    parser.add_argument('--end-date', help="End date for fetching (e.g., 2023-12-31)")
    parser.add_argument('--no-interpolate-price', action='store_true', help="Don't interpolate prices using candlestick hi/lo data")
    parser.add_argument('--no-csv', action='store_true', help="Don't save CSV files")
    parser.add_argument('--cache-path', help="Override cache directory")
    
    args = parser.parse_args()
    
    if args.cache_path:
        config.cache_path = args.cache_path
    
    coins: list[str] = []
    granularities: list[int] = []
    if not args.coin:
        coins = CoinDataModel.get_choices("coin")
    else:
        coins = [args.coin]
        
    if not args.granularity:
        granularities = CoinDataModel.get_choices("data_granularity")
    else:
        granularities = [args.granularity]
    
    for coin in coins:
        for granularity in granularities:
            try:
                fetch_data_for_coin(coin, granularity, save_csv=not args.no_csv,
                                   start_date=args.start_date, end_date=args.end_date,
                                   interpolate_price=not args.no_interpolate_price)
            except Exception as e:
                print(f"Error fetching {coin} at {granularity}s: {e}")
                continue


if __name__ == "__main__":
    main()