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


def fetch_data_for_coin(coin, granularity, save_csv: bool = True, progress_bar_desc: Optional[str] = None):
    """Fetch data for a specific coin and granularity."""
    try:
        from coinbase_data_fetcher.progress import TqdmProgressBar
        progress_bar = TqdmProgressBar(
            total=100, 
            desc=progress_bar_desc or f"{coin.upper()}-{int(granularity/60)}m"
        )
    except ImportError:
        progress_bar = NullProgressBar()
    
    df = fetch_prices(
        coin,
        start_time=COIN_INFO[coin].start_date,
        end_time=pd.Timestamp.now().date() - pd.Timedelta(days=1),
        granularity=granularity,
        progress_bar=progress_bar,
        leave_pure=True
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


def prefetch_all_data():
    """Pre-fetch all coin data for all granularities."""
    coins = CoinDataModel.get_choices("coin")
    granularities = CoinDataModel.get_choices("data_granularity")
    
    print(f"Pre-fetching data for {len(coins)} coins with {len(granularities)} granularities...")
    print(f"Cache directory: {config.cache_path}")
    
    for coin in coins:
        for granularity in granularities:
            try:
                fetch_data_for_coin(coin, granularity)
            except Exception as e:
                print(f"Error fetching {coin} at {granularity}s: {e}")
                continue
    
    print("Pre-fetching completed!")


def main():
    """CLI entry point for prefetching data."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Pre-fetch cryptocurrency data")
    parser.add_argument('--coin', help="Specific coin to fetch (e.g., bitcoin)")
    parser.add_argument('--granularity', type=int, help="Specific granularity in seconds (e.g., 3600)")
    parser.add_argument('--no-csv', action='store_true', help="Don't save CSV files")
    parser.add_argument('--cache-path', help="Override cache directory")
    
    args = parser.parse_args()
    
    if args.cache_path:
        config.cache_path = args.cache_path
    
    if args.coin and args.granularity:
        # Fetch specific coin and granularity
        print(f"Fetching {args.coin} data at {args.granularity}s granularity...")
        fetch_data_for_coin(args.coin, args.granularity, save_csv=not args.no_csv)
    elif args.coin:
        # Fetch all granularities for specific coin
        granularities = CoinDataModel.get_choices("data_granularity")
        print(f"Fetching all granularities for {args.coin}...")
        for granularity in granularities:
            try:
                fetch_data_for_coin(args.coin, granularity, save_csv=not args.no_csv)
            except Exception as e:
                print(f"Error fetching {args.coin} at {granularity}s: {e}")
                continue
    else:
        # Fetch all coins and granularities
        prefetch_all_data()


if __name__ == "__main__":
    main()