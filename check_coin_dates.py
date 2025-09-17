#!/usr/bin/env python3
"""Script to check earliest available dates for cryptocurrencies on Coinbase."""

import json
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple

import pandas as pd
import requests
from ratelimit import limits, sleep_and_retry
from tenacity import retry, retry_if_exception_type, wait_exponential
from tenacity.stop import stop_after_attempt


@sleep_and_retry
@limits(calls=10, period=1)
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.Timeout)
)
def requests_get(url: str, params: dict) -> requests.Response:
    """Rate-limited and retrying GET request."""
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response


def check_date_has_data(symbol: str, test_date: pd.Timestamp) -> bool:
    """Check if a specific date has data for the given symbol."""
    url = f'https://api.exchange.coinbase.com/products/{symbol}/candles'
    
    # Request a small window around the test date
    start_time = test_date
    end_time = test_date + pd.Timedelta(days=1)
    
    params = {
        'granularity': 86400,  # Daily candles
        'start': start_time.isoformat(),
        'end': end_time.isoformat()
    }
    
    try:
        response = requests_get(url, params=params)
        data = response.json()
        return len(data) > 0
    except Exception as e:
        print(f"Error checking {symbol} on {test_date}: {e}")
        return False


def binary_search_earliest_date(symbol: str, start_date: pd.Timestamp, end_date: pd.Timestamp) -> Optional[pd.Timestamp]:
    """Binary search to find the earliest date with data."""
    print(f"  Searching for {symbol} between {start_date.date()} and {end_date.date()}")
    
    # First check if the end date has data
    if not check_date_has_data(symbol, end_date):
        print(f"  No data found for {symbol} on {end_date.date()}")
        return None
    
    earliest_known = end_date
    latest_without = start_date
    
    while (earliest_known - latest_without).days > 1:
        mid_date = latest_without + (earliest_known - latest_without) // 2
        mid_date = mid_date.replace(hour=0, minute=0, second=0, microsecond=0)
        
        if check_date_has_data(symbol, mid_date):
            earliest_known = mid_date
            print(f"  Found data on {mid_date.date()}")
        else:
            latest_without = mid_date
            print(f"  No data on {mid_date.date()}")
        
        time.sleep(0.1)  # Be nice to the API
    
    # Double-check the earliest known date
    if check_date_has_data(symbol, earliest_known):
        return earliest_known
    else:
        # Try the next day
        next_day = earliest_known + pd.Timedelta(days=1)
        if check_date_has_data(symbol, next_day):
            return next_day
    
    return None


def find_earliest_date(coin_symbol: str, coinbase_symbol: str) -> Tuple[str, Optional[pd.Timestamp]]:
    """Find the earliest available date for a coin."""
    print(f"\nChecking {coin_symbol} ({coinbase_symbol})...")
    
    # Start from a very early date (Coinbase was founded in 2012)
    start_date = pd.Timestamp("2012-01-01")
    # Use yesterday to avoid issues with today's data
    end_date = pd.Timestamp("2024-12-31")  # Using a known valid date
    
    earliest_date = binary_search_earliest_date(coinbase_symbol, start_date, end_date)
    
    if earliest_date:
        print(f"  ✓ Earliest date for {coin_symbol}: {earliest_date.date()}")
    else:
        print(f"  ✗ No data found for {coin_symbol}")
    
    return coin_symbol, earliest_date


def main():
    """Main function to check all coins."""
    coins_to_check = [
        ("AVAX", "AVAX-USD"),
        ("DOT", "DOT-USD"),
        ("MATIC", "MATIC-USD"),
        ("LINK", "LINK-USD"),
        ("NEAR", "NEAR-USD"),
        ("ICP", "ICP-USD"),
        ("ATOM", "ATOM-USD"),
        ("APT", "APT-USD"),
        ("ARB", "ARB-USD"),
        ("OP", "OP-USD"),
        ("SUI", "SUI-USD"),
        ("UNI", "UNI-USD"),
        ("AAVE", "AAVE-USD"),
        ("CRV", "CRV-USD"),
        ("MKR", "MKR-USD"),
        ("COMP", "COMP-USD"),
        ("SNX", "SNX-USD"),
        ("LDO", "LDO-USD"),
        ("SUSHI", "SUSHI-USD"),
        ("YFI", "YFI-USD"),
        ("BAL", "BAL-USD"),
        ("PERP", "PERP-USD"),
        ("SAND", "SAND-USD"),
        ("MANA", "MANA-USD"),
        ("AXS", "AXS-USD"),
        ("IMX", "IMX-USD"),
        ("ENS", "ENS-USD"),
        ("BLUR", "BLUR-USD"),
        ("APE", "APE-USD"),
        ("FIL", "FIL-USD"),
        ("GRT", "GRT-USD"),
        ("LRC", "LRC-USD"),
        ("ANKR", "ANKR-USD"),
        ("SKL", "SKL-USD"),
        ("MASK", "MASK-USD"),
        ("BCH", "BCH-USD"),
        ("ETC", "ETC-USD"),
        ("ZEC", "ZEC-USD"),
        ("XLM", "XLM-USD"),
        ("VET", "VET-USD"),
        ("HBAR", "HBAR-USD"),
        ("QNT", "QNT-USD"),
        ("ALGO", "ALGO-USD"),
        ("EOS", "EOS-USD"),
        ("XTZ", "XTZ-USD"),
        ("CHZ", "CHZ-USD"),
        ("BAT", "BAT-USD"),
        ("1INCH", "1INCH-USD"),
        ("SHIB", "SHIB-USD"),
    ]
    
    results = []
    
    for coin_symbol, coinbase_symbol in coins_to_check:
        coin, earliest_date = find_earliest_date(coin_symbol, coinbase_symbol)
        if earliest_date:
            results.append((coin, coinbase_symbol, earliest_date))
    
    # Output the CoinInfo entries
    print("\n" + "="*80)
    print("CoinInfo entries for models.py:")
    print("="*80 + "\n")
    
    for coin, symbol, date in sorted(results, key=lambda x: x[0]):
        # Find the corresponding Coins enum name
        coin_enum = coin.upper()
        if coin == "1INCH":
            coin_enum = "ONEINCH"
        
        print(f'    Coins.{coin_enum}: CoinInfo(coin=Coins.{coin_enum}, symbol="{symbol}", start_date=Timestamp("{date.strftime("%Y-%m-%d")}")),')
    
    print("\n" + "="*80)
    print(f"Total coins found: {len(results)}")
    print("="*80)


if __name__ == "__main__":
    main()