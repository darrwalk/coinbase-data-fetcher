"""Core fetching functionality for Coinbase data."""

import json
import logging
import os
from datetime import timedelta
from functools import lru_cache
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from ratelimit import limits, sleep_and_retry
from tenacity import retry, retry_if_exception_type, wait_exponential
from tenacity.stop import stop_after_attempt

from coinbase_data_fetcher.config import config
from coinbase_data_fetcher.models import COIN_INFO
from coinbase_data_fetcher.config_loader import CoinInfo
from coinbase_data_fetcher.progress import NullProgressBar, ProgressBar
from coinbase_data_fetcher.utils import prepare_dataframe

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


@sleep_and_retry
@limits(calls=10, period=1)
@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(requests.exceptions.Timeout)
)
def requests_get(url, params):
    """Rate-limited and retrying GET request."""
    response = requests.get(url, params=params)
    response.raise_for_status()
    return response


def fetch_prices(coin,
                 start_time, 
                 end_time, 
                 granularity=300, 
                 progress_bar: Optional[ProgressBar] = None,
                 leave_pure=False,
                 use_candle_hi_lo=False):
    """Fetch cryptocurrency prices from Coinbase."""
    
    if progress_bar is None:
        progress_bar = NullProgressBar()
    
    start_time = pd.Timestamp(start_time).floor('s').tz_localize(None)
    end_time = pd.Timestamp(end_time).floor('s').tz_localize(None)
    original_start = start_time
    
    if granularity == 60: # 1 min
        num_results = 300
    elif granularity == 300:
        num_results = 12 * 24
    elif granularity == 900: # 15 min
        num_results = 4 * 24
    elif granularity == 3600: # 1 hour
        num_results = 24 * 7
        start_time = start_time - pd.Timedelta(days=start_time.dayofweek)
    elif granularity == 21600: # 6 hour
        num_results = 4 * 7
        start_time = start_time - pd.Timedelta(days=start_time.dayofweek)
    elif granularity == 86400: # 1 day
        num_results = 1 * 7
        start_time = start_time - pd.Timedelta(days=start_time.dayofweek)
    else:
        raise ValueError(f"Unsupported granularity: {granularity}")
    
    raw_data = fetch_coinbase_data(coin, 
                             start_time, 
                             end_time, 
                             granularity=granularity, 
                             progress_bar=progress_bar,
                             num_results=num_results)
    
    df = prepare_dataframe(raw_data, leave_pure=leave_pure, use_candle_hi_lo=use_candle_hi_lo)
    if granularity >= 3600:
        df = df[df.index >= original_start]
    
    return df


@lru_cache(maxsize=32)
def fetch_coinbase_data(coin,
                        start_time, 
                        end_time, 
                        granularity=300, 
                        progress_bar: ProgressBar = None,
                        num_results=300):
    """Fetch data from Coinbase API with caching."""
    
    if progress_bar is None:
        progress_bar = NullProgressBar()

    start_time = pd.Timestamp(start_time).tz_localize(None)
    end_time = pd.Timestamp(end_time).tz_localize(None)
    product_id = COIN_INFO[coin].symbol

    url = f'https://api.exchange.coinbase.com/products/{product_id}/candles'

    all_data = []

    # Calculate the maximum time span based on the granularity
    max_span = timedelta(seconds=granularity * num_results)

    cache_path = Path(config.cache_path) / product_id / str(granularity)
    cache_path.mkdir(parents=True, exist_ok=True)

    total_span = end_time - start_time
    progress_bar.text(f"Fetching data from Coinbase: {product_id}")
    
    while start_time < end_time:
        # Set the chunk end time
        chunk_end_time = min(start_time + max_span, end_time)
        
        # If the chunk ends on the next day, set to beginning of that day
        if chunk_end_time.day != start_time.day:
            chunk_end_time = chunk_end_time.replace(hour=0, minute=0, second=0)
        
        filename = f'{start_time.isoformat()}_{chunk_end_time.isoformat()}.json'
        cache_filename = cache_path / filename
        
        params = {
            'granularity': granularity,
            'start': start_time.isoformat(),
            'end': chunk_end_time.isoformat()
        }
                          
        if not cache_filename.exists():
            # Fetch and cache data
            data = fetch_or_load_data(url, params, cache_filename, chunk_end_time)
            progress_bar.text(f"Fetching prices: {filename}")
        else:
            try:
                with open(cache_filename, 'r') as f:
                    data = json.load(f)
                progress_bar.text(f"Cached prices: {filename}")
            except json.JSONDecodeError:
                logger.warning(f"Invalid JSON in cache file: {cache_filename}. Fetching fresh data.")
                cache_filename.unlink()
                data = fetch_or_load_data(url, params, cache_filename, chunk_end_time)

        all_data.extend(data)
        start_time = chunk_end_time
        progress_bar.progress(1 - (end_time - start_time) / total_span)

    progress_bar.empty()
    return all_data


def fetch_or_load_data(url, params, cache_filename, chunk_end_time):
    """Fetch data from API or load from cache."""
    try:
        if not cache_filename.exists() or cache_filename.stat().st_size == 0:
            logger.info(f"Fetching fresh data for {cache_filename}")
            response = requests_get(url, params=params)
            
            if response.status_code != 200:
                logger.error(f"Error fetching data: {response.status_code}")
                return None
            
            data = response.json()
            
            # Cache if data is complete (not today's data)
            if chunk_end_time < pd.Timestamp.now():
                logger.info(f"Writing data to cache: {cache_filename}")
                with open(cache_filename, 'w') as f:
                    json.dump(data, f)
        else:
            logger.info(f"Loading data from cache: {cache_filename}")
            with open(cache_filename, 'r') as f:
                data = json.load(f)
        
        return data
    except json.JSONDecodeError as e:
        logger.warning(f"Invalid JSON in cache file: {cache_filename}. Error: {str(e)}")
        cache_filename.unlink()
        return fetch_or_load_data(url, params, cache_filename, chunk_end_time)
    except Exception as e:
        logger.error(f"Unexpected error in fetch_or_load_data: {str(e)}")
        return None