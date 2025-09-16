"""Utility functions for data processing."""

import numpy as np
import pandas as pd


def prepare_dataframe(raw_data, leave_pure=False, use_candle_hi_lo=False):
    """Convert raw data to DataFrame with proper formatting."""
    
    # Convert to dataframe with timestamp as index and price as column
    df = pd.DataFrame(raw_data, columns=['timestamp', 
                                         'low', 'high', 
                                         'open', 'close', 
                                         'volume'])
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    if not leave_pure:
        df['price'] = (df['high'] + df['low']) / 2
    df = df.set_index('timestamp')
    df = df[~df.index.duplicated(keep='first')]
    df = df.sort_index()
    
    if leave_pure:
        return df
    
    if use_candle_hi_lo:    
        df = interpolate_hilo(df)
    
    return df


def interpolate_hilo(df):
    """Interpolate high-low prices based on candlestick direction using vectorized operations."""
    # Create shifted index for mid-points
    time_diff = pd.Series(df.index[1:] - df.index[:-1], index=df.index[:-1])
    mid_points = pd.Series(df.index[:-1] + time_diff/2, index=df.index[:-1])
    
    # Determine candlestick direction and prices vectorially
    is_bullish = df['open'] < df['close']
    first_prices = np.where(is_bullish, df['low'], df['high'])
    second_prices = np.where(is_bullish, df['high'], df['low'])
    
    # Create final index and data arrays with correct sizes
    n = len(df)
    new_index = np.empty(2 * n - 1, dtype='datetime64[ns]')
    new_index[::2] = df.index  # Use full index for even positions
    new_index[1::2] = mid_points  # Use mid_points for odd positions
    
    # Create price array with correct size
    new_prices = np.empty(2 * n - 1)
    new_prices[::2] = first_prices  # Use all first_prices
    new_prices[1::2] = second_prices[:-1]  # Use second_prices except last one
    
    return pd.DataFrame({'price': new_prices}, index=new_index)