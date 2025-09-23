"""Data models for Coinbase data fetcher."""

from datetime import datetime, timedelta
from enum import StrEnum
from typing import Literal, Optional

import pandas as pd
from pandas import Timestamp
from pydantic import BaseModel, Field, field_validator

from coinbase_data_fetcher.progress import ProgressBar, NullProgressBar
from coinbase_data_fetcher.config_loader import load_coin_info, load_coins_config


def yesterday_ts() -> pd.Timestamp:
    """Get yesterday's timestamp."""
    return pd.Timestamp(datetime.now().date() - timedelta(days=1))


def _create_coins_enum():
    """Dynamically create Coins enum from available coin configuration."""
    try:
        coins_config = load_coins_config()
        available_coins = list(coins_config.keys())
        
        # Create enum members dynamically
        enum_dict = {}
        for coin_id in sorted(available_coins):
            # Convert coin ID to enum name (uppercase, replace hyphens with underscores)
            enum_name = coin_id.upper().replace('-', '_').replace(' ', '_')
            # Handle special cases for readability
            name_mapping = {
                'QUANT_NETWORK': 'QNT',
                'RENDER_TOKEN': 'RENDER', 
                'OFFICIAL_TRUMP': 'TRUMP',
                'IMMUTABLE_X': 'IMX',
                'ETHEREUM_NAME_SERVICE': 'ENS',
                'VENICE_TOKEN': 'VVV',
                'WORLDCOIN_WLD': 'WLD',
                'ONDO_FINANCE': 'ONDO',
                'BIG_TIME': 'BIGTIME',
                'MOG_COIN': 'MOG',
                'GODS_UNCHAINED': 'GODS',
                'BIO_PROTOCOL': 'BIO',
                'CONVEX_FINANCE': 'CVX',
                'CHAIN_2': 'XCN',
                'PANCAKESWAP_TOKEN': 'CAKE',
                'PAX_GOLD': 'PAXG',
                'ECHELON_PRIME': 'PRIME',
                'POWER_LEDGER': 'POWR',
                'OCEAN_PROTOCOL': 'OCEAN',
                'ORIGIN_PROTOCOL': 'OGN',
                'WRAPPED_CENTRIFUGE': 'WCFG',
                'PEANUT_THE_SQUIRREL': 'PNUT',
                'STORY_2': 'IP',
                'BANKERCOIN_2': 'BNKR',
                'KERNEL_2': 'KERNEL'
            }
            enum_name = name_mapping.get(enum_name, enum_name)
            enum_dict[enum_name] = coin_id
            
        return StrEnum('Coins', enum_dict)
    except Exception:
        # Fallback to basic coins if config loading fails
        return StrEnum('Coins', {
            'BITCOIN': 'bitcoin',
            'ETHEREUM': 'ethereum',
            'LITECOIN': 'litecoin'
        })

# Create the Coins enum dynamically
Coins = _create_coins_enum()

# Load coin info from configuration file  
_coin_info_str = load_coin_info()
COIN_INFO = {}
for member in Coins:
    if member.value in _coin_info_str:
        COIN_INFO[member] = _coin_info_str[member.value]


class CoinDataModel(BaseModel):
    model_config = {"arbitrary_types_allowed": True}
    
    coin: Coins = Field(
        default=Coins.BITCOIN,
        title="Select Coin",
        description="Choose the cryptocurrency to analyze",
    )
    
    data_granularity: int = Field(
        default=3600,
        title="Data granularity",
        description="E.g. 5min for 5 minutes candles",
        json_schema_extra={
            "choices": {
                60: "1 min.",
                300: "5 min.", 
                900: "15 min.",
                3600: "1 hour",
                21600: "6 hours",
                86400: "1 day"
            }
        }
    )
    
    start_date: pd.Timestamp = Field(
        default_factory=lambda: yesterday_ts() - pd.DateOffset(months=3),
        title="Start Date",
        description="Beginning of simulation"
    )
    
    end_date: pd.Timestamp = Field(
        default_factory=lambda: yesterday_ts(),
        title="End Date",
        description="End of simulation"
    )
    
    price_interpolation: Literal["Hi-Lo", "mean"] = Field(
        default="Hi-Lo",
        title="Price interpolation",
        description="""Hi-Lo: Use the high as the start price of a bearish candle 
        and the low in the middle between the following candle's start.
        For bullish candles vice-verse.
        
        Mean: Use the mean of the high and low of the candle as the 
        price for the entire candle period.""",
    )

    @field_validator('start_date', 'end_date', mode='before')
    @classmethod
    def parse_timestamp(cls, value):
        if isinstance(value, str):
            return pd.Timestamp(value).tz_localize(None)
        return value
    
    @classmethod
    def get_choices(cls, field_name: str) -> list:
        """Get choices for a field if available."""
        field = cls.model_fields.get(field_name)
        if field:
            # Special handling for enum fields
            if field_name == "coin" and hasattr(field.annotation, '__members__'):
                return [member.value for member in field.annotation]
            # Handle json_schema_extra for other fields
            if hasattr(field, 'json_schema_extra'):
                extra = field.json_schema_extra
                if isinstance(extra, dict) and 'choices' in extra:
                    return list(extra['choices'].keys())
        return []


class CoinData:
    
    def __init__(self, model: CoinDataModel):
        self.model = model
        
    def fetch_prices(self, progress_bar: Optional[ProgressBar] = None):
        from coinbase_data_fetcher.fetcher import fetch_prices
        
        if progress_bar is None:
            progress_bar = NullProgressBar()
            
        return fetch_prices(
            coin=self.model.coin,
            start_time=self.model.start_date,
            end_time=self.model.end_date,
            granularity=self.model.data_granularity,
            use_candle_hi_lo=self.model.price_interpolation == "Hi-Lo",
            progress_bar=progress_bar
        )