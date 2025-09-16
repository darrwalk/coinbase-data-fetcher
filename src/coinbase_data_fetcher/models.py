"""Data models for Coinbase data fetcher."""

from datetime import datetime, timedelta
from enum import StrEnum
from typing import Literal, Optional

import pandas as pd
from pandas import Timestamp
from pydantic import BaseModel, Field, field_validator

from coinbase_data_fetcher.progress import ProgressBar, NullProgressBar


def yesterday_ts() -> pd.Timestamp:
    """Get yesterday's timestamp."""
    return pd.Timestamp(datetime.now().date() - timedelta(days=1))


class Coins(StrEnum):
    BITCOIN = "bitcoin"
    ETHEREUM = "ethereum"
    SOLANA = "solana"
    LITECOIN = "litecoin"
    DOGECOIN = "dogecoin"
    WIF = "dogwifhat"
    XRP = "xrp"
    ADA = "ada"


class CoinInfo(BaseModel):
    coin: Coins
    symbol: str
    start_date: Timestamp
    logo_url: str = ""
    
    model_config = {"arbitrary_types_allowed": True}
    
    def __init__(self, **data):
        super().__init__(**data)
        if self.logo_url == "":
            self.logo_url = f"https://cryptologos.cc/logos/thumbs/{self.coin}.png"


COIN_INFO = {
    Coins.BITCOIN: CoinInfo(coin=Coins.BITCOIN, symbol="BTC-USD", 
                            start_date=Timestamp("2015-07-20")),
    Coins.ETHEREUM: CoinInfo(coin=Coins.ETHEREUM, symbol="ETH-USD", 
                            start_date=Timestamp("2016-07-21")),
    Coins.SOLANA: CoinInfo(coin=Coins.SOLANA, symbol="SOL-USD", 
                            start_date=Timestamp("2021-05-24")),
    Coins.LITECOIN: CoinInfo(coin=Coins.LITECOIN, symbol="LTC-USD", 
                            start_date=Timestamp("2017-05-03")),
    Coins.DOGECOIN: CoinInfo(coin=Coins.DOGECOIN, symbol="DOGE-USD", 
                            start_date=Timestamp("2021-06-03")),
    Coins.WIF: CoinInfo(coin=Coins.WIF, symbol="WIF-USD", 
                            start_date=Timestamp("2024-11-13")),
    Coins.XRP: CoinInfo(coin=Coins.XRP, symbol="XRP-USD", 
                            start_date=Timestamp("2019-02-28")),
    Coins.ADA: CoinInfo(coin=Coins.ADA, symbol="ADA-USD", 
                            start_date=Timestamp("2021-03-18")),
}


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
                3600: "1 hour"
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