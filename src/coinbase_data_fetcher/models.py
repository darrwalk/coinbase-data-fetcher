"""Data models for Coinbase data fetcher."""

from datetime import datetime, timedelta
from enum import StrEnum
from typing import Literal, Optional

import pandas as pd
from pandas import Timestamp
from pydantic import BaseModel, Field, field_validator

from coinbase_data_fetcher.progress import ProgressBar, NullProgressBar
from coinbase_data_fetcher.config_loader import load_coin_info


def yesterday_ts() -> pd.Timestamp:
    """Get yesterday's timestamp."""
    return pd.Timestamp(datetime.now().date() - timedelta(days=1))


class Coins(StrEnum):
    # Major cryptocurrencies
    BITCOIN = "bitcoin"
    ETHEREUM = "ethereum"
    
    # Top 10 by market cap
    SOLANA = "solana"
    XRP = "xrp"
    ADA = "ada"
    AVAX = "avalanche"
    DOGECOIN = "dogecoin"
    DOT = "polkadot"
    MATIC = "polygon"
    LINK = "chainlink"
    
    # Layer 1 & Layer 2
    NEAR = "near"
    ICP = "internet-computer"
    ATOM = "cosmos"
    APT = "aptos"
    ARB = "arbitrum"
    OP = "optimism"
    SUI = "sui"
    
    # DeFi tokens
    UNI = "uniswap"
    AAVE = "aave"
    CRV = "curve"
    MKR = "maker"
    COMP = "compound"
    SNX = "synthetix"
    LDO = "lido"
    SUSHI = "sushiswap"
    YFI = "yearn-finance"
    BAL = "balancer"
    PERP = "perpetual-protocol"
    
    # Gaming & Metaverse
    SAND = "sandbox"
    MANA = "decentraland"
    AXS = "axie-infinity"
    IMX = "immutablex"
    ENS = "ethereum-name-service"
    BLUR = "blur"
    APE = "apecoin"
    
    # Infrastructure & Web3
    FIL = "filecoin"
    GRT = "the-graph"
    LRC = "loopring"
    ANKR = "ankr"
    SKL = "skale"
    MASK = "mask-network"
    
    # Bitcoin forks & Classic
    LITECOIN = "litecoin"
    BCH = "bitcoin-cash"
    ETC = "ethereum-classic"
    
    # Privacy & Payments
    ZEC = "zcash"
    XLM = "stellar"
    
    # Enterprise & Other
    VET = "vechain"
    HBAR = "hedera"
    QNT = "quant"
    ALGO = "algorand"
    EOS = "eos"
    XTZ = "tezos"
    CHZ = "chiliz"
    
    # Utility tokens
    BAT = "basic-attention-token"
    ONEINCH = "1inch"
    
    # Meme coins
    SHIB = "shiba-inu"
    WIF = "dogwifhat"


# Load coin info from configuration file
# Map string keys to Coins enum
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