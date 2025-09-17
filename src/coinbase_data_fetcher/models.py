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
    # Major cryptocurrencies
    Coins.BITCOIN: CoinInfo(coin=Coins.BITCOIN, symbol="BTC-USD", 
                            start_date=Timestamp("2015-07-20")),
    Coins.ETHEREUM: CoinInfo(coin=Coins.ETHEREUM, symbol="ETH-USD", 
                            start_date=Timestamp("2016-07-21")),
    
    # Top 10 by market cap
    Coins.SOLANA: CoinInfo(coin=Coins.SOLANA, symbol="SOL-USD", 
                            start_date=Timestamp("2021-05-24")),
    Coins.XRP: CoinInfo(coin=Coins.XRP, symbol="XRP-USD", 
                            start_date=Timestamp("2019-02-28")),
    Coins.ADA: CoinInfo(coin=Coins.ADA, symbol="ADA-USD", 
                            start_date=Timestamp("2021-03-18")),
    Coins.AVAX: CoinInfo(coin=Coins.AVAX, symbol="AVAX-USD", 
                            start_date=Timestamp("2021-09-23")),
    Coins.DOGECOIN: CoinInfo(coin=Coins.DOGECOIN, symbol="DOGE-USD", 
                            start_date=Timestamp("2021-06-03")),
    Coins.DOT: CoinInfo(coin=Coins.DOT, symbol="DOT-USD", 
                            start_date=Timestamp("2021-06-16")),
    Coins.MATIC: CoinInfo(coin=Coins.MATIC, symbol="MATIC-USD", 
                            start_date=Timestamp("2021-03-10")),
    Coins.LINK: CoinInfo(coin=Coins.LINK, symbol="LINK-USD", 
                            start_date=Timestamp("2019-06-27")),
    
    # Layer 1 & Layer 2
    Coins.NEAR: CoinInfo(coin=Coins.NEAR, symbol="NEAR-USD", 
                            start_date=Timestamp("2022-10-13")),
    Coins.ICP: CoinInfo(coin=Coins.ICP, symbol="ICP-USD", 
                            start_date=Timestamp("2021-05-11")),
    Coins.ATOM: CoinInfo(coin=Coins.ATOM, symbol="ATOM-USD", 
                            start_date=Timestamp("2020-01-16")),
    Coins.APT: CoinInfo(coin=Coins.APT, symbol="APT-USD", 
                            start_date=Timestamp("2022-10-19")),
    Coins.ARB: CoinInfo(coin=Coins.ARB, symbol="ARB-USD", 
                            start_date=Timestamp("2023-03-23")),
    Coins.OP: CoinInfo(coin=Coins.OP, symbol="OP-USD", 
                            start_date=Timestamp("2022-07-14")),
    Coins.SUI: CoinInfo(coin=Coins.SUI, symbol="SUI-USD", 
                            start_date=Timestamp("2023-05-03")),
    
    # DeFi tokens
    Coins.UNI: CoinInfo(coin=Coins.UNI, symbol="UNI-USD", 
                            start_date=Timestamp("2020-09-17")),
    Coins.AAVE: CoinInfo(coin=Coins.AAVE, symbol="AAVE-USD", 
                            start_date=Timestamp("2020-12-16")),
    Coins.CRV: CoinInfo(coin=Coins.CRV, symbol="CRV-USD", 
                            start_date=Timestamp("2021-05-03")),
    Coins.MKR: CoinInfo(coin=Coins.MKR, symbol="MKR-USD", 
                            start_date=Timestamp("2020-06-09")),
    Coins.COMP: CoinInfo(coin=Coins.COMP, symbol="COMP-USD", 
                            start_date=Timestamp("2020-06-23")),
    Coins.SNX: CoinInfo(coin=Coins.SNX, symbol="SNX-USD", 
                            start_date=Timestamp("2020-12-16")),
    Coins.LDO: CoinInfo(coin=Coins.LDO, symbol="LDO-USD", 
                            start_date=Timestamp("2023-05-18")),
    Coins.SUSHI: CoinInfo(coin=Coins.SUSHI, symbol="SUSHI-USD", 
                            start_date=Timestamp("2021-03-10")),
    Coins.YFI: CoinInfo(coin=Coins.YFI, symbol="YFI-USD", 
                            start_date=Timestamp("2020-09-14")),
    Coins.BAL: CoinInfo(coin=Coins.BAL, symbol="BAL-USD", 
                            start_date=Timestamp("2020-08-04")),
    Coins.PERP: CoinInfo(coin=Coins.PERP, symbol="PERP-USD", 
                            start_date=Timestamp("2021-11-23")),
    
    # Gaming & Metaverse
    Coins.SAND: CoinInfo(coin=Coins.SAND, symbol="SAND-USD", 
                            start_date=Timestamp("2022-05-26")),
    Coins.MANA: CoinInfo(coin=Coins.MANA, symbol="MANA-USD", 
                            start_date=Timestamp("2021-04-28")),
    Coins.AXS: CoinInfo(coin=Coins.AXS, symbol="AXS-USD", 
                            start_date=Timestamp("2021-08-12")),
    Coins.IMX: CoinInfo(coin=Coins.IMX, symbol="IMX-USD", 
                            start_date=Timestamp("2021-11-25")),
    Coins.ENS: CoinInfo(coin=Coins.ENS, symbol="ENS-USD", 
                            start_date=Timestamp("2021-11-10")),
    Coins.BLUR: CoinInfo(coin=Coins.BLUR, symbol="BLUR-USD", 
                            start_date=Timestamp("2023-02-21")),
    Coins.APE: CoinInfo(coin=Coins.APE, symbol="APE-USD", 
                            start_date=Timestamp("2022-03-16")),
    
    # Infrastructure & Web3
    Coins.FIL: CoinInfo(coin=Coins.FIL, symbol="FIL-USD", 
                            start_date=Timestamp("2020-12-16")),
    Coins.GRT: CoinInfo(coin=Coins.GRT, symbol="GRT-USD", 
                            start_date=Timestamp("2020-12-18")),
    Coins.LRC: CoinInfo(coin=Coins.LRC, symbol="LRC-USD", 
                            start_date=Timestamp("2020-08-06")),
    Coins.ANKR: CoinInfo(coin=Coins.ANKR, symbol="ANKR-USD", 
                            start_date=Timestamp("2021-03-25")),
    Coins.SKL: CoinInfo(coin=Coins.SKL, symbol="SKL-USD", 
                            start_date=Timestamp("2021-03-18")),
    Coins.MASK: CoinInfo(coin=Coins.MASK, symbol="MASK-USD", 
                            start_date=Timestamp("2021-07-27")),
    
    # Bitcoin forks & Classic
    Coins.LITECOIN: CoinInfo(coin=Coins.LITECOIN, symbol="LTC-USD", 
                            start_date=Timestamp("2017-05-03")),
    Coins.BCH: CoinInfo(coin=Coins.BCH, symbol="BCH-USD", 
                            start_date=Timestamp("2017-12-19")),
    Coins.ETC: CoinInfo(coin=Coins.ETC, symbol="ETC-USD", 
                            start_date=Timestamp("2018-08-07")),
    
    # Privacy & Payments
    Coins.ZEC: CoinInfo(coin=Coins.ZEC, symbol="ZEC-USD", 
                            start_date=Timestamp("2020-12-05")),
    Coins.XLM: CoinInfo(coin=Coins.XLM, symbol="XLM-USD", 
                            start_date=Timestamp("2019-03-12")),
    
    # Enterprise & Other
    Coins.VET: CoinInfo(coin=Coins.VET, symbol="VET-USD", 
                            start_date=Timestamp("2023-11-01")),
    Coins.HBAR: CoinInfo(coin=Coins.HBAR, symbol="HBAR-USD", 
                            start_date=Timestamp("2023-02-16")),
    Coins.QNT: CoinInfo(coin=Coins.QNT, symbol="QNT-USD", 
                            start_date=Timestamp("2021-06-08")),
    Coins.ALGO: CoinInfo(coin=Coins.ALGO, symbol="ALGO-USD", 
                            start_date=Timestamp("2019-08-01")),
    Coins.EOS: CoinInfo(coin=Coins.EOS, symbol="EOS-USD", 
                            start_date=Timestamp("2019-05-23")),
    Coins.XTZ: CoinInfo(coin=Coins.XTZ, symbol="XTZ-USD", 
                            start_date=Timestamp("2019-08-05")),
    Coins.CHZ: CoinInfo(coin=Coins.CHZ, symbol="CHZ-USD", 
                            start_date=Timestamp("2021-06-24")),
    
    # Utility tokens
    Coins.BAT: CoinInfo(coin=Coins.BAT, symbol="BAT-USD", 
                            start_date=Timestamp("2021-04-28")),
    Coins.ONEINCH: CoinInfo(coin=Coins.ONEINCH, symbol="1INCH-USD", 
                            start_date=Timestamp("2021-04-27")),
    
    # Meme coins
    Coins.SHIB: CoinInfo(coin=Coins.SHIB, symbol="SHIB-USD", 
                            start_date=Timestamp("2021-09-09")),
    Coins.WIF: CoinInfo(coin=Coins.WIF, symbol="WIF-USD", 
                            start_date=Timestamp("2024-11-13")),
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