"""Configuration loader for coin data."""

import json
from pathlib import Path
from typing import Dict, List, TYPE_CHECKING

import pandas as pd
from pandas import Timestamp
from pydantic import BaseModel

if TYPE_CHECKING:
    pass


class CoinInfo(BaseModel):
    coin: str  # We'll use string here to avoid circular import
    symbol: str
    start_date: Timestamp
    logo_url: str = ""
    
    model_config = {"arbitrary_types_allowed": True}
    
    def __init__(self, **data):
        super().__init__(**data)
        if self.logo_url == "":
            self.logo_url = f"https://cryptologos.cc/logos/thumbs/{self.coin}.png"


def load_coins_config(config_path: Path | str | None = None) -> Dict[str, dict]:
    """Load coins configuration from JSON file.
    
    Args:
        config_path: Path to the configuration file. If None, looks for coins_config.json
                    in the project root directory.
    
    Returns:
        Dictionary mapping coin IDs to their configuration.
    """
    if config_path is None:
        # Try to find the config file in the project root
        config_path = Path(__file__).parent.parent.parent / "coins_config.json"
    
    config_path = Path(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_path, "r") as f:
        config_data = json.load(f)
    
    return {coin["id"]: coin for coin in config_data["coins"]}


def load_coin_info(config_path: Path | str | None = None) -> Dict[str, CoinInfo]:
    """Load coin info from configuration file.
    
    Args:
        config_path: Path to the configuration file.
    
    Returns:
        Dictionary mapping coin IDs to CoinInfo instances.
    """
    coins_config = load_coins_config(config_path)
    coin_info = {}
    
    for coin_id, config in coins_config.items():
        coin_info[coin_id] = CoinInfo(
            coin=coin_id,
            symbol=config["symbol"],
            start_date=pd.Timestamp(config["start_date"]),
            logo_url=config.get("logo", "")
        )
    
    return coin_info


def get_coin_categories(config_path: Path | str | None = None) -> Dict[str, List[str]]:
    """Get coins grouped by category from configuration.
    
    Args:
        config_path: Path to the configuration file.
    
    Returns:
        Dictionary mapping category names to lists of coin IDs.
    """
    coins_config = load_coins_config(config_path)
    categories = {}
    
    for coin_id, config in coins_config.items():
        category = config.get("category", "Other")
        if category not in categories:
            categories[category] = []
        categories[category].append(coin_id)
    
    return categories