"""Tests for models integration with configuration loader."""

import json
import tempfile
from pathlib import Path
from typing import Dict

import pandas as pd
import pytest

from coinbase_data_fetcher.models import COIN_INFO, Coins


class TestModelsWithConfig:
    """Test models integration with configuration loading."""

    def test_coin_info_loaded_from_config(self) -> None:
        """Test that COIN_INFO is properly loaded from configuration."""
        # COIN_INFO should be populated
        assert len(COIN_INFO) > 0
        
        # Check that well-known coins are present
        assert Coins.BITCOIN in COIN_INFO
        assert Coins.ETHEREUM in COIN_INFO
        
        # Check structure of loaded data
        bitcoin_info = COIN_INFO[Coins.BITCOIN]
        assert bitcoin_info.symbol == "BTC-USD"
        assert bitcoin_info.start_date == pd.Timestamp("2015-07-20")
        assert bitcoin_info.logo_url == "https://cryptologos.cc/logos/thumbs/bitcoin.png"

    def test_all_enum_coins_have_config(self) -> None:
        """Test that all coins in the enum have configuration data."""
        missing_coins = []
        
        for coin in Coins:
            if coin not in COIN_INFO:
                missing_coins.append(coin.value)
        
        # All coins in the enum should have configuration
        assert len(missing_coins) == 0, f"Missing configuration for coins: {missing_coins}"

    def test_coin_info_consistency(self) -> None:
        """Test consistency between coin enum values and config."""
        for coin_enum, coin_info in COIN_INFO.items():
            # The coin field in CoinInfo should match the enum value
            assert coin_info.coin == coin_enum.value
            
            # All symbols should end with -USD
            assert coin_info.symbol.endswith("-USD")
            
            # Start dates should be valid timestamps
            assert isinstance(coin_info.start_date, pd.Timestamp)
            assert coin_info.start_date < pd.Timestamp.now()

    def test_config_can_be_extended(self) -> None:
        """Test that new coins can be added via config without code changes."""
        # Create a test config with an additional coin
        test_config = {
            "coins": [
                {
                    "id": "bitcoin",
                    "symbol": "BTC-USD", 
                    "start_date": "2015-07-20",
                    "category": "Major cryptocurrencies"
                },
                {
                    "id": "new-test-coin",
                    "symbol": "NEW-USD",
                    "start_date": "2025-01-01",
                    "category": "Test"
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(test_config, f)
            config_path = Path(f.name)
        
        # Load config from test file
        from coinbase_data_fetcher.config_loader import load_coins_config
        coins_config = load_coins_config(config_path)
        
        # Verify new coin is in config
        assert "new-test-coin" in coins_config
        assert coins_config["new-test-coin"]["symbol"] == "NEW-USD"
        
        # Clean up
        config_path.unlink()

    def test_coin_categories_from_config(self) -> None:
        """Test that coin categories are properly loaded."""
        from coinbase_data_fetcher.config_loader import get_coin_categories
        
        categories = get_coin_categories()
        
        # Check that expected categories exist
        assert "Major cryptocurrencies" in categories
        assert "DeFi tokens" in categories
        assert "Layer 1 & Layer 2" in categories
        
        # Check that coins are properly categorized
        assert "bitcoin" in categories["Major cryptocurrencies"]
        assert "ethereum" in categories["Major cryptocurrencies"]
        assert "uniswap" in categories["DeFi tokens"]