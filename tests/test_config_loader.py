"""Tests for configuration loader."""

import json
import tempfile
from pathlib import Path
from typing import Dict

import pandas as pd
import pytest

from coinbase_data_fetcher.config_loader import (
    CoinInfo,
    get_coin_categories,
    load_coin_info,
    load_coins_config,
)


class TestConfigLoader:
    """Test configuration loading functionality."""

    @pytest.fixture
    def sample_config(self) -> Dict[str, list]:
        """Sample configuration data."""
        return {
            "coins": [
                {
                    "id": "bitcoin",
                    "symbol": "BTC-USD",
                    "start_date": "2015-07-20",
                    "category": "Major cryptocurrencies"
                },
                {
                    "id": "ethereum",
                    "symbol": "ETH-USD",
                    "start_date": "2016-07-21",
                    "category": "Major cryptocurrencies"
                },
                {
                    "id": "uniswap",
                    "symbol": "UNI-USD",
                    "start_date": "2020-09-17",
                    "category": "DeFi tokens"
                }
            ]
        }

    @pytest.fixture
    def config_file(self, sample_config) -> Path:
        """Create a temporary config file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(sample_config, f)
            return Path(f.name)

    def test_load_coins_config(self, config_file: Path, sample_config: Dict[str, list]) -> None:
        """Test loading coins configuration from JSON file."""
        result = load_coins_config(config_file)
        
        assert isinstance(result, dict)
        assert len(result) == 3
        assert "bitcoin" in result
        assert "ethereum" in result
        assert "uniswap" in result
        
        # Check bitcoin config
        assert result["bitcoin"]["symbol"] == "BTC-USD"
        assert result["bitcoin"]["start_date"] == "2015-07-20"
        assert result["bitcoin"]["category"] == "Major cryptocurrencies"

    def test_load_coins_config_file_not_found(self) -> None:
        """Test loading config with non-existent file."""
        with pytest.raises(FileNotFoundError):
            load_coins_config("/path/to/nonexistent/file.json")

    def test_load_coin_info(self, config_file: Path) -> None:
        """Test loading coin info objects."""
        result = load_coin_info(config_file)
        
        assert isinstance(result, dict)
        # Note: This will only include coins that have corresponding enum values
        for coin_id, coin_info in result.items():
            assert isinstance(coin_info, CoinInfo)
            assert isinstance(coin_info.symbol, str)
            assert isinstance(coin_info.start_date, pd.Timestamp)
            assert isinstance(coin_info.logo_url, str)
            assert coin_info.logo_url.startswith("https://cryptologos.cc/logos/thumbs/")

    def test_coin_info_model(self) -> None:
        """Test CoinInfo model creation and validation."""
        coin_info = CoinInfo(
            coin="test-coin",
            symbol="TEST-USD",
            start_date=pd.Timestamp("2024-01-01")
        )
        
        assert coin_info.coin == "test-coin"
        assert coin_info.symbol == "TEST-USD"
        assert coin_info.start_date == pd.Timestamp("2024-01-01")
        assert coin_info.logo_url == "https://cryptologos.cc/logos/thumbs/test-coin.png"

    def test_coin_info_custom_logo(self) -> None:
        """Test CoinInfo with custom logo URL."""
        coin_info = CoinInfo(
            coin="test-coin",
            symbol="TEST-USD",
            start_date=pd.Timestamp("2024-01-01"),
            logo_url="https://example.com/custom-logo.png"
        )
        
        assert coin_info.logo_url == "https://example.com/custom-logo.png"

    def test_get_coin_categories(self, config_file: Path) -> None:
        """Test getting coins grouped by category."""
        result = get_coin_categories(config_file)
        
        assert isinstance(result, dict)
        assert len(result) == 2  # Major cryptocurrencies and DeFi tokens
        assert "Major cryptocurrencies" in result
        assert "DeFi tokens" in result
        
        assert result["Major cryptocurrencies"] == ["bitcoin", "ethereum"]
        assert result["DeFi tokens"] == ["uniswap"]

    def test_load_coins_config_from_default_path(self) -> None:
        """Test loading config from default path."""
        # This test checks if the default path works when file exists
        result = load_coins_config()
        
        # Should load the actual coins_config.json from project root
        assert isinstance(result, dict)
        assert len(result) > 0
        assert "bitcoin" in result

    def test_integration_with_actual_config(self) -> None:
        """Test integration with actual coins_config.json file."""
        coin_info = load_coin_info()
        
        # Check that some expected coins are present
        assert len(coin_info) > 0
        
        # Check structure of loaded data
        for coin_id, info in coin_info.items():
            assert isinstance(info, CoinInfo)
            assert isinstance(info.symbol, str)
            assert isinstance(info.start_date, pd.Timestamp)
            assert info.symbol.endswith("-USD")