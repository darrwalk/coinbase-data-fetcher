"""Tests for models module."""

import pandas as pd
import pytest
from coinbase_data_fetcher.models import CoinData, CoinDataModel, Coins, COIN_INFO
from coinbase_data_fetcher.config_loader import CoinInfo


class TestCoins:
    def test_coin_enum_values(self):
        # Test a sample of important coins
        assert Coins.BITCOIN == "bitcoin"
        assert Coins.ETHEREUM == "ethereum"
        assert Coins.SOLANA == "solana"
        assert Coins.LITECOIN == "litecoin"
        assert Coins.DOGECOIN == "dogecoin"
        assert Coins.WIF == "dogwifhat"
        assert Coins.XRP == "xrp"
        assert Coins.ADA == "ada"
        assert Coins.AVAX == "avalanche"
        assert Coins.DOT == "polkadot"
        assert Coins.MATIC == "polygon"
        assert Coins.UNI == "uniswap"
        assert Coins.ONEINCH == "1inch"
    
    def test_all_coins_in_coin_info(self):
        """Ensure all coins in enum have corresponding COIN_INFO entry."""
        for coin in Coins:
            assert coin in COIN_INFO, f"{coin} not found in COIN_INFO"
            info = COIN_INFO[coin]
            assert info.coin == coin
            assert info.symbol.endswith("-USD")
            assert isinstance(info.start_date, pd.Timestamp)


class TestCoinInfo:
    def test_coin_info_creation(self):
        info = CoinInfo(
            coin=Coins.BITCOIN,
            symbol="BTC-USD",
            start_date=pd.Timestamp("2023-01-01")
        )
        assert info.coin == Coins.BITCOIN
        assert info.symbol == "BTC-USD"
        assert info.logo_url == "https://cryptologos.cc/logos/thumbs/bitcoin.png"
    
    def test_coin_info_with_custom_logo(self):
        info = CoinInfo(
            coin=Coins.ETHEREUM,
            symbol="ETH-USD",
            start_date=pd.Timestamp("2023-01-01"),
            logo_url="https://example.com/logo.png"
        )
        assert info.logo_url == "https://example.com/logo.png"


class TestCoinDataModel:
    def test_default_values(self):
        model = CoinDataModel()
        assert model.coin == Coins.BITCOIN
        assert model.data_granularity == 3600
        assert model.price_interpolation == "Hi-Lo"
        
    def test_parse_timestamp(self):
        model = CoinDataModel(
            start_date="2023-01-01",
            end_date="2023-12-31"
        )
        assert isinstance(model.start_date, pd.Timestamp)
        assert isinstance(model.end_date, pd.Timestamp)
        assert model.start_date.tz is None
        assert model.end_date.tz is None
    
    def test_get_choices_granularity(self):
        choices = CoinDataModel.get_choices("data_granularity")
        assert 60 in choices
        assert 300 in choices
        assert 3600 in choices
        assert 900 in choices
    
    def test_get_choices_coin(self):
        choices = CoinDataModel.get_choices("coin")
        # Should return all coins as strings
        # Verify we have many coins
        assert len(choices) > 50  # We added 56 coins
        # Check some key coins are present
        assert "bitcoin" in choices
        assert "ethereum" in choices
        assert "avalanche" in choices
        assert "polygon" in choices
        assert "uniswap" in choices
        assert "1inch" in choices


class TestCoinData:
    def test_coin_data_creation(self):
        model = CoinDataModel(coin=Coins.ETHEREUM)
        coin_data = CoinData(model)
        assert coin_data.model.coin == Coins.ETHEREUM