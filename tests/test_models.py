"""Tests for models module."""

import pandas as pd
import pytest
from coinbase_data_fetcher.models import CoinData, CoinDataModel, CoinInfo, Coins, COIN_INFO


class TestCoins:
    def test_coin_enum_values(self):
        assert Coins.BITCOIN == "bitcoin"
        assert Coins.ETHEREUM == "ethereum"
        assert Coins.SOLANA == "solana"
        assert Coins.LITECOIN == "litecoin"
        assert Coins.DOGECOIN == "dogecoin"
        assert Coins.WIF == "dogwifhat"
        assert Coins.XRP == "xrp"
        assert Coins.ADA == "ada"
    
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
        expected_coins = [
            "bitcoin", "ethereum", "solana", "litecoin", 
            "dogecoin", "dogwifhat", "xrp", "ada"
        ]
        assert set(choices) == set(expected_coins)
        assert len(choices) == len(expected_coins)


class TestCoinData:
    def test_coin_data_creation(self):
        model = CoinDataModel(coin=Coins.ETHEREUM)
        coin_data = CoinData(model)
        assert coin_data.model.coin == Coins.ETHEREUM