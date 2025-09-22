#!/usr/bin/env python3
"""Optimized script to update coins configuration using Coinbase data with parallel processing."""

import argparse
import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import pandas as pd
from tqdm.asyncio import tqdm

# Add parent directory to path to import our modules
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from coinbase_data_fetcher.config_loader import load_coins_config


# Known start dates for major coins
KNOWN_START_DATES = {
    "BTC-USD": ("2015-01-01", "2015-07-20"),  # (search_from, actual_start)
    "ETH-USD": ("2016-01-01", "2016-07-21"),
    "LTC-USD": ("2017-01-01", "2017-05-03"),
}

# Rate limiting: Coinbase allows 10 requests per second
RATE_LIMIT = 10  # requests per second
SEMAPHORE = asyncio.Semaphore(RATE_LIMIT)


async def rate_limited_request(session: aiohttp.ClientSession, url: str, params: dict = None) -> Optional[dict]:
    """Make a rate-limited request to the API."""
    async with SEMAPHORE:
        try:
            async with session.get(url, params=params, timeout=10) as response:
                if response.status == 200:
                    return await response.json()
                return None
        except Exception:
            return None


async def check_data_exists(session: aiohttp.ClientSession, product_id: str, date: datetime) -> bool:
    """Check if data exists for a product on a specific date."""
    url = f"https://api.exchange.coinbase.com/products/{product_id}/candles"
    
    # Request a small time window
    start = date.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(hours=1)
    
    params = {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "granularity": 3600  # 1 hour
    }
    
    data = await rate_limited_request(session, url, params)
    return data is not None and len(data) > 0


async def find_earliest_date_optimized(session: aiohttp.ClientSession, product_id: str) -> Optional[str]:
    """Find the earliest available date for a product using optimized binary search."""
    
    # Use known dates or smart defaults
    if product_id in KNOWN_START_DATES:
        search_from_str, actual_start = KNOWN_START_DATES[product_id]
        return actual_start
    
    # For all other coins, start searching from 2017 (after LTC)
    search_from = datetime(2017, 1, 1)
    end_date = datetime.now() - timedelta(days=1)
    
    # Quick check if we have recent data
    if not await check_data_exists(session, product_id, end_date):
        return None
    
    # Binary search between bounds
    left = search_from
    right = end_date
    earliest_with_data = None
    
    while (right - left).days > 1:
        mid = left + (right - left) / 2
        
        if await check_data_exists(session, product_id, mid):
            right = mid
            earliest_with_data = mid
        else:
            left = mid
    
    if earliest_with_data:
        return earliest_with_data.strftime("%Y-%m-%d")
    
    return None


async def get_coinbase_products() -> List[Dict]:
    """Fetch all available trading pairs from Coinbase."""
    url = "https://api.exchange.coinbase.com/products"
    
    async with aiohttp.ClientSession() as session:
        data = await rate_limited_request(session, url)
        return data or []


def filter_usd_pairs(products: List[Dict]) -> List[Dict]:
    """Filter products to only USD trading pairs."""
    usd_pairs = []
    seen_bases = set()
    
    for product in products:
        if product.get("quote_currency") == "USD" and product.get("trading_disabled") == False:
            base = product["base_currency"]
            if base not in seen_bases:
                seen_bases.add(base)
                usd_pairs.append({
                    "id": product["id"],
                    "symbol": base,
                    "display_name": product.get("display_name", ""),
                    "base_currency": base,
                    "status": product.get("status", ""),
                })
    
    return usd_pairs


def get_coin_id_from_symbol(symbol: str) -> str:
    """Convert a symbol like BTC to a coin ID like bitcoin."""
    symbol_to_id = {
        "BTC": "bitcoin",
        "ETH": "ethereum",
        "SOL": "solana",
        "XRP": "xrp",
        "ADA": "ada",
        "AVAX": "avalanche",
        "DOGE": "dogecoin",
        "DOT": "polkadot",
        "MATIC": "polygon",
        "LINK": "chainlink",
        "NEAR": "near",
        "ICP": "internet-computer",
        "ATOM": "cosmos",
        "APT": "aptos",
        "ARB": "arbitrum",
        "OP": "optimism",
        "SUI": "sui",
        "UNI": "uniswap",
        "AAVE": "aave",
        "CRV": "curve",
        "MKR": "maker",
        "COMP": "compound",
        "SNX": "synthetix",
        "LDO": "lido",
        "SUSHI": "sushiswap",
        "YFI": "yearn-finance",
        "BAL": "balancer",
        "PERP": "perpetual-protocol",
        "SAND": "sandbox",
        "MANA": "decentraland",
        "AXS": "axie-infinity",
        "IMX": "immutablex",
        "ENS": "ethereum-name-service",
        "BLUR": "blur",
        "APE": "apecoin",
        "FIL": "filecoin",
        "GRT": "the-graph",
        "LRC": "loopring",
        "ANKR": "ankr",
        "SKL": "skale",
        "MASK": "mask-network",
        "LTC": "litecoin",
        "BCH": "bitcoin-cash",
        "ETC": "ethereum-classic",
        "ZEC": "zcash",
        "XLM": "stellar",
        "VET": "vechain",
        "HBAR": "hedera",
        "QNT": "quant",
        "ALGO": "algorand",
        "EOS": "eos",
        "XTZ": "tezos",
        "CHZ": "chiliz",
        "BAT": "basic-attention-token",
        "1INCH": "1inch",
        "SHIB": "shiba-inu",
        "WIF": "dogwifhat"
    }
    
    return symbol_to_id.get(symbol, symbol.lower())


def get_category_for_coin(coin_id: str) -> str:
    """Determine category for a coin."""
    categories = {
        "Major cryptocurrencies": ["bitcoin", "ethereum"],
        "Stablecoins": ["usdc", "usdt", "dai", "tusd", "pax", "busd"],
        "Layer 1 & Layer 2": [
            "solana", "ada", "avalanche", "polkadot", "near", 
            "internet-computer", "cosmos", "aptos", "arbitrum", "optimism", 
            "sui", "algorand", "eos", "tezos", "hedera", "vechain"
        ],
        "DeFi tokens": [
            "uniswap", "aave", "curve", "maker", "compound",
            "synthetix", "lido", "sushiswap", "yearn-finance", 
            "balancer", "perpetual-protocol", "loopring", "1inch"
        ],
        "Gaming & Metaverse": [
            "sandbox", "decentraland", "axie-infinity", "immutablex",
            "ethereum-name-service", "blur", "apecoin"
        ],
        "Infrastructure & Web3": [
            "chainlink", "filecoin", "the-graph", "ankr", "skale", "mask-network"
        ],
        "Meme coins": ["dogecoin", "shiba-inu", "dogwifhat", "pepe", "floki"],
        "Privacy & Payments": ["zcash", "stellar", "xrp"],
        "Bitcoin forks & Classic": ["bitcoin-cash", "litecoin", "ethereum-classic"],
        "Utility tokens": ["basic-attention-token", "chiliz"],
        "Enterprise & Other": ["vechain", "hedera", "quant", "eos", "xrp"]
    }
    
    for category, coins in categories.items():
        if coin_id in coins:
            return category
    
    return "Other"


async def process_coin(session: aiohttp.ClientSession, pair: dict, existing_config: dict, find_dates: bool) -> Optional[dict]:
    """Process a single coin to get its configuration."""
    symbol = pair["symbol"]
    product_id = pair["id"]
    coin_id = get_coin_id_from_symbol(symbol)
    
    # Check if we already have a start date
    if coin_id in existing_config:
        start_date = existing_config[coin_id]["start_date"]
    elif find_dates:
        # Find the precise start date
        start_date = await find_earliest_date_optimized(session, product_id)
        if not start_date:
            return None  # Skip if we couldn't find data
    else:
        # Default to a recent date
        start_date = "2023-01-01"
    
    return {
        "id": coin_id,
        "symbol": product_id,
        "start_date": start_date,
        "category": get_category_for_coin(coin_id)
    }


async def process_all_coins(usd_pairs: List[dict], existing_config: dict, find_dates: bool) -> List[dict]:
    """Process all coins with parallel processing."""
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=20)) as session:
        # Create tasks for all coins
        tasks = [
            process_coin(session, pair, existing_config, find_dates)
            for pair in usd_pairs
        ]
        
        # Process with progress bar
        coins = []
        for coro in tqdm.as_completed(tasks, desc="Processing coins"):
            result = await coro
            if result:
                coins.append(result)
        
        return coins


def get_market_cap_data() -> Dict[str, float]:
    """Fetch market cap data from CoinGecko for sorting."""
    try:
        # Get top 500 coins by market cap
        import requests
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 250,
            "page": 1,
            "sparkline": False,
            "locale": "en"
        }
        
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        coins = response.json()
        
        # Build market cap lookup
        market_caps = {}
        for coin in coins:
            coin_id = coin["id"]
            market_cap = coin.get("market_cap", 0) or 0
            market_caps[coin_id] = market_cap
        
        return market_caps
    except Exception as e:
        print(f"Warning: Could not fetch market cap data: {e}")
        return {}


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Update coins configuration using Coinbase data (optimized)"
    )
    parser.add_argument(
        "--output", 
        type=Path,
        default=Path("coins_config.json"),
        help="Output file path (default: coins_config.json)"
    )
    parser.add_argument(
        "--no-find-dates",
        action="store_true",
        help="Don't find precise start dates (use existing or default)"
    )
    parser.add_argument(
        "--sort",
        choices=["market-cap", "alphabetical", "coinbase"],
        default="coinbase",
        help="Sort order: market-cap (by market cap), alphabetical (by symbol), or coinbase (default Coinbase order)"
    )
    
    args = parser.parse_args()
    
    print("Fetching products from Coinbase...")
    products = await get_coinbase_products()
    
    if not products:
        print("Failed to fetch products")
        return 1
    
    # Filter to USD pairs
    usd_pairs = filter_usd_pairs(products)
    print(f"Found {len(usd_pairs)} USD trading pairs")
    
    # Load existing config to preserve data
    try:
        existing_config = load_coins_config()
    except:
        existing_config = {}
    
    # Process all coins with parallel processing
    print(f"\nProcessing coins (parallel, find_dates={'Yes' if not args.no_find_dates else 'No'}):")
    coins = await process_all_coins(usd_pairs, existing_config, not args.no_find_dates)
    
    # Sort coins based on user preference
    if args.sort == "market-cap":
        print("\nFetching market cap data for sorting...")
        market_caps = get_market_cap_data()
        # Sort by market cap (descending), with unknown coins at the end
        coins.sort(key=lambda x: market_caps.get(x["id"], -1), reverse=True)
    elif args.sort == "alphabetical":
        # Sort alphabetically by symbol
        coins.sort(key=lambda x: x["symbol"])
    # else: keep Coinbase order (default)
    
    # Generate config
    config = {"coins": coins}
    
    with open(args.output, "w") as f:
        json.dump(config, f, indent=2)
    
    print(f"\nGenerated {args.output}")
    print(f"Total coins: {len(coins)}")
    print(f"Sort order: {args.sort}")
    print(f"Find dates: {'Yes (optimized)' if not args.no_find_dates else 'No (using existing/default)'}")
    
    # Show breakdown by category
    category_counts = {}
    for coin in coins:
        cat = coin["category"]
        category_counts[cat] = category_counts.get(cat, 0) + 1
    
    print("\nCoins by category:")
    for cat, count in sorted(category_counts.items()):
        print(f"  {cat}: {count}")
    
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))