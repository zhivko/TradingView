"""
DEX Trade Fetchers
Module for fetching trade data from decentralized exchanges (DEXes)
"""
import asyncio
import aiohttp
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import time
import websockets

# Import Hyperliquid SDK
from hyperliquid import HyperliquidSync

logger = logging.getLogger(__name__)

class DEXTradeFetcher:
    """Base class for DEX trade fetchers"""

    def __init__(self, exchange_name: str, api_base_url: str, rate_limit: int = 10):
        self.exchange_name = exchange_name
        self.api_base_url = api_base_url
        self.rate_limit = rate_limit
        self.last_request_time = 0
        self.request_interval = 1.0 / rate_limit  # seconds between requests

    async def _rate_limit_wait(self):
        """Enforce rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.request_interval:
            await asyncio.sleep(self.request_interval - time_since_last)
        self.last_request_time = time.time()

    async def fetch_trades(self, symbol: str, since: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Fetch trades for a symbol. Override in subclasses."""
        raise NotImplementedError("Subclasses must implement fetch_trades")


class HyperliquidTradeFetcher(DEXTradeFetcher):
    """Hyperliquid DEX trade fetcher"""

    def __init__(self):
        super().__init__("hyperliquid", "https://api.hyperliquid.xyz", rate_limit=10)
        self.info = HyperliquidSync()
        self.websocket_url = "wss://api.hyperliquid.xyz/ws"
        self.collected_trades = []
        self.trade_event = asyncio.Event()
        # Set mainnet URL
        self.info.urls['api']['public'] = 'https://api.hyperliquid.xyz'
        self.info.urls['api']['private'] = 'https://api.hyperliquid.xyz'

    async def fetch_trades(self, symbol: str, since: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch trades from Hyperliquid using WebSocket streaming
        Note: Hyperliquid WebSocket only provides real-time trades, not historical data.
        For gap filling, we collect real-time trades but cannot paginate historical data.
        API: https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket
        """
        await self._rate_limit_wait()

        try:
            # Hyperliquid uses coin names without USDT suffix
            coin = symbol.replace('USDT', '')

            # For historical data (gap filling), Hyperliquid WebSocket doesn't support it
            # We can only collect real-time trades, which may not be sufficient for gap filling
            if since:
                logger.warning(f"Hyperliquid WebSocket cannot fetch historical trades since {since}. Only real-time data available.")

            # Use WebSocket to collect real-time trades
            trades = await self._collect_trades_via_websocket(coin, limit, timeout=10)

            if trades:
                logger.info(f"Fetched {len(trades)} real-time trades from Hyperliquid for {symbol}")
                return trades

            logger.warning(f"No trade data collected from Hyperliquid WebSocket for {symbol}")
            return []

        except Exception as e:
            logger.error(f"Error fetching trades from Hyperliquid: {e}")
            return []

    async def _collect_trades_via_websocket(self, coin: str, limit: int, timeout: int = 10) -> List[Dict[str, Any]]:
        """Collect trades using WebSocket streaming"""
        self.collected_trades = []
        self.trade_event.clear()

        async def websocket_handler():
            try:
                async with websockets.connect(self.websocket_url) as websocket:
                    # Subscribe to trades channel
                    subscription = {
                        "type": "subscribe",
                        "channel": "trades",
                        "coin": coin
                    }
                    await websocket.send(json.dumps(subscription))

                    while len(self.collected_trades) < limit:
                        try:
                            message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                            data = json.loads(message)

                            if isinstance(data, dict) and 'channel' in data:
                                if data['channel'] == 'trades' and 'data' in data:
                                    for trade in data['data']:
                                        if len(self.collected_trades) >= limit:
                                            break

                                        # Convert Hyperliquid trade format to standardized format
                                        try:
                                            standardized_trade = {
                                                "id": str(trade.get("hash", "")),
                                                "timestamp": int(trade.get("time", 0) / 1000000),  # microseconds to seconds
                                                "datetime": datetime.fromtimestamp(int(trade.get("time", 0) / 1000000), timezone.utc).isoformat(),
                                                "symbol": coin + "USDT",  # Convert back to USDT format
                                                "side": trade.get("side", "").lower(),
                                                "price": float(trade.get("px", 0)),
                                                "amount": float(trade.get("sz", 0)),
                                                "cost": float(trade.get("px", 0)) * float(trade.get("sz", 0)),
                                                "fee": 0.0,
                                                "exchange": "hyperliquid",
                                                "info": trade
                                            }
                                            self.collected_trades.append(standardized_trade)
                                        except (KeyError, ValueError, TypeError) as e:
                                            logger.warning(f"Error parsing Hyperliquid WebSocket trade: {e}")
                                            continue

                                    if len(self.collected_trades) >= limit:
                                        self.trade_event.set()
                                        break

                        except asyncio.TimeoutError:
                            continue
                        except websockets.exceptions.ConnectionClosed:
                            break

            except Exception as e:
                logger.error(f"Error in WebSocket connection: {e}")

        try:
            # Start WebSocket handler with timeout
            await asyncio.wait_for(websocket_handler(), timeout=timeout)

            return self.collected_trades[:limit]

        except asyncio.TimeoutError:
            logger.debug(f"WebSocket collection timeout after {timeout}s for {coin}")
            return self.collected_trades[:limit]
        except Exception as e:
            logger.error(f"Error in WebSocket trade collection: {e}")
            return []


class AsterTradeFetcher(DEXTradeFetcher):
    """Aster DEX trade fetcher"""

    def __init__(self):
        # Aster might not be active or API might be different
        super().__init__("aster", "https://api.aster.network", rate_limit=5)

    async def fetch_trades(self, symbol: str, since: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch trades from Aster with pagination support
        API: https://sapi.asterdex.com/api/v1/trades
        """
        await self._rate_limit_wait()

        try:
            async with aiohttp.ClientSession() as session:
                # Aster uses different symbol format - convert BTCUSDT to BTC_USDT
                formatted_symbol = symbol.replace('USDT', '_USDT')

                all_trades = []
                current_since = since
                remaining_limit = limit
                max_api_limit = 1000  # Aster API limit per request

                while remaining_limit > 0:
                    # Use the correct Aster API endpoint from documentation
                    endpoint = f"https://sapi.asterdex.com/api/v1/trades"

                    params = {
                        "symbol": formatted_symbol,
                        "limit": min(remaining_limit, max_api_limit)
                    }

                    if current_since:
                        params["startTime"] = current_since * 1000  # Convert to milliseconds

                    async with session.get(endpoint, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                        if response.status != 200:
                            logger.debug(f"Aster API error: {response.status}")
                            break

                        data = await response.json()

                        batch_trades = []
                        for trade in data:
                            try:
                                # Convert Aster trade format to standardized format
                                standardized_trade = {
                                    "id": str(trade.get("id", "")),
                                    "timestamp": trade.get("time", 0) // 1000,  # Convert from milliseconds to seconds
                                    "datetime": datetime.fromtimestamp(trade.get("time", 0) // 1000, timezone.utc).isoformat(),
                                    "symbol": symbol,
                                    "side": "buy" if trade.get("isBuyerMaker", False) else "sell",
                                    "price": float(trade.get("price", 0)),
                                    "amount": float(trade.get("qty", 0)),
                                    "cost": float(trade.get("price", 0)) * float(trade.get("qty", 0)),
                                    "fee": 0.0,  # Fee not provided in public trades
                                    "exchange": "aster",
                                    "info": trade
                                }
                                batch_trades.append(standardized_trade)
                            except (KeyError, ValueError, TypeError) as e:
                                logger.warning(f"Error parsing Aster trade: {e}")
                                continue

                        # Sort trades by timestamp to ensure chronological order
                        batch_trades.sort(key=lambda x: x['timestamp'])

                        # Add to all trades
                        all_trades.extend(batch_trades)

                        # Check if we hit the API limit (indicating more data available)
                        if len(data) == params["limit"]:
                            # Update current_since to last trade's timestamp + 1 for next batch
                            if batch_trades:
                                current_since = batch_trades[-1]['timestamp'] + 1
                                remaining_limit -= len(batch_trades)
                            else:
                                break  # No more trades in this batch
                        else:
                            # No more data available
                            break

                        # Prevent infinite loops and memory issues
                        if len(all_trades) >= limit:
                            break

                # Return only up to the requested limit
                final_trades = all_trades[:limit]

                if final_trades:
                    logger.info(f"Fetched {len(final_trades)} trades from Aster for {symbol} (paginated: {len(all_trades) > len(final_trades)})")
                    return final_trades

                logger.warning(f"No valid trade data from Aster for {symbol}")
                return []

        except Exception as e:
            logger.error(f"Error fetching trades from Aster: {e}")
            return []


class DydxTradeFetcher(DEXTradeFetcher):
    """dYdX DEX trade fetcher"""

    def __init__(self):
        super().__init__("dxdy", "https://api.dydx.exchange", rate_limit=10)

    async def fetch_trades(self, symbol: str, since: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch trades from dYdX with pagination support
        API: https://indexer.dydx.trade/v4/trades/perpetualMarket/{market}
        """
        await self._rate_limit_wait()

        try:
            async with aiohttp.ClientSession() as session:
                # dYdX uses MARKET-USD format
                market = symbol.replace('USDT', '-USD')

                all_trades = []
                current_since = since
                remaining_limit = limit
                max_api_limit = 100  # dYdX API limit per request

                while remaining_limit > 0:
                    # Use the correct v4 API endpoint for perpetual market trades
                    endpoint = f"https://indexer.dydx.trade/v4/trades/perpetualMarket/{market}"

                    params = {
                        "limit": min(remaining_limit, max_api_limit)
                    }

                    # dYdX API uses 'createdBeforeOrAt' for time filtering
                    if current_since:
                        # Convert timestamp to ISO format for dYdX API
                        since_dt = datetime.fromtimestamp(current_since, timezone.utc)
                        params["createdBeforeOrAt"] = since_dt.isoformat() + "Z"

                    async with session.get(endpoint, params=params) as response:
                        if response.status != 200:
                            logger.debug(f"dYdX API error: {response.status}")
                            break

                        data = await response.json()

                        batch_trades = []
                        # dYdX API returns trades directly as a list, not wrapped in "trades" key
                        trades_list = data if isinstance(data, list) else data.get("trades", [])
                        for trade in trades_list:
                            try:
                                # Convert dYdX trade format to standardized format
                                # Note: dYdX v4 API returns trades with different field names
                                trade_timestamp = int(datetime.fromisoformat(trade.get("createdAt", "").replace('Z', '+00:00')).timestamp())

                                # Skip trades that are before our since timestamp
                                if current_since and trade_timestamp < current_since:
                                    continue

                                standardized_trade = {
                                    "id": trade.get("id", ""),
                                    "timestamp": trade_timestamp,
                                    "datetime": trade.get("createdAt", ""),
                                    "symbol": symbol,
                                    "side": trade.get("side", "").lower(),
                                    "price": float(trade.get("price", 0)),
                                    "amount": float(trade.get("size", 0)),
                                    "cost": float(trade.get("price", 0)) * float(trade.get("size", 0)),
                                    "fee": 0.0,  # Fee not provided in public trades
                                    "exchange": "dxdy",
                                    "info": trade
                                }
                                batch_trades.append(standardized_trade)
                            except (KeyError, ValueError, TypeError) as e:
                                logger.warning(f"Error parsing dYdX trade: {e}")
                                continue

                        # Sort trades by timestamp to ensure chronological order
                        batch_trades.sort(key=lambda x: x['timestamp'])

                        # Add to all trades
                        all_trades.extend(batch_trades)

                        # Check if we hit the API limit (indicating more data available)
                        trades_returned = len(trades_list)
                        if trades_returned == params["limit"]:
                            # Update current_since to last trade's timestamp for next batch
                            if batch_trades:
                                current_since = batch_trades[-1]['timestamp']
                                remaining_limit -= len(batch_trades)
                            else:
                                break  # No more trades in this batch
                        else:
                            # No more data available
                            break

                        # Prevent infinite loops and memory issues
                        if len(all_trades) >= limit:
                            break

                # Return only up to the requested limit
                final_trades = all_trades[:limit]

                if final_trades:
                    logger.info(f"Fetched {len(final_trades)} trades from dYdX for {symbol} (paginated: {len(all_trades) > len(final_trades)})")
                    return final_trades

                logger.warning(f"No valid trade data from dYdX for {symbol}")
                return []

        except Exception as e:
            logger.error(f"Error fetching trades from dYdX: {e}")
            return []


class ApexTradeFetcher(DEXTradeFetcher):
    """Apex DEX trade fetcher"""

    def __init__(self):
        super().__init__("apex", "https://api.apex.pro/v2", rate_limit=10)

    async def fetch_trades(self, symbol: str, since: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch trades from Apex Pro DEX
        Note: Apex Pro trade API integration pending - currently returns empty list
        """
        await self._rate_limit_wait()

        # TODO: Implement Apex Pro trade fetching using apexomni SDK or REST API
        # For now, return empty list to prevent errors
        logger.debug(f"Apex Pro trade fetching not yet implemented for {symbol}")
        return []


# Factory function to get the appropriate DEX fetcher
def get_dex_fetcher(exchange_id: str) -> Optional[DEXTradeFetcher]:
    """Factory function to get DEX trade fetcher by exchange ID"""
    fetchers = {
        "hyperliquid": HyperliquidTradeFetcher,
        "aster": AsterTradeFetcher,
        "dxdy": DydxTradeFetcher,
        "apex": ApexTradeFetcher
    }

    fetcher_class = fetchers.get(exchange_id.lower())
    if fetcher_class:
        return fetcher_class()
    return None


# Note: Mock data generation removed as per requirements - only real API data should be used


async def fetch_dex_trades(exchange_id: str, symbol: str, since: Optional[int] = None, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Convenience function to fetch trades from any supported DEX

    Args:
        exchange_id: DEX exchange identifier (hyperliquid, aster, dxdy)
        symbol: Trading symbol (e.g., BTCUSDT)
        since: Timestamp to fetch trades since (optional)
        limit: Maximum number of trades to fetch

    Returns:
        List of standardized trade dictionaries
    """
    fetcher = get_dex_fetcher(exchange_id)
    if not fetcher:
        logger.error(f"No DEX fetcher available for {exchange_id}")
        return []

    trades = await fetcher.fetch_trades(symbol, since, limit)

    # Return actual trades or empty list - no mock data
    return trades


# Test functions
async def test_dex_fetchers():
    """Test all DEX fetchers using the convenience function"""
    exchanges = ["hyperliquid", "dxdy", "aster"]

    for exchange in exchanges:
        print(f"\n--- Testing {exchange.upper()} ---")
        trades = await fetch_dex_trades(exchange, "BTCUSDT", limit=5)
        print(f"{exchange.upper()} BTCUSDT trades: {len(trades)}")
        if trades:
            print(f"Sample trade: {trades[0]}")
            print(f"Trade has mock data: {trades[0].get('info', {}).get('mock', False)}")
        else:
            print(f"No trades fetched from {exchange}")


if __name__ == "__main__":
    # Run tests
    asyncio.run(test_dex_fetchers())