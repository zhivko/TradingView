"""
WebSocket-based real-time trade streaming for CEX exchanges
Replaces REST API polling with continuous WebSocket connections
"""

import asyncio
import json
import websockets
import aiohttp
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timezone
from config import SUPPORTED_EXCHANGES, SUPPORTED_SYMBOLS
import logging

logger = logging.getLogger(__name__)


class WebSocketTradeManager:
    """Base class for WebSocket trade stream managers"""

    def __init__(self, exchange_id: str, exchange_config: dict, app=None):
        self.exchange_id = exchange_id
        self.exchange_config = exchange_config
        self.name = exchange_config.get('name', exchange_id)
        self.symbol_mappings = exchange_config.get('symbols', {})
        self.websocket_url = self._get_websocket_url()
        self.connection = None
        self.is_running = False
        self.reconnect_attempts = 0
        self.max_reconnect_attempts = 5
        self.reconnect_delay = 5
        self.app = app  # Store app reference for WebSocket notifications

        # Trade aggregation buffers
        self.trade_buffers: Dict[str, List[Dict]] = {}
        self.aggregation_interval = 60  # 1 minute bars

    def _get_websocket_url(self) -> str:
        """Override in subclasses to return exchange-specific WebSocket URL"""
        raise NotImplementedError

    def _get_subscription_message(self, symbols: List[str]) -> str:
        """Override in subclasses to return exchange-specific subscription message"""
        raise NotImplementedError

    def _parse_trade_message(self, message: str) -> Optional[Dict[str, Any]]:
        """Override in subclasses to parse exchange-specific trade message format"""
        raise NotImplementedError

    async def _handle_trade(self, trade: Dict[str, Any], internal_symbol: str) -> None:
        """Handle incoming trade data"""
        try:
            # Standardize trade format
            standardized_trade = {
                "id": str(trade.get("id", trade.get("trade_id", ""))),
                "timestamp": trade["timestamp"],
                "datetime": trade.get("datetime", datetime.fromtimestamp(trade["timestamp"], timezone.utc).isoformat()),
                "symbol": internal_symbol,
                "side": trade.get("side", "buy"),
                "price": float(trade["price"]),
                "amount": float(trade["amount"]),
                "cost": float(trade.get("cost", trade["price"] * trade["amount"])),
                "fee": 0.0,
                "exchange": self.name,
                "info": trade
            }

            # Cache individual trade
            await cache_individual_trades([standardized_trade], self.name, internal_symbol, self.app)

            # Add to aggregation buffer
            if internal_symbol not in self.trade_buffers:
                self.trade_buffers[internal_symbol] = []
            self.trade_buffers[internal_symbol].append(standardized_trade)

            # Check if we should aggregate (every aggregation_interval seconds)
            current_time = int(datetime.now(timezone.utc).timestamp())
            buffer_start_time = min(t['timestamp'] for t in self.trade_buffers[internal_symbol]) if self.trade_buffers[internal_symbol] else current_time

            if current_time - buffer_start_time >= self.aggregation_interval:
                await self._aggregate_and_publish(internal_symbol)

            # Notify clients of new trade
            await notify_clients_of_new_trade(internal_symbol, self.exchange_id, standardized_trade)

        except Exception as e:
            logger.error(f"Error handling trade for {internal_symbol} on {self.exchange_id}: {e}")

    async def _aggregate_and_publish(self, symbol: str) -> None:
        """Aggregate buffered trades and publish trade bars"""
        try:
            if symbol not in self.trade_buffers or not self.trade_buffers[symbol]:
                return

            trades = self.trade_buffers[symbol]
            if not trades:
                return

            # Aggregate trades into bars
            trade_bars = await aggregate_trades_from_redis(self.name, symbol,
                                                          min(t['timestamp'] for t in trades),
                                                          max(t['timestamp'] for t in trades),
                                                          self.aggregation_interval)

            if trade_bars:
                # Publish recent bars (within last 2 minutes)
                current_ts = int(datetime.now(timezone.utc).timestamp())
                for bar in trade_bars:
                    if current_ts - bar['time'] <= 120:  # Within last 2 minutes
                        await publish_trade_bar(symbol, self.exchange_id, bar)

            # Clear buffer
            self.trade_buffers[symbol] = []

        except Exception as e:
            logger.error(f"Error aggregating trades for {symbol} on {self.exchange_id}: {e}")

    async def start(self) -> None:
        """Start the WebSocket connection and trade streaming"""
        logger.info(f"🚀 Starting WebSocket trade stream for {self.name} ({self.exchange_id})")
        self.is_running = True

        while self.is_running and self.reconnect_attempts < self.max_reconnect_attempts:
            try:
                # Get symbols to subscribe to
                symbols_to_subscribe = []
                for internal_symbol in self.symbol_mappings.keys():
                    if internal_symbol in SUPPORTED_SYMBOLS:
                        symbols_to_subscribe.append(internal_symbol)

                if not symbols_to_subscribe:
                    logger.warning(f"No supported symbols found for {self.exchange_id}")
                    return

                logger.info(f"📡 Connecting to {self.websocket_url} for symbols: {symbols_to_subscribe}")

                async with websockets.connect(self.websocket_url) as websocket:
                    self.connection = websocket
                    self.reconnect_attempts = 0  # Reset on successful connection

                    # Start ping task for OKX
                    if self.exchange_id.lower() == 'okx':
                        self.ping_task = asyncio.create_task(self._send_ping(websocket))

                    # Send subscription message
                    subscription_msg = self._get_subscription_message(symbols_to_subscribe)
                    await websocket.send(subscription_msg)
                    logger.info(f"✅ Subscribed to trade streams for {len(symbols_to_subscribe)} symbols on {self.exchange_id}")

                    # Listen for messages
                    async for message in websocket:
                        try:
                            trade_data = self._parse_trade_message(message)
                            if trade_data:
                                # Find which internal symbol this trade corresponds to
                                for internal_symbol, exchange_symbol in self.symbol_mappings.items():
                                    if trade_data.get('symbol') == exchange_symbol or trade_data.get('symbol') == internal_symbol:
                                        await self._handle_trade(trade_data, internal_symbol)
                                        break

                        except json.JSONDecodeError:
                            continue  # Skip non-JSON messages
                        except Exception as e:
                            logger.error(f"Error processing message from {self.exchange_id}: {e}")
                            continue

            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"WebSocket connection closed for {self.exchange_id}, attempting reconnect...")
            except Exception as e:
                logger.error(f"WebSocket error for {self.exchange_id}: {e}")

            if self.is_running:
                self.reconnect_attempts += 1
                if self.reconnect_attempts < self.max_reconnect_attempts:
                    logger.info(f"🔄 Reconnecting {self.exchange_id} in {self.reconnect_delay}s (attempt {self.reconnect_attempts}/{self.max_reconnect_attempts})")
                    await asyncio.sleep(self.reconnect_delay)
                    self.reconnect_delay = min(self.reconnect_delay * 2, 60)  # Exponential backoff, max 60s
                else:
                    logger.error(f"❌ Max reconnection attempts reached for {self.exchange_id}")

        logger.info(f"🛑 Stopped WebSocket trade stream for {self.name} ({self.exchange_id})")

    async def stop(self) -> None:
        """Stop the WebSocket connection"""
        logger.info(f"🛑 Stopping WebSocket trade stream for {self.name} ({self.exchange_id})")
        self.is_running = False
        if self.ping_task:
            self.ping_task.cancel()
            try:
                await self.ping_task
            except asyncio.CancelledError:
                pass
        if self.connection:
            await self.connection.close()


class BinanceWebSocketManager(WebSocketTradeManager):
    """Binance WebSocket trade stream manager"""

    def __init__(self, exchange_id: str, exchange_config: dict, app=None):
        super().__init__(exchange_id, exchange_config, app)
        self.ping_task = None
        self.ping_interval = exchange_config.get('websocket_ping_interval', 30)

    def _get_websocket_url(self) -> str:
        return "wss://stream.binance.com:9443/ws"

    def _get_subscription_message(self, symbols: List[str]) -> str:
        # Convert internal symbols to Binance format - symbols should already be in lowercase
        streams = []
        for symbol in symbols:
            exchange_symbol = self.symbol_mappings.get(symbol, symbol.lower())
            streams.append(f"{exchange_symbol.lower()}@trade")
            
        logger.info(f"Binance subscription message for streams: {streams}")
        return json.dumps({
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        })

    def _parse_trade_message(self, message: str) -> Optional[Dict[str, Any]]:
        try:
            data = json.loads(message)

            # Skip subscription confirmations
            if 'result' in data or 'id' in data:
                return None

            # Check for error messages
            if 'error' in data:
                logger.error(f"Binance WebSocket error: {data['error']}")
                return None

            # Parse trade data
            if 'stream' in data and 'data' in data:
                trade = data['data']
                if trade.get('e') == 'trade':  # Trade event
                    return {
                        'id': str(trade['t']),  # Trade ID
                        'timestamp': int(trade['T'] / 1000),  # Convert ms to seconds
                        'datetime': datetime.fromtimestamp(int(trade['T'] / 1000), timezone.utc).isoformat(),
                        'symbol': trade['s'],  # Symbol
                        'side': 'buy' if trade['m'] else 'sell',  # Is buyer maker
                        'price': float(trade['p']),
                        'amount': float(trade['q']),
                        'cost': float(trade['p']) * float(trade['q'])
                    }

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON from Binance: {e}")
        except Exception as e:
            logger.error(f"Error parsing Binance trade message: {e}")

        return None

    async def start(self) -> None:
        """Override to add ping mechanism for Binance"""
        logger.info(f"🚀 Starting Binance WebSocket trade stream for {self.name}")
        self.is_running = True

        while self.is_running and self.reconnect_attempts < self.max_reconnect_attempts:
            try:
                # Get symbols to subscribe to
                symbols_to_subscribe = []
                for internal_symbol in self.symbol_mappings.keys():
                    if internal_symbol in SUPPORTED_SYMBOLS:
                        symbols_to_subscribe.append(internal_symbol)

                if not symbols_to_subscribe:
                    logger.warning(f"No supported symbols found for {self.exchange_id}")
                    return

                logger.info(f"📡 Connecting to {self.websocket_url} for symbols: {symbols_to_subscribe}")

                async with websockets.connect(
                    self.websocket_url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5
                ) as websocket:
                    self.connection = websocket
                    self.reconnect_attempts = 0  # Reset on successful connection

                    # Start ping task for Binance
                    self.ping_task = asyncio.create_task(self._send_ping(websocket))

                    # Send subscription message
                    subscription_msg = self._get_subscription_message(symbols_to_subscribe)
                    await websocket.send(subscription_msg)
                    logger.info(f"✅ Subscribed to trade streams for {len(symbols_to_subscribe)} symbols on {self.exchange_id}")

                    # Listen for messages
                    async for message in websocket:
                        try:
                            trade_data = self._parse_trade_message(message)
                            if trade_data:
                                # Find which internal symbol this trade corresponds to
                                for internal_symbol, exchange_symbol in self.symbol_mappings.items():
                                    if trade_data.get('symbol') == exchange_symbol or trade_data.get('symbol') == internal_symbol:
                                        await self._handle_trade(trade_data, internal_symbol)
                                        break

                        except websockets.exceptions.ConnectionClosed:
                            logger.warning(f"Binance WebSocket connection closed during message processing")
                            raise
                        except json.JSONDecodeError:
                            logger.debug(f"Received non-JSON message from Binance: {message[:100]}...")
                            continue
                        except Exception as e:
                            logger.error(f"Error processing message from {self.exchange_id}: {e}")
                            continue

            except websockets.exceptions.ConnectionClosed:
                logger.warning(f"WebSocket connection closed for {self.exchange_id}, attempting reconnect...")
            except websockets.exceptions.InvalidURI:
                logger.error(f"Invalid WebSocket URI for {self.exchange_id}: {self.websocket_url}")
                raise
            except Exception as e:
                logger.error(f"WebSocket error for {self.exchange_id}: {e}")

            if self.is_running:
                self.reconnect_attempts += 1
                if self.reconnect_attempts < self.max_reconnect_attempts:
                    logger.info(f"🔄 Reconnecting {self.exchange_id} in {self.reconnect_delay}s (attempt {self.reconnect_attempts}/{self.max_reconnect_attempts})")
                    await asyncio.sleep(self.reconnect_delay)
                    self.reconnect_delay = min(self.reconnect_delay * 2, 60)  # Exponential backoff, max 60s
                else:
                    logger.error(f"❌ Max reconnection attempts reached for {self.exchange_id}")

        logger.info(f"🛑 Stopped WebSocket trade stream for {self.name} ({self.exchange_id})")

    async def _send_ping(self, websocket) -> None:
        """Send periodic ping messages to Binance WebSocket to keep connection alive"""
        try:
            while self.is_running:
                await asyncio.sleep(self.ping_interval)
                if self.is_running and websocket.open:
                    try:
                        await websocket.ping()
                        logger.debug(f"Sent ping to Binance WebSocket")
                    except Exception as e:
                        logger.error(f"Failed to send ping to Binance: {e}")
                        break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in Binance ping task: {e}")

    async def stop(self) -> None:
        """Stop the WebSocket connection"""
        logger.info(f"🛑 Stopping Binance WebSocket trade stream for {self.name}")
        self.is_running = False
        if self.ping_task:
            self.ping_task.cancel()
            try:
                await self.ping_task
            except asyncio.CancelledError:
                pass
        if self.connection:
            await self.connection.close()


class BybitWebSocketManager(WebSocketTradeManager):
    """Bybit WebSocket trade stream manager"""

    def _get_websocket_url(self) -> str:
        return "wss://stream.bybit.com/v5/public/linear"

    def _get_subscription_message(self, symbols: List[str]) -> str:
        # Convert internal symbols to Bybit format
        topics = []
        for symbol in symbols:
            exchange_symbol = self.symbol_mappings.get(symbol, symbol)
            topics.append(f"publicTrade.{exchange_symbol}")

        return json.dumps({
            "op": "subscribe",
            "args": topics
        })

    def _parse_trade_message(self, message: str) -> Optional[Dict[str, Any]]:
        try:
            data = json.loads(message)

            if data.get('topic', '').startswith('publicTrade.') and 'data' in data:
                for trade in data['data']:
                    return {
                        'id': str(trade['i']),  # Trade ID
                        'timestamp': int(trade['T'] / 1000),  # Convert ms to seconds
                        'datetime': datetime.fromtimestamp(int(trade['T'] / 1000), timezone.utc).isoformat(),
                        'symbol': trade['s'],  # Symbol
                        'side': trade['S'],  # Side
                        'price': float(trade['p']),
                        'amount': float(trade['v']),
                        'cost': float(trade['p']) * float(trade['v'])
                    }

        except Exception as e:
            logger.error(f"Error parsing Bybit trade message: {e}")

        return None


class KuCoinWebSocketManager(WebSocketTradeManager):
    """KuCoin WebSocket trade stream manager"""

    def __init__(self, exchange_id: str, exchange_config: dict, app=None):
        super().__init__(exchange_id, exchange_config, app)
        self.token = None
        self.connect_id = None

    async def _get_websocket_token(self) -> tuple[str, str]:
        """Get KuCoin WebSocket token and endpoint"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post('https://api.kucoin.com/api/v1/bullet-public') as response:
                    data = await response.json()
                    token = data['data']['token']
                    endpoint = data['data']['instanceServers'][0]['endpoint']
                    return token, endpoint
        except Exception as e:
            logger.error(f"Error getting KuCoin WebSocket token: {e}")
            raise

    def _get_websocket_url(self) -> str:
        # This will be set after getting token
        return ""

    async def start(self) -> None:
        """Override to handle KuCoin's token-based authentication"""
        logger.info(f"🚀 Starting KuCoin WebSocket trade stream for {self.name}")
        self.is_running = True

        while self.is_running and self.reconnect_attempts < self.max_reconnect_attempts:
            try:
                # Get WebSocket token and endpoint
                token, endpoint = await self._get_websocket_token()
                websocket_url = f"{endpoint}?token={token}&connectId={self.connect_id or '1'}"

                # Get symbols to subscribe to
                symbols_to_subscribe = []
                for internal_symbol in self.symbol_mappings.keys():
                    if internal_symbol in SUPPORTED_SYMBOLS:
                        symbols_to_subscribe.append(internal_symbol)

                if not symbols_to_subscribe:
                    logger.warning(f"No supported symbols found for {self.exchange_id}")
                    return

                logger.info(f"📡 Connecting to KuCoin WebSocket: {websocket_url}")

                async with websockets.connect(websocket_url) as websocket:
                    self.connection = websocket
                    self.reconnect_attempts = 0

                    # Subscribe to trade topics
                    topics = []
                    for symbol in symbols_to_subscribe:
                        exchange_symbol = self.symbol_mappings.get(symbol, symbol)
                        topics.append(f"/market/match:{exchange_symbol}")

                    subscription_msg = {
                        "id": "2",
                        "type": "subscribe",
                        "topic": "/market/match:" + ",".join([self.symbol_mappings.get(s, s) for s in symbols_to_subscribe]),
                        "privateChannel": False,
                        "response": True
                    }

                    await websocket.send(json.dumps(subscription_msg))
                    logger.info(f"✅ Subscribed to KuCoin trade streams for {len(symbols_to_subscribe)} symbols")

                    # Listen for messages
                    async for message in websocket:
                        try:
                            data = json.loads(message)

                            if data.get('type') == 'message' and 'data' in data:
                                trade = data['data']
                                # Find which internal symbol this corresponds to
                                for internal_symbol, exchange_symbol in self.symbol_mappings.items():
                                    if trade.get('symbol') == exchange_symbol:
                                        timestamp_seconds = int(int(trade['time']) / 1000000000)  # Convert nanoseconds to seconds
                                        standardized_trade = {
                                            'id': str(trade['tradeId']),
                                            'timestamp': timestamp_seconds,
                                            'datetime': datetime.fromtimestamp(timestamp_seconds, timezone.utc).isoformat(),
                                            'symbol': exchange_symbol,
                                            'side': trade['side'],
                                            'price': float(trade['price']),
                                            'amount': float(trade['size']),
                                            'cost': float(trade['price']) * float(trade['size'])
                                        }
                                        await self._handle_trade(standardized_trade, internal_symbol)
                                        break

                        except json.JSONDecodeError:
                            continue
                        except Exception as e:
                            logger.error(f"Error processing KuCoin message: {e}")
                            continue

            except Exception as e:
                logger.error(f"KuCoin WebSocket error: {e}")

            if self.is_running:
                self.reconnect_attempts += 1
                if self.reconnect_attempts < self.max_reconnect_attempts:
                    logger.info(f"🔄 Reconnecting KuCoin in {self.reconnect_delay}s (attempt {self.reconnect_attempts}/{self.max_reconnect_attempts})")
                    await asyncio.sleep(self.reconnect_delay)
                    self.reconnect_delay = min(self.reconnect_delay * 2, 60)
                else:
                    logger.error(f"❌ Max reconnection attempts reached for KuCoin")


class OKXWebSocketManager(WebSocketTradeManager):
    """OKX WebSocket trade stream manager"""

    def __init__(self, exchange_id: str, exchange_config: dict, app=None):
        super().__init__(exchange_id, exchange_config, app)
        self.ping_task = None

    def _get_websocket_url(self) -> str:
        return "wss://ws.okx.com:8443/ws/v5/public"

    def _get_subscription_message(self, symbols: List[str]) -> str:
        # Convert internal symbols to OKX format
        args = []
        for symbol in symbols:
            exchange_symbol = self.symbol_mappings.get(symbol, symbol)
            args.append({
                "channel": "trades",
                "instId": exchange_symbol
            })

        return json.dumps({
            "op": "subscribe",
            "args": args
        })

    def _parse_trade_message(self, message: str) -> Optional[Dict[str, Any]]:
        try:
            data = json.loads(message)

            if data.get('event') == 'subscribe':
                return None  # Skip subscription confirmations

            if 'arg' in data and 'data' in data:
                arg = data['arg']
                if arg.get('channel') == 'trades':
                    for trade in data['data']:
                        ts_ms = int(trade['ts'])  # Convert to int first
                        ts_s = ts_ms // 1000  # Convert ms to seconds
                        return {
                            'id': str(trade['tradeId']),
                            'timestamp': ts_s,
                            'datetime': datetime.fromtimestamp(ts_s, timezone.utc).isoformat(),
                            'symbol': arg['instId'],
                            'side': trade['side'],
                            'price': float(trade['px']),
                            'amount': float(trade['sz']),
                            'cost': float(trade['px']) * float(trade['sz'])
                        }

        except Exception as e:
            logger.error(f"Error parsing OKX trade message: {e}")

        return None

    async def _send_ping(self, websocket) -> None:
        """Send periodic ping messages to OKX WebSocket to keep connection alive"""
        try:
            while self.is_running:
                await asyncio.sleep(20)  # OKX requires ping every 30 seconds, send every 20 for safety
                if self.is_running and websocket.open:
                    ping_msg = json.dumps({"op": "ping"})
                    await websocket.send(ping_msg)
                    logger.debug(f"Sent ping to OKX WebSocket")
        except Exception as e:
            logger.error(f"Error sending ping to OKX: {e}")


# Factory function to create WebSocket managers
def create_websocket_manager(exchange_id: str, app=None) -> Optional[WebSocketTradeManager]:
    """Create appropriate WebSocket manager for exchange"""
    if exchange_id not in SUPPORTED_EXCHANGES:
        return None

    exchange_config = SUPPORTED_EXCHANGES[exchange_id]
    exchange_type = exchange_config.get('type', 'cex')

    # Only create managers for CEX exchanges (DEX already has WebSocket support)
    if exchange_type == 'dex':
        return None

    managers = {
        'binance': BinanceWebSocketManager,
        'bybit': BybitWebSocketManager,
        'kucoin': KuCoinWebSocketManager,
        'okx': OKXWebSocketManager
    }

    manager_class = managers.get(exchange_id.lower())
    if manager_class:
        return manager_class(exchange_id, exchange_config, app)

    return None


# Global manager instances
websocket_managers: Dict[str, WebSocketTradeManager] = {}


async def start_websocket_trade_streams(app=None) -> None:
    """Start WebSocket trade streams for all supported CEX exchanges"""
    logger.info("🚀 Starting WebSocket trade streams for CEX exchanges")

    for exchange_id in SUPPORTED_EXCHANGES.keys():
        manager = create_websocket_manager(exchange_id, app)
        if manager:
            websocket_managers[exchange_id] = manager
            # Start in background
            asyncio.create_task(manager.start())

    logger.info(f"✅ Started WebSocket streams for {len(websocket_managers)} exchanges")


async def stop_websocket_trade_streams() -> None:
    """Stop all WebSocket trade streams"""
    logger.info("🛑 Stopping WebSocket trade streams")

    stop_tasks = []
    for manager in websocket_managers.values():
        stop_tasks.append(manager.stop())

    if stop_tasks:
        await asyncio.gather(*stop_tasks, return_exceptions=True)

    websocket_managers.clear()
    logger.info("✅ Stopped all WebSocket trade streams")


async def get_websocket_status() -> Dict[str, Any]:
    """Get status of all WebSocket connections"""
    status = {}
    for exchange_id, manager in websocket_managers.items():
        status[exchange_id] = {
            'running': manager.is_running,
            'reconnect_attempts': manager.reconnect_attempts,
            'symbols': list(manager.symbol_mappings.keys())
        }
    return status


# Test function
async def test_websocket_streams():
    """Test WebSocket streams for a short duration"""
    logger.info("🧪 Testing WebSocket trade streams...")

    await start_websocket_trade_streams()

    # Run for 30 seconds
    await asyncio.sleep(30)

    await stop_websocket_trade_streams()

    logger.info("✅ WebSocket test completed")


if __name__ == "__main__":
    # Run test
    asyncio.run(test_websocket_streams())