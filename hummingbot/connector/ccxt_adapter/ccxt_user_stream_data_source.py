import asyncio
import time
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, Optional

import ccxt

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.ws_assistant import WSAssistant

if TYPE_CHECKING:
    from hummingbot.connector.ccxt_adapter.ccxt_perpetual_derivative import CcxtPerpetualDerivative

CHANNEL_ORDERS = "orders"
CHANNEL_TRADES = "trades"
CHANNEL_BALANCE = "balance"
CHANNEL_POSITIONS = "positions"


class CcxtPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    """
    Private stream on top of ccxt.pro. Every message put in the output queue is a dictionary
    ``{"channel": "orders" | "trades" | "balance", "data": <ccxt unified structure>}``.

    Only the streams the exchange offers are opened (``client.has``). An exchange without a stream for something (MEXC has no ``watch_positions``)
    or without any private stream: what is not streamed is polled by the connector.
    """

    RETRY_DELAY = 5.0
    PRIVATE_WATCH_METHODS = ("watchOrders", "watchMyTrades", "watchBalance", "watchPositions")

    def __init__(self, connector: "CcxtPerpetualDerivative"):
        super().__init__()
        self._connector = connector
        self._last_recv_time = 0.0

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    @property
    def streams_private_data(self) -> bool:
        """False when ccxt.pro offers no private stream for the exchange, its private data is then polled."""
        return any(self._connector.ccxt_client.has.get(method) for method in self.PRIVATE_WATCH_METHODS)

    async def listen_for_user_stream(self, output: asyncio.Queue):
        client = self._connector.ccxt_client
        streams = [
            ("watchOrders", "watch_orders", CHANNEL_ORDERS, True),
            ("watchMyTrades", "watch_my_trades", CHANNEL_TRADES, True),
            ("watchBalance", "watch_balance", CHANNEL_BALANCE, False),
            ("watchPositions", "watch_positions", CHANNEL_POSITIONS, True),
        ]  # not every exchange streams everything, the rest is polled by the connector
        tasks = [asyncio.ensure_future(self._watch(getattr(client, method), channel, output, many))
                 for capability, method, channel, many in streams if client.has.get(capability)]
        if not self.streams_private_data:
            self._last_recv_time = time.time()  # nothing to wait for, the stream counts as initialized
            self.logger().info(f"{self._connector.name} has no private stream in ccxt, private data is polled.")
        try:
            await asyncio.gather(*tasks) if tasks else await asyncio.Event().wait()
        finally:
            for task in tasks:
                task.cancel()

    async def _watch(self, watch_method: Callable[[], Awaitable[Any]], channel: str, output: asyncio.Queue,
                     many: bool):
        while True:
            try:
                data = await watch_method()
                self._last_recv_time = time.time()
                for item in (data if many else [data]):
                    output.put_nowait({"channel": channel, "data": item})
            except asyncio.CancelledError:
                raise
            except ccxt.NetworkError as connection_exception:
                self.logger().warning(f"The websocket connection was closed ({connection_exception})")
                await self._sleep(self.RETRY_DELAY)
            except Exception:
                self.logger().exception("Unexpected error while listening to user stream. Retrying after 5 seconds...")
                await self._sleep(self.RETRY_DELAY)

    # The websocket assistant based hooks of the base class are not used, ccxt.pro owns the connections.
    async def _connected_websocket_assistant(self) -> WSAssistant:
        raise NotImplementedError

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        raise NotImplementedError

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        raise NotImplementedError

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        pass
