import asyncio
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional

import ccxt

from hummingbot.connector.ccxt_adapter import ccxt_utils
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource

if TYPE_CHECKING:
    from hummingbot.connector.ccxt_adapter.ccxt_perpetual_derivative import CcxtPerpetualDerivative


class CcxtPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    """
    Order book data source on top of ccxt.

    ccxt.pro hands back the full, already maintained order book on every ``watch_order_book`` update, so each
    update is forwarded to the order book tracker as a SNAPSHOT message and no diff stream is used.
    """

    FUNDING_INFO_POLL_INTERVAL = 60.0
    STREAM_RETRY_DELAY = 5.0
    ORDER_BOOK_POLL_INTERVAL = 1.0

    def __init__(self, trading_pairs: List[str], connector: "CcxtPerpetualDerivative"):
        super().__init__(trading_pairs)
        self._connector = connector

    # ---- REST ---------------------------------------------------------------------------------------------------
    async def get_last_traded_prices(self, trading_pairs: List[str], domain: Optional[str] = None) -> Dict[str, float]:
        symbols = [await self._connector.exchange_symbol_associated_to_pair(trading_pair=pair)
                   for pair in trading_pairs]
        tickers = await self._connector.ccxt_call("fetch_tickers", symbols)
        prices = {}
        for pair, symbol in zip(trading_pairs, symbols):
            ticker = tickers.get(symbol)
            if ticker is not None and ticker.get("last") is not None:
                prices[pair] = float(ticker["last"])
        return prices

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        order_book = await self._connector.ccxt_call("fetch_order_book", symbol)
        return self._snapshot_message(trading_pair, symbol, order_book)

    async def get_funding_info(self, trading_pair: str) -> FundingInfo:
        funding = await self._funding_info_message(trading_pair)
        return FundingInfo(
            trading_pair=trading_pair,
            index_price=ccxt_utils.to_decimal(funding.get("indexPrice"), Decimal("0")),
            mark_price=ccxt_utils.to_decimal(funding.get("markPrice"), Decimal("0")),
            next_funding_utc_timestamp=self._seconds(funding.get("nextFundingTimestamp") or funding.get("fundingTimestamp")),
            rate=ccxt_utils.to_decimal(funding.get("fundingRate"), Decimal("0")),
        )

    async def _funding_info_message(self, trading_pair: str) -> Dict[str, Any]:
        """ccxt funding rate structure completed with the index and mark prices when the exchange omits them."""
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        funding = dict(await self._connector.ccxt_call("fetch_funding_rate", symbol))
        if funding.get("indexPrice") is None or funding.get("markPrice") is None:
            ticker = await self._connector.ccxt_call("fetch_ticker", symbol)
            index_price, mark_price = self._connector.funding_prices_from_ticker(ticker)
            funding["indexPrice"] = funding.get("indexPrice") or index_price
            funding["markPrice"] = funding.get("markPrice") or mark_price
        funding["symbol"] = symbol
        return funding

    # ---- streaming ----------------------------------------------------------------------------------------------
    async def listen_for_subscriptions(self):
        """
        ccxt has no funding streaming method, so the funding information is polled and pushed to the funding info
        queue, which ``listen_for_funding_info`` of the base class consumes.
        """
        queue = self._message_queue[self._funding_info_messages_queue_key]
        while True:
            try:
                for trading_pair in self._trading_pairs:
                    queue.put_nowait(await self._funding_info_message(trading_pair))
                await self._sleep(self.FUNDING_INFO_POLL_INTERVAL)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Unexpected error when processing public funding info updates from exchange")
                await self._sleep(self.STREAM_RETRY_DELAY)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """No diff stream, see the class docstring."""
        await self._block_forever()

    async def listen_for_trades(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        """Public trades are not streamed by this adapter yet."""
        await self._block_forever()

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.AbstractEventLoop, output: asyncio.Queue):
        tasks = [asyncio.ensure_future(self._watch_order_book(trading_pair, output))
                 for trading_pair in self._trading_pairs]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()

    async def _watch_order_book(self, trading_pair: str, output: asyncio.Queue):
        while True:
            try:
                if self._connector.USE_ORDER_BOOK_STREAM:
                    symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                    order_book = await self._connector.ccxt_client.watch_order_book(symbol)
                    output.put_nowait(self._snapshot_message(trading_pair, symbol, order_book))
                else:  # the exchange has no usable ccxt.pro order book stream, poll the REST order book
                    output.put_nowait(await self._order_book_snapshot(trading_pair))
                    await self._sleep(self.ORDER_BOOK_POLL_INTERVAL)
            except asyncio.CancelledError:
                raise
            except ccxt.NetworkError as connection_exception:
                self.logger().warning(f"The websocket connection was closed ({connection_exception})")
                await self._sleep(self.STREAM_RETRY_DELAY)
            except Exception:
                self.logger().exception("Unexpected error when processing public order book snapshots from exchange")
                await self._sleep(self.STREAM_RETRY_DELAY)

    async def _parse_funding_info_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(raw_message["symbol"])
        message_queue.put_nowait(FundingInfoUpdate(
            trading_pair=trading_pair,
            index_price=ccxt_utils.to_decimal(raw_message.get("indexPrice")),
            mark_price=ccxt_utils.to_decimal(raw_message.get("markPrice")),
            next_funding_utc_timestamp=self._seconds(
                raw_message.get("nextFundingTimestamp") or raw_message.get("fundingTimestamp")) or None,
            rate=ccxt_utils.to_decimal(raw_message.get("fundingRate")),
        ))

    async def subscribe_to_trading_pair(self, trading_pair: str) -> bool:
        if trading_pair in self._trading_pairs:
            return True
        self._trading_pairs.append(trading_pair)
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair: str) -> bool:
        if trading_pair in self._trading_pairs:
            self._trading_pairs.remove(trading_pair)
        return True

    # ---- helpers ------------------------------------------------------------------------------------------------
    def _snapshot_message(self, trading_pair: str, symbol: str, order_book: Dict[str, Any]) -> OrderBookMessage:
        size = self._connector.contract_size(symbol)
        timestamp = self._seconds(order_book.get("timestamp")) or time.time()
        update_id = order_book.get("nonce") or int(timestamp * 1e3)
        return OrderBookMessage(
            OrderBookMessageType.SNAPSHOT,
            {
                "trading_pair": trading_pair,
                "update_id": update_id,
                "bids": self._to_base_levels(order_book.get("bids", []), size),
                "asks": self._to_base_levels(order_book.get("asks", []), size),
            },
            timestamp=timestamp,
        )

    @staticmethod
    def _to_base_levels(levels: List[List[float]], size: Decimal) -> List[List[float]]:
        """Order book levels as ``[price, amount in base units]``, converted from contracts without float noise."""
        return [[price, float(ccxt_utils.to_decimal(amount) * size)] for price, amount, *_ in levels]

    @staticmethod
    def _seconds(milliseconds: Optional[float]) -> int:
        return int(milliseconds // 1000) if milliseconds else 0

    @staticmethod
    async def _block_forever():
        await asyncio.Event().wait()
