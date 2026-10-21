import asyncio
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock, MagicMock

import ccxt

from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_derivative import AsterPerpetualDerivative
from hummingbot.core.data_type.order_book_message import OrderBookMessageType

HB_PAIR = "COINALPHA-HBOT"
CCXT_SYMBOL = "COINALPHA/HBOT:HBOT"
MARKET = {
    "id": "COINALPHAHBOT", "symbol": CCXT_SYMBOL, "base": "COINALPHA", "quote": "HBOT", "settle": "HBOT",
    "type": "swap", "swap": True, "linear": True, "inverse": False, "active": True, "contract": True,
    "contractSize": 1.0, "precision": {"amount": 0.001, "price": 0.01},
    "limits": {"amount": {"min": 0.001, "max": 1000}, "cost": {"min": 5}},
}
# no private streams at all, only the public order book, as in ccxt.pro for Aster
HAS_NO_PRIVATE_STREAMS = {"watchOrders": None, "watchMyTrades": None, "watchBalance": False, "watchPositions": None}


def stream(*items):
    pending = list(items)

    async def watch(*args, **kwargs):
        if not pending:
            await asyncio.Event().wait()
        item = pending.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item
    return watch


class AsterPerpetualStreamsTests(IsolatedAsyncioWrapperTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.ccxt = MagicMock()
        self.ccxt.has = dict(HAS_NO_PRIVATE_STREAMS)
        self.ccxt.precisionMode = ccxt.TICK_SIZE
        self.ccxt.load_markets = AsyncMock(return_value={CCXT_SYMBOL: MARKET})
        self.ccxt.close = AsyncMock()
        self.exchange = AsterPerpetualDerivative(
            aster_perpetual_user="0x" + "1" * 40, aster_perpetual_signer="0x" + "2" * 40,
            aster_perpetual_private_key="0x" + "3" * 64, trading_pairs=[HB_PAIR],
            trading_required=True, ccxt_client=self.ccxt)
        await self.exchange._initialize_trading_pair_symbol_map()

    # ---- private data is polled ------------------------------------------------------------------------------
    async def test_without_private_streams_nothing_is_watched_and_the_stream_counts_as_initialized(self):
        data_source = self.exchange._user_stream_tracker.data_source
        self.assertFalse(data_source.streams_private_data)
        self.assertEqual(0, data_source.last_recv_time)

        task = asyncio.ensure_future(data_source.listen_for_user_stream(asyncio.Queue()))
        await asyncio.sleep(0.05)
        task.cancel()

        self.assertGreater(data_source.last_recv_time, 0)  # nothing to wait for
        self.ccxt.watch_orders.assert_not_called()
        self.ccxt.watch_my_trades.assert_not_called()
        self.ccxt.watch_balance.assert_not_called()

    async def test_polling_only_connectors_poll_every_short_interval(self):
        self.assertEqual(self.exchange.SHORT_POLL_INTERVAL, self.exchange._get_poll_interval(1_000_000))

    async def test_only_the_available_private_streams_are_opened(self):
        self.ccxt.has = {**HAS_NO_PRIVATE_STREAMS, "watchOrders": True}
        self.ccxt.watch_orders = AsyncMock(side_effect=stream([{"id": "1", "status": "open"}]))
        self.ccxt.watch_my_trades = AsyncMock()
        data_source = self.exchange._user_stream_tracker.data_source
        queue = asyncio.Queue()

        task = asyncio.ensure_future(data_source.listen_for_user_stream(queue))
        message = await asyncio.wait_for(queue.get(), timeout=2)
        task.cancel()

        self.assertTrue(data_source.streams_private_data)
        self.assertEqual("orders", message["channel"])
        self.ccxt.watch_my_trades.assert_not_called()

    # ---- public data -----------------------------------------------------------------------------------------
    async def test_order_book_snapshot_without_a_sequence_number_uses_the_timestamp(self):
        data_source = self.exchange._orderbook_ds
        self.ccxt.fetch_order_book = AsyncMock(return_value={
            "bids": [[100.0, 2.0], [99.5, 1.0]], "asks": [[100.5, 3.0]], "timestamp": 1_700_000_000_000,
            "nonce": None})

        message = await data_source._order_book_snapshot(HB_PAIR)

        self.assertEqual(OrderBookMessageType.SNAPSHOT, message.type)
        self.assertEqual(1_700_000_000_000, message.update_id)
        self.assertEqual([[100.0, 2.0], [99.5, 1.0]], message.content["bids"])  # contract size 1, amounts unchanged

    async def test_order_book_is_polled_because_the_ccxt_pro_stream_is_unusable(self):
        """ccxt.pro 4.5.32 raises TypeError in watch_order_book for Aster, so the REST book is polled."""
        data_source = self.exchange._orderbook_ds
        book = {"bids": [[100.0, 2.0]], "asks": [[100.5, 3.0]], "timestamp": 1_700_000_000_000, "nonce": None}
        self.ccxt.fetch_order_book = AsyncMock(return_value=book)
        self.ccxt.watch_order_book = AsyncMock()

        async def yield_to_the_loop(seconds):  # a mocked sleep must still let the other tasks run
            await asyncio.sleep(0)

        data_source._sleep = AsyncMock(side_effect=yield_to_the_loop)
        queue = asyncio.Queue()

        task = asyncio.ensure_future(data_source.listen_for_order_book_snapshots(self.local_event_loop, queue))
        first = await asyncio.wait_for(queue.get(), timeout=2)
        second = await asyncio.wait_for(queue.get(), timeout=2)  # polling goes on
        task.cancel()

        self.assertFalse(self.exchange.USE_ORDER_BOOK_STREAM)
        self.assertEqual(HB_PAIR, first.content["trading_pair"])
        self.assertEqual(OrderBookMessageType.SNAPSHOT, second.type)
        data_source._sleep.assert_awaited_with(data_source.ORDER_BOOK_POLL_INTERVAL)
        self.ccxt.watch_order_book.assert_not_called()

    async def test_funding_info_needs_no_ticker_because_the_premium_index_has_the_prices(self):
        data_source = self.exchange._orderbook_ds
        self.ccxt.fetch_funding_rate = AsyncMock(return_value={
            "symbol": CCXT_SYMBOL, "markPrice": 101.1, "indexPrice": 101.0, "fundingRate": 0.0001,
            "fundingTimestamp": None, "nextFundingTimestamp": 1_700_028_800_000})
        self.ccxt.fetch_ticker = AsyncMock()

        info = await data_source.get_funding_info(HB_PAIR)

        self.assertEqual((Decimal("101"), Decimal("101.1"), Decimal("0.0001"), 1_700_028_800),
                         (info.index_price, info.mark_price, info.rate, info.next_funding_utc_timestamp))
        self.ccxt.fetch_ticker.assert_not_called()
