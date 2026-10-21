import asyncio
from decimal import Decimal
from test.hummingbot.connector.derivative.mexc_perpetual.ccxt_fakes import (
    CCXT_SYMBOL,
    HB_PAIR,
    fake_ccxt_client,
    make_exchange,
    stream,
)
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

import ccxt

from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessageType

ORDER_BOOK = {"symbol": CCXT_SYMBOL, "bids": [[100.0, 20], [99.5, 10]], "asks": [[100.5, 30]],
              "timestamp": 1_700_000_000_000, "nonce": 77}
FUNDING_RATE = {"symbol": CCXT_SYMBOL, "markPrice": None, "indexPrice": None, "fundingRate": 0.0001,
                "fundingTimestamp": 1_700_028_800_000, "nextFundingTimestamp": None}
TICKER = {"symbol": CCXT_SYMBOL, "last": 101.5, "info": {"indexPrice": 100.1, "fairPrice": 100.2}}


class MexcPerpetualAPIOrderBookDataSourceTests(IsolatedAsyncioWrapperTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.ccxt = fake_ccxt_client()
        self.exchange = make_exchange(self.ccxt, trading_required=False)
        self.data_source = self.exchange._orderbook_ds
        await self.exchange._initialize_trading_pair_symbol_map()

    async def test_snapshot_message_converts_contracts_to_base_amounts(self):
        self.ccxt.fetch_order_book = AsyncMock(return_value=ORDER_BOOK)

        message = await self.data_source._order_book_snapshot(HB_PAIR)

        self.assertEqual(OrderBookMessageType.SNAPSHOT, message.type)
        self.assertEqual(HB_PAIR, message.content["trading_pair"])
        self.assertEqual(77, message.update_id)
        self.assertEqual(1_700_000_000, message.timestamp)
        self.assertEqual([[100.0, 2.0], [99.5, 1.0]], message.content["bids"])
        self.assertEqual([[100.5, 3.0]], message.content["asks"])  # 30 * 0.1 without float noise
        self.ccxt.fetch_order_book.assert_awaited_once_with(CCXT_SYMBOL)

    async def test_get_new_order_book_is_populated(self):
        self.ccxt.fetch_order_book = AsyncMock(return_value=ORDER_BOOK)

        order_book = await self.data_source.get_new_order_book(HB_PAIR)

        bids, asks = order_book.snapshot
        self.assertEqual(100.0, bids.iloc[0]["price"])
        self.assertEqual(2.0, bids.iloc[0]["amount"])
        self.assertEqual(100.5, asks.iloc[0]["price"])

    async def test_stream_forwards_every_watch_order_book_update_as_a_snapshot(self):
        self.ccxt.watch_order_book = AsyncMock(side_effect=stream(ORDER_BOOK, {**ORDER_BOOK, "nonce": 78}))
        queue = asyncio.Queue()

        task = asyncio.ensure_future(self.data_source.listen_for_order_book_snapshots(self.local_event_loop, queue))
        first = await asyncio.wait_for(queue.get(), timeout=2)
        second = await asyncio.wait_for(queue.get(), timeout=2)
        task.cancel()

        self.assertEqual([77, 78], [first.update_id, second.update_id])
        self.assertEqual(OrderBookMessageType.SNAPSHOT, first.type)
        self.ccxt.watch_order_book.assert_awaited_with(CCXT_SYMBOL)

    async def test_the_stream_is_used_rather_than_polling(self):
        self.assertTrue(self.exchange.USE_ORDER_BOOK_STREAM)
        self.ccxt.fetch_order_book = AsyncMock()
        self.ccxt.watch_order_book = AsyncMock(side_effect=stream(ORDER_BOOK))
        queue = asyncio.Queue()

        task = asyncio.ensure_future(self.data_source.listen_for_order_book_snapshots(self.local_event_loop, queue))
        await asyncio.wait_for(queue.get(), timeout=2)
        task.cancel()

        self.ccxt.fetch_order_book.assert_not_called()

    async def test_stream_recovers_after_a_network_error(self):
        self.ccxt.watch_order_book = AsyncMock(side_effect=stream(ccxt.NetworkError("ws dropped"), ORDER_BOOK))
        self.data_source._sleep = AsyncMock()  # no real backoff in tests
        queue = asyncio.Queue()

        task = asyncio.ensure_future(self.data_source.listen_for_order_book_snapshots(self.local_event_loop, queue))
        message = await asyncio.wait_for(queue.get(), timeout=2)
        task.cancel()

        self.assertEqual(HB_PAIR, message.content["trading_pair"])
        self.data_source._sleep.assert_awaited_once_with(self.data_source.STREAM_RETRY_DELAY)

    async def test_get_funding_info_completes_missing_prices_from_the_ticker(self):
        self.ccxt.fetch_funding_rate = AsyncMock(return_value=FUNDING_RATE)
        self.ccxt.fetch_ticker = AsyncMock(return_value=TICKER)

        info = await self.data_source.get_funding_info(HB_PAIR)

        self.assertIsInstance(info, FundingInfo)
        self.assertEqual(HB_PAIR, info.trading_pair)
        self.assertEqual(Decimal("100.1"), info.index_price)
        self.assertEqual(Decimal("100.2"), info.mark_price)  # MEXC's fair price
        self.assertEqual(Decimal("0.0001"), info.rate)
        self.assertEqual(1_700_028_800, info.next_funding_utc_timestamp)

    async def test_get_funding_info_skips_the_ticker_when_the_funding_rate_has_the_prices(self):
        self.ccxt.fetch_funding_rate = AsyncMock(return_value={**FUNDING_RATE, "markPrice": 5.5, "indexPrice": 5.4})
        self.ccxt.fetch_ticker = AsyncMock()

        info = await self.data_source.get_funding_info(HB_PAIR)

        self.assertEqual((Decimal("5.4"), Decimal("5.5")), (info.index_price, info.mark_price))
        self.ccxt.fetch_ticker.assert_not_called()

    async def test_funding_info_poller_feeds_the_funding_info_queue(self):
        self.ccxt.fetch_funding_rate = AsyncMock(return_value=FUNDING_RATE)
        self.ccxt.fetch_ticker = AsyncMock(return_value=TICKER)
        self.data_source._sleep = AsyncMock(side_effect=asyncio.CancelledError)  # one polling round
        funding_queue = self.data_source._message_queue[self.data_source._funding_info_messages_queue_key]

        with self.assertRaises(asyncio.CancelledError):
            await self.data_source.listen_for_subscriptions()

        self.assertEqual(1, funding_queue.qsize())
        update_queue = asyncio.Queue()
        await self.data_source._parse_funding_info_message(funding_queue.get_nowait(), update_queue)
        update: FundingInfoUpdate = update_queue.get_nowait()
        self.assertEqual(HB_PAIR, update.trading_pair)
        self.assertEqual(Decimal("0.0001"), update.rate)
        self.assertEqual(1_700_028_800, update.next_funding_utc_timestamp)

    async def test_last_traded_prices(self):
        self.ccxt.fetch_tickers = AsyncMock(return_value={CCXT_SYMBOL: TICKER})
        self.assertEqual({HB_PAIR: 101.5}, await self.data_source.get_last_traded_prices([HB_PAIR]))

    async def test_subscribe_and_unsubscribe_track_the_trading_pairs(self):
        self.assertTrue(await self.data_source.subscribe_to_trading_pair("ETH-HBOT"))
        self.assertIn("ETH-HBOT", self.data_source._trading_pairs)
        self.assertTrue(await self.data_source.unsubscribe_from_trading_pair("ETH-HBOT"))
        self.assertNotIn("ETH-HBOT", self.data_source._trading_pairs)
