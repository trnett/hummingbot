import asyncio
from test.hummingbot.connector.derivative.mexc_perpetual.ccxt_fakes import (
    CCXT_SYMBOL,
    blocked,
    fake_ccxt_client,
    make_exchange,
    stream,
)
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from unittest.mock import AsyncMock

import ccxt

ORDER = {"id": "EX-1", "status": "open", "filled": 0, "symbol": CCXT_SYMBOL}
TRADE = {"id": "T1", "order": "EX-1", "symbol": CCXT_SYMBOL}
BALANCE = {"HBOT": {"free": 90.0, "used": 10.0, "total": 100.0}}
POSITION = {"symbol": CCXT_SYMBOL, "side": "long", "contracts": 20}


class MexcPerpetualUserStreamDataSourceTests(IsolatedAsyncioWrapperTestCase):
    """
    ccxt.pro offers watch_orders, watch_my_trades and watch_balance for MEXC, but NOT watch_positions
    (``exchange.has["watchPositions"]`` is None), so MEXC positions are polled, not streamed.
    """

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.ccxt = fake_ccxt_client()
        for method in ("watch_orders", "watch_my_trades", "watch_balance", "watch_positions"):
            setattr(self.ccxt, method, AsyncMock(side_effect=blocked()))
        self.exchange = make_exchange(self.ccxt)
        self.data_source = self.exchange._user_stream_tracker.data_source
        self.data_source._sleep = AsyncMock()  # no real backoff in tests

    async def _collect(self, count: int):
        queue = asyncio.Queue()
        task = asyncio.ensure_future(self.data_source.listen_for_user_stream(queue))
        try:
            return [await asyncio.wait_for(queue.get(), timeout=2) for _ in range(count)]
        finally:
            task.cancel()

    async def test_orders_are_forwarded_one_message_per_order(self):
        self.ccxt.watch_orders = AsyncMock(side_effect=stream([ORDER, {**ORDER, "id": "EX-2"}]))

        messages = await self._collect(2)

        self.assertEqual([("orders", "EX-1"), ("orders", "EX-2")],
                         [(message["channel"], message["data"]["id"]) for message in messages])

    async def test_trades_and_balance_are_forwarded(self):
        self.ccxt.watch_my_trades = AsyncMock(side_effect=stream([TRADE]))
        self.ccxt.watch_balance = AsyncMock(side_effect=stream(BALANCE))

        messages = await self._collect(2)

        self.assertEqual({"trades": TRADE, "balance": BALANCE}, {m["channel"]: m["data"] for m in messages})

    async def test_last_receive_time_is_updated_by_messages(self):
        self.assertEqual(0, self.data_source.last_recv_time)
        self.ccxt.watch_orders = AsyncMock(side_effect=stream([ORDER]))
        await self._collect(1)
        self.assertGreater(self.data_source.last_recv_time, 0)

    async def test_positions_are_not_streamed_when_the_exchange_cannot(self):
        self.ccxt.watch_orders = AsyncMock(side_effect=stream([ORDER]))
        await self._collect(1)
        self.ccxt.watch_positions.assert_not_called()

    async def test_positions_are_streamed_when_the_exchange_supports_it(self):
        self.ccxt.has = {"watchPositions": True}
        self.ccxt.watch_positions = AsyncMock(side_effect=stream([POSITION]))

        messages = await self._collect(1)

        self.assertEqual(("positions", POSITION), (messages[0]["channel"], messages[0]["data"]))

    async def test_a_failing_stream_is_retried_and_does_not_stop_the_others(self):
        self.ccxt.watch_orders = AsyncMock(side_effect=stream(ccxt.NetworkError("ws dropped"), [ORDER]))
        self.ccxt.watch_balance = AsyncMock(side_effect=stream(BALANCE))

        messages = await self._collect(2)

        self.assertEqual({"orders", "balance"}, {message["channel"] for message in messages})
        self.data_source._sleep.assert_awaited_with(self.data_source.RETRY_DELAY)
