"""
Rehearsal of the live checklist against a fake exchange, with Hummingbot's real time clock and connector lifecycle. It
proves the checklist itself works (so a live run does not fail on a bug of the script) and exercises the whole
connector lifecycle of a polling-only exchange: readiness, order book polling, order and fill tracking, positions.
"""
from decimal import Decimal
from test.hummingbot.connector.ccxt_adapter.fake_exchange import SYMBOL, FakeExchange
from test.hummingbot.connector.ccxt_adapter.live_checks import LiveChecks
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import List

from hummingbot.connector.ccxt_adapter.ccxt_perpetual_derivative import CcxtPerpetualDerivative
from hummingbot.connector.ccxt_adapter.ccxt_utils import CCXT_RATE_LIMIT_ID
from hummingbot.core.api_throttler.data_types import RateLimit


class RehearsalConnector(CcxtPerpetualDerivative):
    SHORT_POLL_INTERVAL = 0.5
    USE_ORDER_BOOK_STREAM = False

    @property
    def name(self) -> str:
        return "binance_perpetual"  # a registered connector name, fees are looked up by it

    @property
    def ccxt_exchange_id(self) -> str:
        return "fake"

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return [RateLimit(limit_id=CCXT_RATE_LIMIT_ID, limit=10_000, time_interval=1)]


class LiveChecklistRehearsalTests(IsolatedAsyncioWrapperTestCase):
    async def test_the_checklist_passes_on_a_well_behaved_exchange(self):
        exchange = FakeExchange()
        connector = RehearsalConnector(api_key="k", api_secret="s", trading_pairs=["ETH-USDT"], ccxt_client=exchange)
        connector._orderbook_ds.ORDER_BOOK_POLL_INTERVAL = 0.2

        async with LiveChecks(connector, "ETH-USDT", Decimal("60"), allow_trading=True) as checks:
            await checks.run_all()

        statuses = {step: status for step, status, _ in checks.report}
        self.assertNotIn("FAIL", statuses.values())
        for step in ("connectivity", "balances", "position mode", "clean pair", "resting order", "leverage",
                     "position", "fills", "round trip cost"):
            self.assertEqual("OK", statuses[step], step)
        self.assertEqual(0.0, exchange.contracts)  # the position was closed again
        self.assertTrue(all(order["status"] in ("closed", "canceled") for order in exchange.orders.values()))
        self.assertEqual(SYMBOL, checks.symbol)

    async def test_a_pair_with_an_open_position_is_refused(self):
        exchange = FakeExchange()
        exchange.contracts, exchange.entry = 1.0, 2000.0
        connector = RehearsalConnector(api_key="k", api_secret="s", trading_pairs=["ETH-USDT"], ccxt_client=exchange)
        connector._orderbook_ds.ORDER_BOOK_POLL_INTERVAL = 0.2

        with self.assertRaises(AssertionError) as raised:
            async with LiveChecks(connector, "ETH-USDT", Decimal("60"), allow_trading=False) as checks:
                await checks.run_all()

        self.assertIn("already has a position", str(raised.exception))
        self.assertEqual([], [call for call in exchange.calls if call.startswith("create_order")])  # nothing sent

    async def test_without_the_trade_flag_no_market_order_is_sent(self):
        exchange = FakeExchange()
        connector = RehearsalConnector(api_key="k", api_secret="s", trading_pairs=["ETH-USDT"], ccxt_client=exchange)
        connector._orderbook_ds.ORDER_BOOK_POLL_INTERVAL = 0.2

        async with LiveChecks(connector, "ETH-USDT", Decimal("60"), allow_trading=False) as checks:
            await checks.run_all()

        self.assertFalse([call for call in exchange.calls if "market" in call])
        self.assertEqual("SKIP", {step: status for step, status, _ in checks.report}["market round trip"])

    async def test_an_order_above_the_cap_is_never_sent(self):
        exchange = FakeExchange()
        connector = RehearsalConnector(api_key="k", api_secret="s", trading_pairs=["ETH-USDT"], ccxt_client=exchange)
        connector._orderbook_ds.ORDER_BOOK_POLL_INTERVAL = 0.2

        with self.assertRaises(AssertionError):
            async with LiveChecks(connector, "ETH-USDT", Decimal("1"), allow_trading=True) as checks:
                await checks.run_all()

        self.assertEqual([], [call for call in exchange.calls if call.startswith("create_order")])
