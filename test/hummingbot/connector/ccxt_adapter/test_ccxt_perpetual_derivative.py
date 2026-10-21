"""
Tests of the behaviour every ccxt based connector inherits from ``CcxtPerpetualDerivative``, on a minimal concrete
connector with a fake ccxt client. They pin the hardening that makes the adapter behave like a native Hummingbot
connector: time synchronization, ambiguous order submissions, fill recovery, balances, positions, logging.
"""
import asyncio
import gc
import time
import weakref
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import List
from unittest.mock import AsyncMock, MagicMock, patch

import ccxt

from hummingbot.connector.ccxt_adapter import ccxt_utils
from hummingbot.connector.ccxt_adapter.ccxt_perpetual_derivative import CcxtPerpetualDerivative
from hummingbot.connector.ccxt_adapter.ccxt_utils import CCXT_RATE_LIMIT_ID
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo

HB_PAIR = "ETH-USDT"
SYMBOL = "ETH/USDT:USDT"
MARKET = {
    "id": "ETHUSDT", "symbol": SYMBOL, "base": "ETH", "quote": "USDT", "settle": "USDT", "type": "swap",
    "swap": True, "linear": True, "inverse": False, "active": True, "contract": True, "contractSize": 0.1,
    "precision": {"amount": 1, "price": 0.01}, "limits": {"amount": {"min": 1, "max": 1_000_000}, "cost": {}},
}


class FakeConnector(CcxtPerpetualDerivative):
    @property
    def name(self) -> str:
        return "binance_perpetual"  # any registered connector name: fees are looked up by it

    @property
    def ccxt_exchange_id(self) -> str:
        return "fake"

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return [RateLimit(limit_id=CCXT_RATE_LIMIT_ID, limit=10_000, time_interval=1)]  # no throttling in tests


def fake_client() -> MagicMock:
    client = MagicMock()
    client.has = {"watchOrders": True, "watchMyTrades": True, "watchBalance": True, "watchPositions": True,
                  "fetchOrderTrades": False}
    client.precisionMode = ccxt.TICK_SIZE
    client.load_markets = AsyncMock(return_value={SYMBOL: MARKET})
    client.close = AsyncMock()
    client.fetch_time = AsyncMock(side_effect=lambda: int(time.time() * 1000))
    return client


class CcxtAdapterTestCase(IsolatedAsyncioWrapperTestCase):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.ccxt = fake_client()
        self.connector = FakeConnector(api_key="key", api_secret="secret", trading_pairs=[HB_PAIR],
                                       trading_required=True, ccxt_client=self.ccxt)
        self.connector._set_current_timestamp(1_700_000_000)
        self.connector._sleep = AsyncMock()  # no real waiting
        await self.connector._initialize_trading_pair_symbol_map()
        self.log_records = []
        for logger in (self.connector.logger(), self.connector._time_synchronizer.logger()):
            logger.setLevel(1)
            logger.addHandler(self)

    def handle(self, record):
        self.log_records.append(record)

    @property
    def level(self):
        return 1

    def logged(self, level: str, fragment: str) -> bool:
        return any(r.levelname == level and fragment in r.getMessage() for r in self.log_records)

    def track_order(self, order_id="HB-1", exchange_order_id="EX-1", amount="1", price="100",
                    trade_type=TradeType.BUY, position_action=PositionAction.OPEN):
        self.connector.start_tracking_order(
            order_id=order_id, exchange_order_id=exchange_order_id, trading_pair=HB_PAIR, trade_type=trade_type,
            price=Decimal(price), amount=Decimal(amount), order_type=OrderType.LIMIT, position_action=position_action)
        return self.connector.in_flight_orders[order_id]

    async def place(self, order_id="HB-1"):
        return await self.connector._place_order(
            order_id, HB_PAIR, Decimal("1"), TradeType.BUY, OrderType.LIMIT, Decimal("100"), PositionAction.OPEN)


class TimeSynchronizationTests(CcxtAdapterTestCase):
    async def test_signed_timestamps_follow_the_hummingbot_time_synchronizer(self):
        self.ccxt.fetch_time = AsyncMock(side_effect=lambda: int(time.time() * 1000) + 5000)  # the exchange is ahead
        await self.connector._update_time_synchronizer()
        self.assertAlmostEqual(time.time() * 1000 + 5000, self.ccxt.milliseconds(), delta=300)

    async def test_without_any_sample_the_local_clock_is_used(self):
        self.assertAlmostEqual(time.time() * 1000, self.ccxt.milliseconds(), delta=300)

    async def test_the_synchronizer_is_fed_by_the_exchange_server_time(self):
        self.ccxt.fetch_time = AsyncMock(side_effect=lambda: int(time.time() * 1000) + 3000)

        await self.connector._update_time_synchronizer()

        self.assertAlmostEqual(time.time() + 3, self.connector._time_synchronizer.time(), delta=0.3)

    async def test_a_failing_server_time_request_does_not_raise(self):
        self.ccxt.fetch_time = AsyncMock(side_effect=ccxt.NetworkError("down"))
        await self.connector._update_time_synchronizer()
        self.assertTrue(self.logged("NETWORK", "Error getting server time"))

    async def test_a_timestamp_error_resynchronizes_and_retries_once(self):
        self.ccxt.create_order = AsyncMock(side_effect=[ccxt.InvalidNonce("outside recvWindow"), {"id": "EX-1"}])

        result = await self.connector.ccxt_call("create_order", SYMBOL)

        self.assertEqual({"id": "EX-1"}, result)
        self.assertEqual(2, self.ccxt.create_order.await_count)
        self.ccxt.fetch_time.assert_awaited()

    async def test_a_second_timestamp_error_is_raised(self):
        self.ccxt.create_order = AsyncMock(side_effect=ccxt.InvalidNonce("outside recvWindow"))
        with self.assertRaises(ccxt.InvalidNonce):
            await self.connector.ccxt_call("create_order", SYMBOL)
        self.assertEqual(2, self.ccxt.create_order.await_count)

    async def test_other_errors_are_not_retried(self):
        self.ccxt.create_order = AsyncMock(side_effect=ccxt.ExchangeError("bad"))
        with self.assertRaises(ccxt.ExchangeError):
            await self.connector.ccxt_call("create_order", SYMBOL)
        self.assertEqual(1, self.ccxt.create_order.await_count)


class ErrorMessageTests(CcxtAdapterTestCase):
    def test_signatures_are_removed_from_error_text(self):
        text = "fake GET https://api.example.com/v3/order?nonce=1&signature=0x5faad6c6ab12"
        cleaned = ccxt_utils.sanitize_error_message(text)
        self.assertNotIn("5faad6c6ab12", cleaned)
        self.assertIn("signature=***", cleaned)
        self.assertNotIn("0x5faad", cleaned)
        self.assertIn("nonce=1", cleaned)

    async def test_the_error_keeps_its_class_and_loses_the_signature(self):
        self.ccxt.cancel_order = AsyncMock(side_effect=ccxt.ExchangeNotAvailable("GET /x?signature=abcdef&a=1"))
        with self.assertRaises(ccxt.ExchangeNotAvailable) as raised:
            await self.connector.ccxt_call("cancel_order", "1", SYMBOL)
        self.assertNotIn("abcdef", str(raised.exception))

    async def test_text_without_signature_is_unchanged(self):
        self.ccxt.cancel_order = AsyncMock(side_effect=ccxt.OrderNotFound('mexc {"code": 2040}'))
        with self.assertRaises(ccxt.OrderNotFound) as raised:
            await self.connector.ccxt_call("cancel_order", "1", SYMBOL)
        self.assertEqual('mexc {"code": 2040}', str(raised.exception))


class AmbiguousOrderSubmissionTests(CcxtAdapterTestCase):
    """A timeout may mean the exchange accepted the order: Hummingbot would fail it and the strategy could repeat it."""

    async def test_an_accepted_order_is_recovered_by_its_client_order_id(self):
        self.ccxt.create_order = AsyncMock(side_effect=ccxt.RequestTimeout("timed out"))
        self.ccxt.fetch_order = AsyncMock(return_value={"id": "EX-9", "status": "open", "timestamp": 1_700_000_001_000})

        exchange_order_id, timestamp = await self.place()

        self.assertEqual(("EX-9", 1_700_000_001.0), (exchange_order_id, timestamp))
        self.assertEqual(1, self.ccxt.create_order.await_count)  # never sent twice
        self.ccxt.fetch_order.assert_awaited_with(None, SYMBOL, {"clientOrderId": "HB-1"})
        self.assertTrue(self.logged("WARNING", "recovered"))

    async def test_an_order_the_exchange_never_saw_fails_with_the_original_error(self):
        self.ccxt.create_order = AsyncMock(side_effect=ccxt.RequestTimeout("timed out"))
        self.ccxt.fetch_order = AsyncMock(side_effect=ccxt.OrderNotFound("no such order"))

        with self.assertRaises(ccxt.RequestTimeout):
            await self.place()

        self.assertEqual(3, self.ccxt.fetch_order.await_count)  # the exchange may lag, it is asked a few times
        self.assertEqual(set(), self.connector._unresolved_order_tasks)

    async def test_the_lookup_stops_as_soon_as_the_order_is_found(self):
        self.ccxt.create_order = AsyncMock(side_effect=ccxt.ExchangeNotAvailable("502"))
        self.ccxt.fetch_order = AsyncMock(side_effect=[ccxt.OrderNotFound("lag"), {"id": "EX-9", "status": "open"}])

        exchange_order_id, _ = await self.place()

        self.assertEqual("EX-9", exchange_order_id)
        self.assertEqual(2, self.ccxt.fetch_order.await_count)

    async def test_errors_that_prove_the_order_was_rejected_are_not_reconciled(self):
        for error in (ccxt.InsufficientFunds("no money"), ccxt.ExchangeError("rejected"),
                      ccxt.DDoSProtection("429"), ccxt.InvalidOrder("bad size")):
            with self.subTest(error=type(error).__name__):
                self.ccxt.create_order = AsyncMock(side_effect=error)
                self.ccxt.fetch_order = AsyncMock()
                with self.assertRaises(type(error)):
                    await self.place()
                self.ccxt.fetch_order.assert_not_called()

    async def test_an_unresolved_order_is_watched_and_cancelled_when_it_shows_up(self):
        self.ccxt.create_order = AsyncMock(side_effect=ccxt.RequestTimeout("timed out"))
        lookups = [ccxt.NetworkError("down")] * 3 + [{"id": "EX-9", "status": "open"}]
        self.ccxt.fetch_order = AsyncMock(side_effect=lookups)
        self.ccxt.cancel_order = AsyncMock(return_value={})

        with self.assertRaises(ccxt.RequestTimeout):
            await self.place()
        self.assertEqual(1, len(self.connector._unresolved_order_tasks))
        self.assertTrue(self.logged("ERROR", "could not be confirmed"))
        await asyncio.gather(*self.connector._unresolved_order_tasks)

        self.ccxt.cancel_order.assert_awaited_once_with("EX-9", SYMBOL)
        self.assertTrue(self.logged("WARNING", "cancelled"))
        self.assertEqual(set(), self.connector._unresolved_order_tasks)

    async def test_an_unresolved_order_that_was_filled_is_reported_loudly(self):
        self.ccxt.create_order = AsyncMock(side_effect=ccxt.RequestTimeout("timed out"))
        self.ccxt.fetch_order = AsyncMock(side_effect=[ccxt.NetworkError("down")] * 3
                                          + [{"id": "EX-9", "status": "closed", "filled": 1}])
        self.ccxt.cancel_order = AsyncMock()

        with self.assertRaises(ccxt.RequestTimeout):
            await self.place()
        await asyncio.gather(*self.connector._unresolved_order_tasks)

        self.ccxt.cancel_order.assert_not_called()
        self.assertTrue(self.logged("ERROR", "was executed"))

    async def test_the_watch_gives_up_after_a_while(self):
        self.ccxt.create_order = AsyncMock(side_effect=ccxt.RequestTimeout("timed out"))
        self.ccxt.fetch_order = AsyncMock(side_effect=ccxt.OrderNotFound("never existed"))
        self.connector._unresolved_order_tasks = set()
        self.ccxt.fetch_order = AsyncMock(side_effect=ccxt.NetworkError("down"))

        with self.assertRaises(ccxt.RequestTimeout):
            await self.place()
        await asyncio.wait_for(asyncio.gather(*self.connector._unresolved_order_tasks), timeout=2)

        self.assertTrue(self.logged("ERROR", "gave up"))

    async def test_stop_network_cancels_the_watchers(self):
        self.ccxt.create_order = AsyncMock(side_effect=ccxt.RequestTimeout("timed out"))
        self.ccxt.fetch_order = AsyncMock(side_effect=ccxt.NetworkError("down"))
        self.connector._sleep = AsyncMock(side_effect=lambda *_: asyncio.sleep(0.01))
        with self.assertRaises(ccxt.RequestTimeout):
            await self.place()
        tasks = list(self.connector._unresolved_order_tasks)

        await self.connector.stop_network()

        self.assertTrue(all(task.done() for task in tasks))
        self.assertEqual(set(), self.connector._unresolved_order_tasks)


class FillRecoveryTests(CcxtAdapterTestCase):
    def trade(self, trade_id, order="EX-1", timestamp=1_700_000_000_000, amount=1.0):
        return {"id": trade_id, "order": order, "timestamp": timestamp, "side": "buy", "price": 100.0,
                "amount": amount, "fee": {"currency": "USDT", "cost": 0.1}}

    async def test_the_order_specific_endpoint_is_used_when_the_exchange_has_one(self):
        self.ccxt.has["fetchOrderTrades"] = True
        self.ccxt.fetch_order_trades = AsyncMock(return_value=[self.trade("T1"), self.trade("T2")])
        self.ccxt.fetch_my_trades = AsyncMock()
        order = self.track_order()

        updates = await self.connector._all_trade_updates_for_order(order)

        self.assertEqual(["T1", "T2"], [update.trade_id for update in updates])
        self.ccxt.fetch_order_trades.assert_awaited_once_with("EX-1", SYMBOL)
        self.ccxt.fetch_my_trades.assert_not_called()

    async def test_account_trades_are_paged_until_the_last_page_is_short(self):
        self.connector.TRADES_PAGE_SIZE = 3
        pages = [[self.trade("A", order="OTHER", timestamp=1), self.trade("B", timestamp=2),
                  self.trade("C", order="OTHER", timestamp=3)],
                 [self.trade("D", timestamp=4), self.trade("E", order="OTHER", timestamp=5),
                  self.trade("F", timestamp=6)],
                 [self.trade("G", timestamp=7)]]
        self.ccxt.fetch_my_trades = AsyncMock(side_effect=pages)
        order = self.track_order()

        updates = await self.connector._all_trade_updates_for_order(order)

        self.assertEqual(["B", "D", "F", "G"], [update.trade_id for update in updates])  # fills of other pages found
        self.assertEqual(3, self.ccxt.fetch_my_trades.await_count)
        sinces = [call.args[1] for call in self.ccxt.fetch_my_trades.await_args_list]
        self.assertEqual([3, 6], sinces[1:])  # each page continues from the last timestamp seen, ties are deduplicated

    async def test_the_first_request_starts_at_the_order_creation_time(self):
        self.ccxt.fetch_my_trades = AsyncMock(return_value=[])
        order = self.track_order()

        await self.connector._all_trade_updates_for_order(order)

        since = self.ccxt.fetch_my_trades.await_args.args[1]
        self.assertLessEqual(since, int(order.creation_timestamp * 1000))
        self.assertGreater(since, int(order.creation_timestamp * 1000) - 60_000)  # a small safety margin only

    async def test_an_order_without_a_creation_time_is_searched_a_day_back(self):
        self.ccxt.fetch_my_trades = AsyncMock(return_value=[])
        for created in (float("nan"), -1, None):
            with self.subTest(created=created):
                order = self.track_order()
                order.creation_timestamp = created

                await self.connector._all_trade_updates_for_order(order)

                since = self.ccxt.fetch_my_trades.await_args.args[1]
                self.assertAlmostEqual(time.time() * 1000 - 86_400_000, since, delta=2000)

    async def test_a_trade_returned_by_two_pages_is_counted_once(self):
        self.connector.TRADES_PAGE_SIZE = 2
        self.ccxt.fetch_my_trades = AsyncMock(side_effect=[
            [self.trade("A", timestamp=5), self.trade("B", timestamp=6)],
            [self.trade("B", timestamp=6), self.trade("C", timestamp=7)],
            []])
        order = self.track_order()

        updates = await self.connector._all_trade_updates_for_order(order)

        self.assertEqual(["A", "B", "C"], [update.trade_id for update in updates])

    async def test_paging_cannot_loop_forever(self):
        self.connector.TRADES_PAGE_SIZE = 1
        self.ccxt.fetch_my_trades = AsyncMock(return_value=[self.trade("A", timestamp=5)])  # same page every time
        order = self.track_order()

        updates = await self.connector._all_trade_updates_for_order(order)

        self.assertEqual(["A"], [update.trade_id for update in updates])
        self.assertLessEqual(self.ccxt.fetch_my_trades.await_count, self.connector.TRADES_MAX_PAGES)

    async def test_account_trade_params_come_from_the_exchange_hook(self):
        self.ccxt.fetch_my_trades = AsyncMock(return_value=[])
        order = self.track_order()
        with patch.object(FakeConnector, "_order_trades_params", return_value={"orderId": "EX-1"}):
            await self.connector._all_trade_updates_for_order(order)
        self.assertEqual({"orderId": "EX-1"}, self.ccxt.fetch_my_trades.await_args.args[3])

    async def test_an_order_without_an_exchange_id_has_no_trades(self):
        self.ccxt.fetch_my_trades = AsyncMock()
        order = self.track_order(exchange_order_id=None)
        self.assertEqual([], await self.connector._all_trade_updates_for_order(order))
        self.ccxt.fetch_my_trades.assert_not_called()


class BalanceTests(CcxtAdapterTestCase):
    def test_balances_are_real_time_only_when_the_exchange_streams_them(self):
        self.assertTrue(self.connector.real_time_balance_update)
        no_stream = fake_client()
        no_stream.has["watchBalance"] = False
        polled = FakeConnector(api_key="k", api_secret="s", trading_pairs=[HB_PAIR], ccxt_client=no_stream)
        self.assertFalse(polled.real_time_balance_update)

    async def test_a_rest_update_replaces_the_balances(self):
        self.ccxt.fetch_balance = AsyncMock(return_value={"USDT": {"free": 1, "used": 0, "total": 1},
                                                          "BTC": {"free": 2, "used": 0, "total": 2}})
        await self.connector._update_balances()
        self.ccxt.fetch_balance = AsyncMock(return_value={"USDT": {"free": 5, "used": 0, "total": 5}})

        await self.connector._update_balances()

        self.assertEqual({"USDT": Decimal("5")}, self.connector.get_all_balances())

    async def test_a_stream_update_changes_only_the_assets_it_carries(self):
        self.connector._account_balances = {"USDT": Decimal("100"), "BTC": Decimal("2")}
        self.connector._account_available_balances = {"USDT": Decimal("90"), "BTC": Decimal("2")}
        self.connector._user_stream_tracker._user_stream = AsyncMock()
        self.connector._user_stream_tracker._user_stream.get.side_effect = [
            {"channel": "balance", "data": {"USDT": {"free": 80.0, "used": 20.0, "total": 100.0}}},
            asyncio.CancelledError]

        with self.assertRaises(asyncio.CancelledError):
            await self.connector._user_stream_event_listener()

        self.assertEqual(Decimal("2"), self.connector.get_balance("BTC"))  # untouched
        self.assertEqual(Decimal("80"), self.connector.available_balances["USDT"])


class PositionTests(CcxtAdapterTestCase):
    def position(self, **overrides):
        values = {"symbol": SYMBOL, "side": "long", "contracts": 20, "contractSize": 0.1, "entryPrice": 100.0,
                  "unrealizedPnl": 5.0, "leverage": 10}
        values.update(overrides)
        return values

    def set_mark_price(self, price: str):
        self.connector._perpetual_trading._funding_info[HB_PAIR] = FundingInfo(
            HB_PAIR, Decimal("1"), Decimal(price), 1, Decimal("0"))

    async def test_the_exchange_unrealized_pnl_is_used_when_given(self):
        self.ccxt.fetch_positions = AsyncMock(return_value=[self.position()])
        await self.connector._update_positions()
        self.assertEqual(Decimal("5"), list(self.connector.account_positions.values())[0].unrealized_pnl)

    async def test_it_is_computed_from_the_mark_price_when_the_exchange_gives_none(self):
        self.set_mark_price("103")
        self.ccxt.fetch_positions = AsyncMock(return_value=[self.position(unrealizedPnl=None)])
        await self.connector._update_positions()
        position = list(self.connector.account_positions.values())[0]
        self.assertEqual(Decimal("6"), position.unrealized_pnl)  # (103 - 100) * 2 ETH

    async def test_a_short_position_gains_when_the_price_falls(self):
        self.set_mark_price("97")
        self.ccxt.fetch_positions = AsyncMock(return_value=[self.position(unrealizedPnl=None, side="short")])
        await self.connector._update_positions()
        position = list(self.connector.account_positions.values())[0]
        self.assertEqual(Decimal("6"), position.unrealized_pnl)  # (100 - 97) * 2 ETH

    async def test_without_any_price_the_pnl_is_zero_and_the_gap_is_logged_once(self):
        self.ccxt.fetch_positions = AsyncMock(return_value=[self.position(unrealizedPnl=None)])
        await self.connector._update_positions()
        await self.connector._update_positions()
        self.assertEqual(Decimal("0"), list(self.connector.account_positions.values())[0].unrealized_pnl)
        self.assertEqual(1, sum(1 for r in self.log_records if "unrealized PnL" in r.getMessage()))

    async def test_the_exchange_can_supply_its_own_pnl_field(self):
        with patch.object(FakeConnector, "_exchange_unrealized_pnl", return_value=Decimal("7.5")):
            self.ccxt.fetch_positions = AsyncMock(return_value=[self.position(unrealizedPnl=None)])
            await self.connector._update_positions()
        self.assertEqual(Decimal("7.5"), list(self.connector.account_positions.values())[0].unrealized_pnl)

    async def test_a_position_event_without_pnl_uses_the_same_rules(self):
        self.set_mark_price("101")
        await self.connector._apply_position(self.position(unrealizedPnl=None))
        position = list(self.connector.account_positions.values())[0]
        self.assertEqual(PositionSide.LONG, position.position_side)
        self.assertEqual(Decimal("2"), position.unrealized_pnl)


class OrderEventTests(CcxtAdapterTestCase):
    async def feed(self, *events):
        self.connector._user_stream_tracker._user_stream = AsyncMock()
        self.connector._user_stream_tracker._user_stream.get.side_effect = [*events, asyncio.CancelledError]
        with self.assertRaises(asyncio.CancelledError):
            await self.connector._user_stream_event_listener()
        await asyncio.sleep(0.1)  # the tracker applies the update in a task

    async def test_an_order_event_finds_the_order_by_its_client_id_when_the_exchange_id_is_unknown(self):
        """The response that carried the exchange id may have been lost: the stream can still identify the order."""
        order = self.track_order(exchange_order_id=None)
        await self.feed({"channel": "orders", "data": {
            "id": "EX-7", "clientOrderId": "HB-1", "status": "open", "filled": 0, "lastUpdateTimestamp": 1_700_000_000_000}})
        self.assertEqual("EX-7", order.exchange_order_id)
        self.assertTrue(order.is_open)

    async def test_an_event_of_an_unknown_order_is_ignored(self):
        await self.feed({"channel": "orders", "data": {"id": "EX-404", "clientOrderId": "other", "status": "open"}})
        self.assertEqual({}, self.connector.in_flight_orders)

    async def test_an_exchange_id_match_wins_over_the_client_id(self):
        order = self.track_order(exchange_order_id="EX-1")
        await self.feed({"channel": "orders", "data": {
            "id": "EX-1", "clientOrderId": "something else", "status": "canceled", "filled": 0,
            "lastUpdateTimestamp": 1_700_000_000_000}})
        self.assertTrue(order.is_cancelled)


class PositionModeTests(CcxtAdapterTestCase):
    def test_one_way_is_the_only_mode_until_an_exchange_proves_more(self):
        self.assertEqual([PositionMode.ONEWAY], self.connector.supported_position_modes())

    async def test_an_account_in_hedge_mode_is_reported(self):
        self.ccxt.fetch_position_mode = AsyncMock(return_value={"hedged": True})
        await self.connector._check_account_position_mode()
        self.assertTrue(self.logged("ERROR", "hedge mode"))

    async def test_a_matching_account_mode_is_silent(self):
        self.ccxt.fetch_position_mode = AsyncMock(return_value={"hedged": False})
        await self.connector._check_account_position_mode()
        self.assertFalse(any(r.levelname in ("ERROR", "WARNING") for r in self.log_records))

    async def test_an_unreadable_account_mode_is_only_a_warning(self):
        self.ccxt.fetch_position_mode = AsyncMock(side_effect=ccxt.NotSupported("no"))
        await self.connector._check_account_position_mode()
        self.assertFalse(self.logged("ERROR", "hedge mode"))
        self.assertTrue(self.logged("WARNING", "position mode"))

    async def test_a_connector_with_both_modes_syncs_through_the_native_path(self):
        with patch.object(FakeConnector, "SUPPORTED_POSITION_MODES", [PositionMode.ONEWAY, PositionMode.HEDGE]):
            self.assertEqual(2, len(self.connector.supported_position_modes()))


class LoggingTests(CcxtAdapterTestCase):
    async def test_stream_errors_use_the_messages_of_hummingbots_data_sources(self):
        data_source = self.connector._user_stream_tracker.data_source
        data_source._sleep = AsyncMock()
        for error, level, message in [
                (ccxt.NetworkError("dropped"), "warning", "The websocket connection was closed (dropped)"),
                (ValueError("boom"), "exception",
                 "Unexpected error while listening to user stream. Retrying after 5 seconds...")]:
            with self.subTest(error=type(error).__name__):
                self.ccxt.watch_orders = AsyncMock(side_effect=[error, asyncio.CancelledError])
                with patch.object(data_source, "logger") as logger:
                    with self.assertRaises(asyncio.CancelledError):
                        await data_source._watch(self.ccxt.watch_orders, "orders", asyncio.Queue(), many=True)
                getattr(logger.return_value, level).assert_called_once_with(message)

    async def test_order_book_errors_use_the_messages_of_hummingbots_data_sources(self):
        data_source = self.connector._orderbook_ds
        data_source._sleep = AsyncMock()
        self.ccxt.watch_order_book = AsyncMock(side_effect=[ValueError("boom"), asyncio.CancelledError])
        with patch.object(data_source, "logger") as logger:
            with self.assertRaises(asyncio.CancelledError):
                await data_source._watch_order_book(HB_PAIR, asyncio.Queue())
        logger.return_value.exception.assert_called_once_with(
            "Unexpected error when processing public order book snapshots from exchange")


class DiscardedConnectorTests(IsolatedAsyncioWrapperTestCase):
    """
    Hummingbot's connect command builds a connector to test the keys and drops it without stopping it. ccxt then
    warns that its session was never closed, so a dropped connector must close its own ccxt session.
    """

    def make_connector(self, session):
        client = fake_client()
        client.session = session
        return FakeConnector(api_key="k", api_secret="s", trading_pairs=[HB_PAIR], ccxt_client=client), client

    async def test_a_dropped_connector_closes_its_ccxt_session(self):
        connector, client = self.make_connector(session=object())
        del connector
        gc.collect()
        await asyncio.sleep(0)
        client.close.assert_awaited_once()

    async def test_a_connector_without_an_open_session_has_nothing_to_close(self):
        connector, client = self.make_connector(session=None)
        del connector
        gc.collect()
        await asyncio.sleep(0)
        client.close.assert_not_called()

    async def test_a_stopped_connector_is_not_closed_twice(self):
        connector, client = self.make_connector(session=object())
        await connector.stop_network()
        client.session = None  # what ccxt's own close does
        client.close.reset_mock()
        del connector
        gc.collect()
        await asyncio.sleep(0)
        client.close.assert_not_called()

    def test_without_a_running_loop_the_session_is_dropped_so_ccxt_does_not_warn(self):
        client = fake_client()
        client.session = object()
        ccxt_utils.release_ccxt_client(client)
        self.assertIsNone(client.session)

    async def test_the_client_does_not_keep_its_connector_alive(self):
        connector, _ = self.make_connector(session=None)
        reference = weakref.ref(connector)
        del connector
        gc.collect()
        self.assertIsNone(reference())


class ReadRetryTests(CcxtAdapterTestCase):
    """The route to an exchange can drop a request: reads are repeated, writes never are."""

    async def test_a_timed_out_read_is_repeated(self):
        self.ccxt.load_markets = AsyncMock(side_effect=[ccxt.RequestTimeout("timed out"), {SYMBOL: MARKET}])

        result = await self.connector.ccxt_call("load_markets", True)

        self.assertEqual({SYMBOL: MARKET}, result)
        self.assertEqual(2, self.ccxt.load_markets.await_count)
        self.connector._sleep.assert_awaited_once_with(self.connector.READ_RETRY_DELAY)

    async def test_a_read_that_keeps_failing_raises_after_the_retries(self):
        self.ccxt.fetch_balance = AsyncMock(side_effect=ccxt.RequestTimeout("timed out"))
        with self.assertRaises(ccxt.RequestTimeout):
            await self.connector.ccxt_call("fetch_balance")
        self.assertEqual(1 + self.connector.READ_RETRIES, self.ccxt.fetch_balance.await_count)

    async def test_writes_are_never_repeated(self):
        for method in ("create_order", "cancel_order", "set_leverage", "set_position_mode"):
            with self.subTest(method=method):
                setattr(self.ccxt, method, AsyncMock(side_effect=ccxt.RequestTimeout("timed out")))
                with self.assertRaises(ccxt.RequestTimeout):
                    await self.connector.ccxt_call(method, "x")
                self.assertEqual(1, getattr(self.ccxt, method).await_count)

    async def test_errors_that_will_not_go_away_are_not_repeated(self):
        for error in (ccxt.ExchangeError("bad"), ccxt.AuthenticationError("bad key"), ccxt.DDoSProtection("429"),
                      ccxt.ExchangeNotAvailable("mexc GET /x 403 Forbidden")):
            with self.subTest(error=type(error).__name__):
                self.ccxt.fetch_balance = AsyncMock(side_effect=error)
                with self.assertRaises(type(error)):
                    await self.connector.ccxt_call("fetch_balance")
                self.assertEqual(1, self.ccxt.fetch_balance.await_count)

    async def test_a_server_error_is_repeated(self):
        self.ccxt.fetch_time = AsyncMock(side_effect=[ccxt.ExchangeNotAvailable("mexc GET /x 502 Bad Gateway"), 1])
        self.assertEqual(1, await self.connector.ccxt_call("fetch_time"))

    async def test_order_lookups_keep_their_own_recovery_and_are_not_repeated(self):
        """The reconciliation of a lost order asks a few times itself, repeating inside would multiply it."""
        self.ccxt.fetch_order = AsyncMock(side_effect=ccxt.RequestTimeout("timed out"))
        with self.assertRaises(ccxt.RequestTimeout):
            await self.connector.ccxt_call("fetch_order", "1", SYMBOL)
        self.assertEqual(1, self.ccxt.fetch_order.await_count)


class MarketsLoadedByTheConnectorTests(IsolatedAsyncioWrapperTestCase):
    """ccxt loads markets by itself when it has none: for some exchanges that means spot markets and private calls."""

    async def test_markets_are_loaded_by_the_connector_before_the_first_call_that_needs_them(self):
        client = fake_client()
        client.fetch_balance = AsyncMock(return_value={})
        connector = FakeConnector(api_key="k", api_secret="s", trading_pairs=[HB_PAIR], ccxt_client=client)

        await connector.ccxt_call("fetch_balance")
        await connector.ccxt_call("fetch_balance")

        client.load_markets.assert_awaited_once()  # by the connector, once
        self.assertEqual(Decimal("0.1"), connector.contract_size(SYMBOL))

    async def test_calls_that_need_no_markets_do_not_load_them(self):
        client = fake_client()
        connector = FakeConnector(api_key="k", api_secret="s", trading_pairs=[HB_PAIR], ccxt_client=client)
        await connector.ccxt_call("fetch_time")
        client.load_markets.assert_not_called()
