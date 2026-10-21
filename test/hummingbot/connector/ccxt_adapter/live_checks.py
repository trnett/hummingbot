"""
Live checklist for the ccxt based perpetual connectors. It trades REAL MONEY in tiny sizes, so it only runs when the
live test modules are enabled with environment variables (see ``test_*_live.py``).

It starts the connector the way the Hummingbot client does (a real time clock, the network check, ``start_network``)
and then checks, one step at a time, what no mocked test can: permissions, order lifecycle, fills, fees, positions,
balances. Every step is recorded in a report that is printed at the end, so the numbers can be compared by eye with
the exchange's own screens.

Safety: orders are capped by ``max_notional``, a far post-only order is the default and the market round trip must be
enabled explicitly, a pair with an open position or open orders is refused.
"""
import asyncio
import time
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from hummingbot.connector.ccxt_adapter import ccxt_utils
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.clock import Clock, ClockMode
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode
from hummingbot.core.network_iterator import NetworkStatus


def far_buy_price(best_bid: Decimal, rule: TradingRule, distance: Decimal = Decimal("0.05")) -> Decimal:
    """A buy price far below the market (so a post-only order rests and never fills), on the exchange's price grid."""
    price = best_bid * (Decimal("1") - distance)
    step = rule.min_price_increment
    return (price / step).to_integral_value(rounding=ROUND_DOWN) * step


def order_amount(rule: TradingRule, price: Decimal, target_notional: Decimal) -> Decimal:
    """The smallest order of about ``target_notional`` that the exchange accepts, on its size grid."""
    amount = max(rule.min_order_size, target_notional / price, rule.min_notional_size / price)
    step = rule.min_base_amount_increment
    return (amount / step).to_integral_value(rounding=ROUND_UP) * step


def check_within_cap(amount: Decimal, price: Decimal, max_notional: Decimal):
    notional = amount * price
    if notional > max_notional:
        raise AssertionError(f"The smallest possible order is worth {notional:.2f} which is above the cap of "
                             f"{max_notional}. Raise HBOT_LIVE_MAX_NOTIONAL or choose a cheaper pair.")


class LiveChecks:
    def __init__(self, connector: Any, trading_pair: str, max_notional: Decimal, allow_trading: bool,
                 extra_checks: Optional[List[Callable[["LiveChecks"], Awaitable[None]]]] = None):
        self.connector = connector
        self.pair = trading_pair
        self.max_notional = max_notional
        self.allow_trading = allow_trading
        self.extra_checks = extra_checks or []
        self.report: List[Tuple[str, str, str]] = []
        self._clock: Optional[Clock] = None
        self._clock_task: Optional[asyncio.Task] = None

    # ---- lifecycle -------------------------------------------------------------------------------------------
    async def __aenter__(self) -> "LiveChecks":
        self._clock = Clock(ClockMode.REALTIME, tick_size=1.0)
        self._clock.add_iterator(self.connector)
        self._clock.__enter__()  # the clock must be entered to start its iterators, as the client does with `with`
        self._clock_task = asyncio.ensure_future(self._clock.run())
        await self.wait_until(lambda: self.connector.ready, 90, "the connector to be ready",
                              hint=lambda: f"status: {self.connector.status_dict}")
        return self

    async def __aexit__(self, *exc_info):
        if self._clock_task is not None:
            self._clock_task.cancel()
            await asyncio.gather(self._clock_task, return_exceptions=True)
        if self._clock is not None:
            self._clock.__exit__(None, None, None)
        await self.connector.stop_network()

    # ---- helpers ---------------------------------------------------------------------------------------------
    async def wait_until(self, predicate: Callable[[], bool], timeout: float, description: str,
                         hint: Callable[[], str] = lambda: "") -> float:
        started = time.time()
        while not predicate():
            if time.time() - started > timeout:
                raise AssertionError(f"Timed out after {timeout}s waiting for {description}. {hint()}")
            await asyncio.sleep(0.25)
        return time.time() - started

    def record(self, step: str, status: str, detail: str):
        self.report.append((step, status, detail))
        print(f"[{status:^5}] {step}: {detail}")

    @property
    def rule(self) -> TradingRule:
        return self.connector.trading_rules[self.pair]

    @property
    def symbol(self) -> str:
        return ccxt_utils.trading_pair_to_ccxt_symbol(self.pair)

    async def ccxt(self, method: str, *args, **kwargs):
        return await self.connector.ccxt_call(method, *args, **kwargs)

    # ---- checklist -------------------------------------------------------------------------------------------
    async def run_all(self):
        await self.check_connectivity()
        await self.check_balances()
        await self.check_position_mode()
        await self.check_clean_pair()
        await self.check_resting_order_lifecycle()
        if self.allow_trading:
            await self.check_leverage()
            await self.check_market_round_trip()
        else:
            self.record("market round trip", "SKIP", "set HBOT_LIVE_ALLOW_TRADE=1 to trade a minimal size")
        await self.check_funding()
        for check in self.extra_checks:
            await check(self)
        self.print_report()

    async def check_connectivity(self):
        status = await self.connector.check_network()
        assert status == NetworkStatus.CONNECTED, f"network status is {status}"
        self.record("connectivity", "OK", f"{self.connector.name} ready, status {self.connector.status_dict}")

    async def check_balances(self):
        quote = self.pair.split("-")[1]
        total, available = self.connector.get_balance(quote), self.connector.get_available_balance(quote)
        assert total >= available >= 0, f"inconsistent balances total={total} available={available}"
        self.record("balances", "OK", f"{quote}: total {total}, available {available} (compare with the exchange)")

    async def check_position_mode(self):
        mode = await self.connector._fetch_account_position_mode()
        assert mode == PositionMode.ONEWAY, (
            f"The account position mode is {mode}. Switch the account to one-way mode before testing: this "
            f"connector supports only one-way mode.")
        self.record("position mode", "OK", "the account is in one-way mode")

    async def check_clean_pair(self):
        await self.connector._update_positions()
        open_positions = [p for p in self.connector.account_positions.values() if p.trading_pair == self.pair]
        assert not open_positions, f"{self.pair} already has a position {open_positions}. Close it or use another pair."
        open_orders = await self.ccxt("fetch_open_orders", self.symbol)
        assert not open_orders, f"{self.pair} already has open orders {open_orders}. Cancel them or use another pair."
        self.record("clean pair", "OK", f"no position and no open order on {self.pair}")

    async def check_resting_order_lifecycle(self):
        """A far post-only buy: created, visible on the exchange, cancelled. It must never fill."""
        best_bid = self.connector.get_price(self.pair, False)
        price = far_buy_price(best_bid, self.rule)
        amount = order_amount(self.rule, price, self.max_notional / 2)
        check_within_cap(amount, price, self.max_notional)

        started = time.time()
        order_id = self.connector.buy(self.pair, amount, OrderType.LIMIT_MAKER, price,
                                      position_action=PositionAction.OPEN)
        order = await self._tracked(order_id)
        created_after = await self.wait_until(lambda: order.exchange_order_id is not None and order.is_open, 30,
                                              "the resting order to be created",
                                              hint=lambda: f"state {order.current_state}")
        remote = await self.ccxt("fetch_order", order.exchange_order_id, self.symbol)
        remote_amount = Decimal(str(remote["amount"])) * self.connector.contract_size(self.symbol)
        assert remote_amount == amount, f"the exchange has {remote_amount} where {amount} was sent (contract size?)"
        assert Decimal(str(remote["price"])) == price, f"the exchange price {remote['price']} is not {price}"
        listed = [o["id"] for o in await self.ccxt("fetch_open_orders", self.symbol)]
        assert order.exchange_order_id in listed, (
            f"the order {order.exchange_order_id} is not in the open orders {listed}")

        self.connector.cancel(self.pair, order_id)
        cancelled_after = await self.wait_until(lambda: order.is_cancelled, 30, "the order to be cancelled",
                                                hint=lambda: f"state {order.current_state}")
        assert not [o for o in await self.ccxt("fetch_open_orders", self.symbol)], "the order is still open remotely"
        self.record("resting order", "OK", f"{amount} @ {price} created in {created_after:.1f}s, cancelled in "
                                           f"{cancelled_after:.1f}s (total {time.time() - started:.1f}s)")

    async def check_leverage(self):
        success, message = await self.connector._set_trading_pair_leverage(self.pair, 1)
        assert success, f"setting the leverage to 1 failed: {message}"
        self.record("leverage", "OK", "set to 1x")

    async def check_market_round_trip(self):
        """Opens and closes the smallest position at market: fills, fees, position and balances are cross-checked."""
        price = self.connector.get_price(self.pair, True)
        amount = order_amount(self.rule, price, self.max_notional / 2)
        check_within_cap(amount, price, self.max_notional)
        balance_before = self.connector.get_balance(self.pair.split("-")[1])

        buy = await self._market_order(is_buy=True, amount=amount, action=PositionAction.OPEN)
        await self.wait_until(lambda: self._position_amount() == amount, 60, "the long position to appear",
                              hint=lambda: f"positions {self.connector.account_positions}")
        remote = [p for p in await self.ccxt("fetch_positions") if p["symbol"] == self.symbol]
        remote_amount = sum(Decimal(str(p["contracts"])) for p in remote) * self.connector.contract_size(self.symbol)
        assert remote_amount == amount, f"the exchange position is {remote_amount}, Hummingbot has {amount}"
        self.record("position", "OK", f"long {amount} {self.pair}, matches the exchange")

        sell = await self._market_order(is_buy=False, amount=amount, action=PositionAction.CLOSE)
        await self.wait_until(lambda: self._position_amount() == 0, 60, "the position to close",
                              hint=lambda: f"positions {self.connector.account_positions}")
        remote = [p for p in await self.ccxt("fetch_positions") if p["symbol"] == self.symbol and p["contracts"]]
        assert not remote, f"the exchange still has a position {remote}"

        await self._compare_fills([buy, sell])
        await asyncio.sleep(3)
        await self.connector._update_balances()
        cost = balance_before - self.connector.get_balance(self.pair.split("-")[1])
        quote = self.pair.split("-")[1]
        self.record("round trip cost", "OK",
                    f"balance change {-cost} {quote} (spread plus fees, compare with the exchange statement)")

    async def _market_order(self, is_buy: bool, amount: Decimal, action: PositionAction):
        place = self.connector.buy if is_buy else self.connector.sell
        order_id = place(self.pair, amount, OrderType.MARKET, Decimal("NaN"), position_action=action)
        order = await self._tracked(order_id)
        filled_after = await self.wait_until(lambda: order.is_done, 60, "the market order to complete",
                                             hint=lambda: f"state {order.current_state}")
        assert order.is_filled, f"the market order ended as {order.current_state}"
        self.record(f"market {'buy' if is_buy else 'sell'}", "OK",
                    f"{amount} {action.name} filled in {filled_after:.1f}s, average price {order.average_executed_price}")
        return order

    async def _tracked(self, order_id: str):
        """
        ``buy`` and ``sell`` return at once, the order is tracked when the order creation task starts. A market order
        can be filled and leave the active orders before the next look, so the cached orders are searched too.
        """
        tracker = self.connector._order_tracker
        await self.wait_until(lambda: tracker.fetch_order(client_order_id=order_id) is not None, 10,
                              f"order {order_id} to be tracked")
        return tracker.fetch_order(client_order_id=order_id)

    def _position_amount(self) -> Decimal:
        return sum((abs(p.amount) for p in self.connector.account_positions.values() if p.trading_pair == self.pair),
                   Decimal("0"))

    async def _compare_fills(self, orders: List[Any]):
        """Fills Hummingbot recorded against the exchange's own trade list, and against the recovery path."""
        remote_trades = await self.ccxt("fetch_my_trades", self.symbol)
        for order in orders:
            recovered = await self.connector._all_trade_updates_for_order(order)
            tracked_ids = set(order.order_fills.keys())
            recovered_ids = {update.trade_id for update in recovered}
            assert tracked_ids == recovered_ids, f"tracked fills {tracked_ids} differ from recovered {recovered_ids}"
            remote = [t for t in remote_trades if str(t.get("order")) == order.exchange_order_id]
            assert {str(t["id"]) for t in remote} == tracked_ids, (
                f"the exchange lists trades {[t['id'] for t in remote]}, Hummingbot has {tracked_ids}")
            fees = sum((fee.amount for update in order.order_fills.values() for fee in update.fee.flat_fees),
                       Decimal("0"))
            remote_fees = sum(Decimal(str((t.get("fee") or {}).get("cost") or 0)) for t in remote)
            assert fees == remote_fees, f"fee mismatch: Hummingbot {fees}, exchange {remote_fees}"
            self.record("fills", "OK", f"{order.client_order_id}: {len(tracked_ids)} trade(s), fee {fees} "
                                       f"match the exchange and the recovery path")

    async def check_funding(self):
        info = self.connector.get_funding_info(self.pair)
        last = await self.connector._fetch_last_fee_payment(self.pair)
        self.record("funding", "INFO", f"rate {info.rate}, mark {info.mark_price}, index {info.index_price}, next "
                                       f"{info.next_funding_utc_timestamp}; last payment (time, rate, amount) {last}. "
                                       f"Check the sign of the amount against the exchange (negative means paid).")

    def print_report(self):
        print("\n=== live check report ===")
        for step, status, detail in self.report:
            print(f"{status:>5}  {step:<18} {detail}")


def env_flag(environ: Dict[str, str], name: str) -> bool:
    return environ.get(name, "") == "1"
