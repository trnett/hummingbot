from decimal import Decimal
from test.hummingbot.connector.ccxt_adapter.live_checks import check_within_cap, far_buy_price, order_amount
from unittest import TestCase

from hummingbot.connector.trading_rule import TradingRule


def rule(**overrides) -> TradingRule:
    values = dict(trading_pair="ETH-USDT", min_order_size=Decimal("0.01"), min_price_increment=Decimal("0.05"),
                  min_base_amount_increment=Decimal("0.01"), min_notional_size=Decimal("5"))
    values.update(overrides)
    return TradingRule(**values)


class LiveChecksHelpersTests(TestCase):
    """The sizing of the live checklist is what keeps its orders tiny, so it is tested like any other code."""

    def test_the_resting_price_is_far_below_the_bid_and_on_the_price_grid(self):
        price = far_buy_price(Decimal("2000.03"), rule())
        self.assertEqual(Decimal("1900.00"), price)  # 5% below, rounded down to 0.05
        self.assertEqual(0, price % Decimal("0.05"))

    def test_the_amount_covers_the_minimum_notional_and_the_size_grid(self):
        amount = order_amount(rule(), Decimal("1900"), target_notional=Decimal("1"))
        self.assertGreaterEqual(amount * Decimal("1900"), Decimal("5"))  # the exchange minimum wins over the target
        self.assertEqual(0, amount % Decimal("0.01"))

    def test_the_amount_follows_the_target_when_it_is_above_the_minimums(self):
        amount = order_amount(rule(), Decimal("1900"), target_notional=Decimal("19"))
        self.assertEqual(Decimal("0.01"), amount)  # 19 / 1900

    def test_the_amount_is_rounded_up_never_down(self):
        amount = order_amount(rule(min_order_size=Decimal("0.001")), Decimal("1900"), Decimal("20"))
        self.assertGreaterEqual(amount * Decimal("1900"), Decimal("20"))

    def test_an_order_above_the_cap_is_refused(self):
        with self.assertRaises(AssertionError):
            check_within_cap(Decimal("1"), Decimal("2000"), Decimal("15"))
        check_within_cap(Decimal("0.005"), Decimal("2000"), Decimal("15"))
