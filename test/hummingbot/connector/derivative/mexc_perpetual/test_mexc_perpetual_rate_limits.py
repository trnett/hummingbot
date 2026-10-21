"""
MEXC contract API rate limits as MEXC documents them (mexcdevelop.github.io/apidocs/contract_v1_en): every endpoint has
its own budget of 20 requests per 2 seconds, except contract/detail (1 per 5 seconds) and order/submit_batch (1 per
2 seconds).
"""
import unittest

from hummingbot.connector.derivative.mexc_perpetual import mexc_perpetual_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler


def resolve(path, api=("contract", "private"), method="GET", params=None):
    return CONSTANTS.limit_id_for_request(path, list(api), method, params or {})


class MexcPerpetualRateLimitTests(unittest.TestCase):
    def limits(self):
        return {limit.limit_id: limit for limit in CONSTANTS.RATE_LIMITS}

    def test_requests_are_mapped_to_the_endpoint_template_they_belong_to(self):
        cases = [
            ("detail", ("contract", "public"), "detail"),
            ("ticker", ("contract", "public"), "ticker"),
            ("depth/XRP_USDT", ("contract", "public"), "depth/{symbol}"),
            ("funding_rate/XRP_USDT", ("contract", "public"), "funding_rate/{symbol}"),
            ("funding_rate/history", ("contract", "public"), "funding_rate/history"),
            ("account/assets", ("contract", "private"), "account/assets"),
            ("position/open_positions", ("contract", "private"), "position/open_positions"),
            ("position/funding_records", ("contract", "private"), "position/funding_records"),
            ("order/get/102015012431820288", ("contract", "private"), "order/get/{order_id}"),
            ("order/external/XRP_USDT/HBOT-1", ("contract", "private"), "order/external/{symbol}/{external_oid}"),
            ("order/deal_details/1", ("contract", "private"), "order/deal_details/{order_id}"),
            ("order/list/open_orders/XRP_USDT", ("contract", "private"), "order/list/open_orders/{symbol}"),
            ("order/submit", ("contract", "private"), "order/submit"),
            ("order/cancel", ("contract", "private"), "order/cancel"),
            ("position/change_leverage", ("contract", "private"), "position/change_leverage"),
            ("position/change_position_mode", ("contract", "private"), "position/change_position_mode"),
        ]
        for path, api, expected in cases:
            with self.subTest(path=path):
                self.assertEqual(expected, resolve(path, api))

    def test_every_endpoint_has_its_own_budget_of_20_requests_per_2_seconds(self):
        limits = self.limits()
        for endpoint in ("ticker", "depth/{symbol}", "account/assets", "order/get/{order_id}", "order/submit",
                         "order/cancel", "position/open_positions", "position/change_leverage"):
            with self.subTest(endpoint=endpoint):
                self.assertEqual(20, limits[endpoint].limit)
                self.assertEqual(2, limits[endpoint].time_interval)
                self.assertEqual([], limits[endpoint].linked_limits)  # independent of the other endpoints

    def test_the_contract_detail_endpoint_is_limited_to_1_request_per_5_seconds(self):
        limit = self.limits()["detail"]
        self.assertEqual((1, 5), (limit.limit, limit.time_interval))

    def test_batch_orders_are_limited_to_1_request_per_2_seconds(self):
        limit = self.limits()["order/submit_batch"]
        self.assertEqual((1, 2), (limit.limit, limit.time_interval))

    def test_an_unknown_endpoint_falls_back_to_a_shared_budget_that_exists(self):
        limit_id = resolve("something/new")
        self.assertEqual(CONSTANTS.DEFAULT_LIMIT_ID, limit_id)
        self.assertEqual((20, 2), (self.limits()[limit_id].limit, self.limits()[limit_id].time_interval))

    def test_every_mapped_limit_id_is_defined(self):
        defined = set(self.limits())
        for template in CONSTANTS.ENDPOINT_TEMPLATES:
            self.assertIn(template, defined)

    def test_the_budget_of_one_endpoint_does_not_slow_another_one(self):
        throttler = AsyncThrottler(CONSTANTS.RATE_LIMITS)
        related, _ = throttler.get_related_limits("order/submit")
        other, _ = throttler.get_related_limits("order/get/{order_id}")
        self.assertIsNot(related, other)
        self.assertEqual([], related.linked_limits)
