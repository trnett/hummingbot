"""
Aster futures API V3 rate limits as Aster documents and returns them in /fapi/v3/exchangeInfo: 2400 request weight per
minute per IP, 1200 orders per minute and 300 orders per 10 seconds per account, and a weight for every endpoint.
"""
import unittest

from hummingbot.connector.derivative.aster_perpetual import aster_perpetual_constants as CONSTANTS


def resolve(path, api="fapiPrivate", method="GET", params=None):
    return CONSTANTS.limit_id_for_request(path, api, method, params or {})


class AsterPerpetualRateLimitTests(unittest.TestCase):
    def limits(self):
        return {limit.limit_id: limit for limit in CONSTANTS.RATE_LIMITS}

    def weight_of(self, limit_id):
        pairs = {pair.limit_id: pair.weight for pair in self.limits()[limit_id].linked_limits}
        return pairs.get(CONSTANTS.REQUEST_WEIGHT_LIMIT_ID)

    def test_the_account_wide_limits_are_the_ones_of_the_exchange_info(self):
        limits = self.limits()
        self.assertEqual((2400, 60), (limits[CONSTANTS.REQUEST_WEIGHT_LIMIT_ID].limit,
                                      limits[CONSTANTS.REQUEST_WEIGHT_LIMIT_ID].time_interval))
        self.assertEqual((1200, 60), (limits[CONSTANTS.ORDERS_1M_LIMIT_ID].limit,
                                      limits[CONSTANTS.ORDERS_1M_LIMIT_ID].time_interval))
        self.assertEqual((300, 10), (limits[CONSTANTS.ORDERS_10S_LIMIT_ID].limit,
                                     limits[CONSTANTS.ORDERS_10S_LIMIT_ID].time_interval))

    def test_ccxt_v1_paths_are_resolved_on_their_v3_endpoint(self):
        self.assertEqual("GET v3/account", resolve("v4/account"))
        self.assertEqual("GET v3/positionRisk", resolve("v2/positionRisk"))
        self.assertEqual("GET v3/order", resolve("v1/order"))
        self.assertEqual("DELETE v3/order", resolve("v1/order", method="DELETE"))
        self.assertEqual("GET v3/leverageBrackets", resolve("v1/leverageBracket"))

    def test_documented_weights(self):
        cases = {
            "GET v3/time": 1, "GET v3/exchangeInfo": 1, "GET v3/premiumIndex": 1, "GET v3/fundingRate": 1,
            "GET v3/order": 1, "DELETE v3/order": 1, "GET v3/openOrder": 1, "GET v3/allOrders": 5,
            "GET v3/account": 5, "GET v3/balance": 5, "GET v3/positionRisk": 5, "GET v3/userTrades": 5,
            "GET v3/income": 30, "GET v3/positionSide/dual": 30, "POST v3/positionSide/dual": 1,
            "POST v3/leverage": 1, "POST v3/marginType": 1, "GET v3/leverageBrackets": 1,
            "GET v3/commissionRate": 20, "POST v3/listenKey": 1, "PUT v3/listenKey": 1, "DELETE v3/listenKey": 1,
        }
        for limit_id, weight in cases.items():
            with self.subTest(limit_id=limit_id):
                self.assertEqual(weight, self.weight_of(limit_id))

    def test_the_weight_of_depth_follows_its_limit(self):
        for limit, weight in {5: 2, 10: 2, 20: 2, 50: 2, 100: 5, 500: 10, 1000: 20}.items():
            with self.subTest(limit=limit):
                self.assertEqual(weight, self.weight_of(resolve("v1/depth", "fapiPublic", params={"limit": limit})))
        self.assertEqual(10, self.weight_of(resolve("v1/depth", "fapiPublic")))  # default limit is 500

    def test_open_orders_and_the_ticker_cost_40_without_a_symbol(self):
        self.assertEqual(1, self.weight_of(resolve("v1/openOrders", params={"symbol": "BTCUSDT"})))
        self.assertEqual(40, self.weight_of(resolve("v1/openOrders")))
        self.assertEqual(1, self.weight_of(resolve("v1/ticker/24hr", "fapiPublic", params={"symbol": "BTCUSDT"})))
        self.assertEqual(40, self.weight_of(resolve("v1/ticker/24hr", "fapiPublic")))

    def test_a_new_order_has_no_weight_but_counts_in_both_order_limits(self):
        limit_id = resolve("v1/order", method="POST")
        self.assertEqual("POST v3/order", limit_id)
        self.assertIsNone(self.weight_of(limit_id))
        linked = {pair.limit_id: pair.weight for pair in self.limits()[limit_id].linked_limits}
        self.assertEqual({CONSTANTS.ORDERS_1M_LIMIT_ID: 1, CONSTANTS.ORDERS_10S_LIMIT_ID: 1}, linked)

    def test_an_unknown_endpoint_is_charged_a_cautious_weight(self):
        limit_id = resolve("v1/somethingNew")
        self.assertEqual(CONSTANTS.DEFAULT_LIMIT_ID, limit_id)
        self.assertEqual(CONSTANTS.DEFAULT_WEIGHT, self.weight_of(limit_id))

    def test_every_resolvable_limit_id_is_defined(self):
        defined = set(self.limits())
        for path, api, method, params in [
            ("v1/order", "fapiPrivate", "GET", {}), ("v1/order", "fapiPrivate", "POST", {}),
            ("v1/openOrders", "fapiPrivate", "GET", {}), ("v1/depth", "fapiPublic", "GET", {"limit": 100}),
            ("v1/ticker/24hr", "fapiPublic", "GET", {}), ("v4/account", "fapiPrivate", "GET", {}),
            ("v1/nothing", "fapiPublic", "GET", {}),
        ]:
            self.assertIn(resolve(path, api, method, params), defined)

    def test_the_linked_account_limits_are_defined(self):
        defined = set(self.limits())
        for limit in CONSTANTS.RATE_LIMITS:
            for pair in limit.linked_limits:
                self.assertIn(pair.limit_id, defined)
