import unittest

from hummingbot.connector.derivative.aster_perpetual import aster_perpetual_utils as utils


class AsterPerpetualUtilsTests(unittest.TestCase):
    def test_symbol_round_trip(self):
        for pair in ["BTC-USDT", "ETH-USDT", "COINALPHA-HBOT"]:
            self.assertEqual(pair, utils.ccxt_symbol_to_trading_pair(utils.trading_pair_to_ccxt_symbol(pair)))
        self.assertEqual("BTC/USDT:USDT", utils.trading_pair_to_ccxt_symbol("BTC-USDT"))

    def test_only_active_linear_swaps_are_valid(self):
        self.assertTrue(utils.is_exchange_information_valid({"type": "swap", "swap": True, "linear": True,
                                                             "active": True}))
        self.assertFalse(utils.is_exchange_information_valid({"type": "spot", "swap": False, "active": True}))
        self.assertFalse(utils.is_exchange_information_valid({"type": "swap", "swap": True, "linear": True,
                                                              "active": False}))

    def test_config_map_asks_for_the_api_wallet_not_for_a_v1_api_key(self):
        fields = utils.KEYS.__class__.model_fields
        self.assertEqual(
            ["aster_perpetual_user", "aster_perpetual_signer", "aster_perpetual_private_key"],
            [name for name in fields if name.startswith("aster_perpetual_")])
        for field in fields.values():
            if field.json_schema_extra and field.json_schema_extra.get("is_connect_key"):
                self.assertTrue(field.json_schema_extra["is_secure"])
        self.assertEqual("aster_perpetual", utils.KEYS.connector)

    def test_default_fees_are_defined(self):
        self.assertIsNotNone(utils.DEFAULT_FEES.maker_percent_fee_decimal)
        self.assertIsNotNone(utils.DEFAULT_FEES.taker_percent_fee_decimal)
