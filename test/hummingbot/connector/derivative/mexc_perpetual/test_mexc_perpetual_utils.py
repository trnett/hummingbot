import unittest

from hummingbot.connector.derivative.mexc_perpetual import mexc_perpetual_utils as utils


class MexcPerpetualUtilsTests(unittest.TestCase):
    def test_trading_pair_to_ccxt_symbol(self):
        self.assertEqual("BTC/USDT:USDT", utils.trading_pair_to_ccxt_symbol("BTC-USDT"))

    def test_ccxt_symbol_to_trading_pair(self):
        self.assertEqual("BTC-USDT", utils.ccxt_symbol_to_trading_pair("BTC/USDT:USDT"))

    def test_symbol_round_trip(self):
        for pair in ["BTC-USDT", "ETH-USDC", "COINALPHA-HBOT"]:
            self.assertEqual(pair, utils.ccxt_symbol_to_trading_pair(utils.trading_pair_to_ccxt_symbol(pair)))

    def test_is_exchange_information_valid_accepts_active_linear_swap(self):
        market = {"type": "swap", "swap": True, "linear": True, "active": True}
        self.assertTrue(utils.is_exchange_information_valid(market))

    def test_is_exchange_information_valid_rejects_inverse_spot_and_inactive(self):
        self.assertFalse(utils.is_exchange_information_valid({"type": "swap", "swap": True, "linear": False, "active": True}))
        self.assertFalse(utils.is_exchange_information_valid({"type": "spot", "swap": False, "linear": None, "active": True}))
        self.assertFalse(utils.is_exchange_information_valid({"type": "swap", "swap": True, "linear": True, "active": False}))

    def test_config_map_has_api_credentials(self):
        self.assertIn("mexc_perpetual_api_key", utils.KEYS.__class__.model_fields)
        self.assertIn("mexc_perpetual_api_secret", utils.KEYS.__class__.model_fields)
        self.assertEqual("mexc_perpetual", utils.KEYS.connector)

    def test_default_fees_are_defined(self):
        self.assertIsNotNone(utils.DEFAULT_FEES.maker_percent_fee_decimal)
        self.assertIsNotNone(utils.DEFAULT_FEES.taker_percent_fee_decimal)
