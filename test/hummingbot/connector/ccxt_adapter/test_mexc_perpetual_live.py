"""
LIVE test of the MEXC perpetual connector with real money in tiny sizes. Skipped unless enabled.

    HBOT_LIVE_TEST=1 MEXC_PERP_API_KEY=... MEXC_PERP_API_SECRET=... \\
    python -m pytest test/hummingbot/connector/ccxt_adapter/test_mexc_perpetual_live.py -s

Optional: HBOT_LIVE_PAIR (default XRP-USDT), HBOT_LIVE_MAX_NOTIONAL (default 15, in quote asset),
HBOT_LIVE_ALLOW_TRADE=1 to also open and close a minimal position at market. Use an API key without withdrawal rights
and an account in one-way mode.
"""
import os
import unittest
from decimal import Decimal
from test.hummingbot.connector.ccxt_adapter.live_checks import LiveChecks, env_flag
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.derivative.mexc_perpetual.mexc_perpetual_derivative import MexcPerpetualDerivative

API_KEY = os.environ.get("MEXC_PERP_API_KEY", "")
API_SECRET = os.environ.get("MEXC_PERP_API_SECRET", "")
ENABLED = env_flag(os.environ, "HBOT_LIVE_TEST") and bool(API_KEY and API_SECRET)


async def check_wallet_balance_formula(checks: LiveChecks):
    """The connector's wallet balance (available + frozen + position margin) against MEXC's own cashBalance."""
    raw = (await checks.ccxt("fetch_balance"))["info"]["data"]
    quote = checks.pair.split("-")[1]
    asset = next(a for a in raw if a["currency"] == quote)
    formula = Decimal(str(asset["availableBalance"])) + Decimal(str(asset["frozenBalance"])) \
        + Decimal(str(asset["positionMargin"]))
    cash = Decimal(str(asset["cashBalance"]))
    status = "OK" if formula == cash else "WARN"
    checks.record("balance formula", status, f"available+frozen+margin = {formula}, cashBalance = {cash}"
                                             + ("" if status == "OK" else ". They differ: tell the developer."))


@unittest.skipUnless(ENABLED, "live test: set HBOT_LIVE_TEST=1, MEXC_PERP_API_KEY and MEXC_PERP_API_SECRET")
class MexcPerpetualLiveTests(IsolatedAsyncioWrapperTestCase):
    async def test_live_checklist(self):
        pair = os.environ.get("HBOT_LIVE_PAIR", "XRP-USDT")
        connector = MexcPerpetualDerivative(
            mexc_perpetual_api_key=API_KEY, mexc_perpetual_api_secret=API_SECRET, trading_pairs=[pair])
        async with LiveChecks(connector, pair, Decimal(os.environ.get("HBOT_LIVE_MAX_NOTIONAL", "15")),
                              env_flag(os.environ, "HBOT_LIVE_ALLOW_TRADE"),
                              extra_checks=[check_wallet_balance_formula]) as checks:
            await checks.run_all()
