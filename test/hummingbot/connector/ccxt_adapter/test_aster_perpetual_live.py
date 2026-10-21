"""
LIVE test of the Aster perpetual connector with real money in tiny sizes. Skipped unless enabled.

    HBOT_LIVE_TEST=1 ASTER_PERP_USER=0x... ASTER_PERP_SIGNER=0x... ASTER_PERP_PRIVATE_KEY=0x... \\
    python -m pytest test/hummingbot/connector/ccxt_adapter/test_aster_perpetual_live.py -s

Optional: HBOT_LIVE_PAIR (default ETH-USDT), HBOT_LIVE_MAX_NOTIONAL (default 15, in quote asset),
HBOT_LIVE_ALLOW_TRADE=1 to also open and close a minimal position at market. ASTER_PERP_USER is the main wallet, ASTER_PERP_SIGNER
the address and ASTER_PERP_PRIVATE_KEY the private key of an API wallet (Pro API, V3). An API wallet cannot withdraw.
Use an account in one-way mode. Aster has no private stream in ccxt: fills and balances are polled every 5 seconds,
so the waits in this test are longer than on MEXC.
"""
import os
import unittest
from decimal import Decimal
from test.hummingbot.connector.ccxt_adapter.live_checks import LiveChecks, env_flag
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase

from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_derivative import AsterPerpetualDerivative

USER = os.environ.get("ASTER_PERP_USER", "")
SIGNER = os.environ.get("ASTER_PERP_SIGNER", "")
PRIVATE_KEY = os.environ.get("ASTER_PERP_PRIVATE_KEY", "")
ENABLED = env_flag(os.environ, "HBOT_LIVE_TEST") and bool(USER and SIGNER and PRIVATE_KEY)


@unittest.skipUnless(ENABLED, "live test: set HBOT_LIVE_TEST=1, ASTER_PERP_USER, ASTER_PERP_SIGNER and ASTER_PERP_PRIVATE_KEY")
class AsterPerpetualLiveTests(IsolatedAsyncioWrapperTestCase):
    async def test_live_checklist(self):
        pair = os.environ.get("HBOT_LIVE_PAIR", "ETH-USDT")
        connector = AsterPerpetualDerivative(
            aster_perpetual_user=USER, aster_perpetual_signer=SIGNER, aster_perpetual_private_key=PRIVATE_KEY,
            trading_pairs=[pair])
        async with LiveChecks(connector, pair, Decimal(os.environ.get("HBOT_LIVE_MAX_NOTIONAL", "15")),
                              env_flag(os.environ, "HBOT_LIVE_ALLOW_TRADE")) as checks:
            await checks.run_all()
