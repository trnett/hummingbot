import asyncio
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock

import ccxt

from hummingbot.connector.derivative.mexc_perpetual.mexc_perpetual_derivative import MexcPerpetualDerivative

HB_PAIR = "COINALPHA-HBOT"
CCXT_SYMBOL = "COINALPHA/HBOT:HBOT"

MARKET = {
    "id": "COINALPHA_HBOT", "symbol": CCXT_SYMBOL, "base": "COINALPHA", "quote": "HBOT", "settle": "HBOT",
    "type": "swap", "swap": True, "linear": True, "inverse": False, "active": True, "contract": True,
    "contractSize": 0.1, "precision": {"amount": 1, "price": 0.01},
    "limits": {"amount": {"min": 1, "max": 1_000_000}, "cost": {"min": None}},
}


def fake_ccxt_client(watch_positions: Optional[bool] = None) -> MagicMock:
    """A stand-in for ``ccxt.pro.mexc`` that serves the markets, for the tests that exercise the streams."""
    client = MagicMock()
    client.has = {"watchOrders": True, "watchMyTrades": True, "watchBalance": True, "watchPositions": watch_positions}
    client.precisionMode = ccxt.TICK_SIZE
    client.fetch_swap_markets = AsyncMock(return_value=[MARKET])
    client.set_markets = MagicMock(return_value={CCXT_SYMBOL: MARKET})
    client.close = AsyncMock()
    return client


def make_exchange(client: MagicMock, trading_required: bool = True) -> MexcPerpetualDerivative:
    return MexcPerpetualDerivative(
        mexc_perpetual_api_key="key", mexc_perpetual_api_secret="secret",
        trading_pairs=[HB_PAIR], trading_required=trading_required, ccxt_client=client)


def stream(*items: Any) -> Callable[..., Any]:
    """
    Async side effect for a ``watch_*`` method: returns (or raises) each item in turn, then blocks like a real
    websocket waiting for the next message.
    """
    pending: List[Any] = list(items)

    async def watch(*args, **kwargs):
        if not pending:
            await asyncio.Event().wait()
        item = pending.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item
    return watch


def blocked() -> Callable[..., Any]:
    return stream()


def market_with(overrides: Dict[str, Any]) -> Dict[str, Any]:
    return {**MARKET, **overrides}
