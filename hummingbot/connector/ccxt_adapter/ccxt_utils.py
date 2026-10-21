"""
Helpers shared by every connector built on top of the generic ccxt adapter.

This module must stay free of any ``ccxt`` import so the connector settings scanner can load the ``*_utils``
modules of ccxt based connectors even when the optional ``ccxt`` dependency is not installed.
"""
import asyncio
import re
from decimal import Decimal
from typing import Any, Dict, Optional, Set

from hummingbot.connector.utils import combine_to_hb_trading_pair, split_hb_trading_pair

# Id of the single throttler bucket every ccxt REST call goes through.
CCXT_RATE_LIMIT_ID = "ccxt_rest"


def trading_pair_to_ccxt_symbol(trading_pair: str) -> str:
    """Hummingbot ``BTC-USDT`` to the ccxt unified symbol of a linear swap, ``BTC/USDT:USDT``."""
    base, quote = split_hb_trading_pair(trading_pair)
    return f"{base}/{quote}:{quote}"


def ccxt_symbol_to_trading_pair(symbol: str) -> str:
    """ccxt unified symbol ``BTC/USDT:USDT`` to the Hummingbot trading pair ``BTC-USDT``."""
    pair = symbol.split(":")[0]
    base, quote = pair.split("/")
    return combine_to_hb_trading_pair(base=base, quote=quote)


def is_exchange_information_valid(market: Dict[str, Any]) -> bool:
    """True for an active linear perpetual swap market, in ccxt unified market format."""
    if not market.get("swap") or market.get("type") != "swap":
        return False
    if not market.get("linear") or market.get("active") is False:
        return False
    quote, settle = market.get("quote"), market.get("settle")
    return quote is None or settle is None or quote == settle


def clean_decimal(value: Decimal) -> Decimal:
    """Strips the trailing zeros of a Decimal without switching to scientific notation (``100000.0`` -> ``100000``)."""
    return Decimal(format(value.normalize(), "f"))


def to_decimal(value: Any, default: Optional[Decimal] = None) -> Optional[Decimal]:
    """Converts ccxt floats and strings to Decimal through ``str`` to avoid binary float artefacts."""
    if value is None:
        return default
    return clean_decimal(Decimal(str(value)))


def contract_size(market: Optional[Dict[str, Any]]) -> Decimal:
    """Size of one contract in base units (1 when the market does not define it)."""
    if not market:
        return Decimal("1")
    return to_decimal(market.get("contractSize"), Decimal("1")) or Decimal("1")


_SIGNATURE = re.compile(r"(signature=)(?:0x)?[0-9a-fA-F]+", re.IGNORECASE)


def sanitize_error_message(message: str) -> str:
    """Removes the signature of a signed request from an error text, so it is never logged or shown to the user."""
    return _SIGNATURE.sub(r"\1***", message)


_closing_tasks: Set[asyncio.Task] = set()


def release_ccxt_client(client: Any) -> None:
    """
    Closes the HTTP session of a ccxt client whose connector is being dropped. Hummingbot's connect command builds a
    connector to test the keys and throws it away without stopping it, and ccxt warns loudly about an open session.
    """
    if getattr(client, "session", None) is None:
        return  # never used, or already closed by stop_network
    try:
        task = asyncio.get_running_loop().create_task(client.close())
    except RuntimeError:
        client.session = None  # no loop left to close it with, the connections go with the object
        return
    _closing_tasks.add(task)
    task.add_done_callback(_closing_tasks.discard)
