import re
from typing import Any, Dict, List

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

EXCHANGE_NAME = "aster_perpetual"
CCXT_EXCHANGE_ID = "aster"

POSITION_SIDE_LONG = "LONG"
POSITION_SIDE_SHORT = "SHORT"

# window used to find the funding rate that belongs to a funding payment
FUNDING_RATE_LOOKUP_WINDOW_MS = 60_000

# Aster API V3 (V1 is discontinued): EIP-712 chain id of the signature
SIGNATURE_CHAIN_ID = 1666

# the few V1 paths of ccxt that are not the same path with another version number in V3
V3_PATH_OVERRIDES = {
    "v1/leverageBracket": "v3/leverageBrackets",
}
_VERSIONED_PATH = re.compile(r"^v\d+/")


def v3_path(path: str) -> str:
    """The Aster API V3 path of a path of ccxt, which still names the V1, V2 and V4 ones."""
    return V3_PATH_OVERRIDES.get(path) or _VERSIONED_PATH.sub("v3/", path, count=1)


# ---- rate limits ------------------------------------------------------------------------------------------------
# As documented by Aster and returned by GET /fapi/v3/exchangeInfo (rateLimits): 2400 request weight per minute per IP,
# 1200 orders per minute and 300 orders per 10 seconds per account. Every endpoint has a documented weight; a new order
# weighs 0 and only counts in the order limits.
REQUEST_WEIGHT_LIMIT_ID = "REQUEST_WEIGHT"
ORDERS_1M_LIMIT_ID = "ORDERS_1M"
ORDERS_10S_LIMIT_ID = "ORDERS_10S"
DEFAULT_LIMIT_ID = "DEFAULT"
DEFAULT_WEIGHT = 5  # charged to an endpoint this table does not know, as much as the heaviest regular reads

NEW_ORDER_LIMIT_ID = "POST v3/order"
_OPEN_ORDERS = "GET v3/openOrders"
_TICKER_24H = "GET v3/ticker/24hr"
_DEPTH = "GET v3/depth"
_ALL_SYMBOLS_WEIGHT = 40
_DEPTH_WEIGHTS = ((50, 2), (100, 5), (500, 10), (1000, 20))  # (limit up to, weight)
DEFAULT_DEPTH_LIMIT = 500

ENDPOINT_WEIGHTS: Dict[str, int] = {
    "GET v3/ping": 1, "GET v3/time": 1, "GET v3/exchangeInfo": 1, "GET v3/premiumIndex": 1,
    "GET v3/fundingRate": 1, "GET v3/fundingInfo": 1,
    _TICKER_24H: 1, _OPEN_ORDERS: 1, _DEPTH: 10,
    "GET v3/order": 1, "DELETE v3/order": 1, "GET v3/openOrder": 1, "GET v3/allOrders": 5,
    "DELETE v3/allOpenOrders": 1, "DELETE v3/batchOrders": 1,
    "GET v3/account": 5, "GET v3/balance": 5, "GET v3/positionRisk": 5, "GET v3/userTrades": 5,
    "GET v3/income": 30,
    "GET v3/positionSide/dual": 30, "POST v3/positionSide/dual": 1,
    "POST v3/leverage": 1, "POST v3/marginType": 1, "GET v3/leverageBrackets": 1,
    "GET v3/commissionRate": 20,
    "POST v3/listenKey": 1, "PUT v3/listenKey": 1, "DELETE v3/listenKey": 1,
}
# endpoints whose weight depends on a parameter get one limit per weight
_DEPTH_IDS = {weight: f"{_DEPTH}#{weight}" for _, weight in _DEPTH_WEIGHTS}
_ALL_SYMBOLS_IDS = {_OPEN_ORDERS: f"{_OPEN_ORDERS}#all", _TICKER_24H: f"{_TICKER_24H}#all"}


def _weighted(limit_id: str, weight: int) -> RateLimit:
    return RateLimit(limit_id=limit_id, limit=2400, time_interval=60,
                     linked_limits=[LinkedLimitWeightPair(REQUEST_WEIGHT_LIMIT_ID, weight)])


def _build_rate_limits() -> List[RateLimit]:
    limits = [
        RateLimit(limit_id=REQUEST_WEIGHT_LIMIT_ID, limit=2400, time_interval=60),
        RateLimit(limit_id=ORDERS_1M_LIMIT_ID, limit=1200, time_interval=60),
        RateLimit(limit_id=ORDERS_10S_LIMIT_ID, limit=300, time_interval=10),
        _weighted(DEFAULT_LIMIT_ID, DEFAULT_WEIGHT),
        RateLimit(limit_id=NEW_ORDER_LIMIT_ID, limit=2400, time_interval=60,
                  linked_limits=[LinkedLimitWeightPair(ORDERS_1M_LIMIT_ID, 1),
                                 LinkedLimitWeightPair(ORDERS_10S_LIMIT_ID, 1)]),
    ]
    limits += [_weighted(limit_id, weight) for limit_id, weight in ENDPOINT_WEIGHTS.items()]
    limits += [_weighted(limit_id, weight) for weight, limit_id in _DEPTH_IDS.items()]
    limits += [_weighted(limit_id, _ALL_SYMBOLS_WEIGHT) for limit_id in _ALL_SYMBOLS_IDS.values()]
    return limits


RATE_LIMITS = _build_rate_limits()


def limit_id_for_request(path: str, api: Any, method: str, params: Dict[str, Any]) -> str:
    """Id of the rate limit an HTTP request of ccxt (still written for the V1 paths) is charged to."""
    if api in ("sapiPublic", "sapiPrivate"):
        return DEFAULT_LIMIT_ID  # spot, never used by a perpetual connector
    key = f"{method} {v3_path(path)}"
    if key == NEW_ORDER_LIMIT_ID:
        return NEW_ORDER_LIMIT_ID
    if key == _DEPTH:
        limit = int(params.get("limit") or DEFAULT_DEPTH_LIMIT)
        return _DEPTH_IDS[next((weight for up_to, weight in _DEPTH_WEIGHTS if limit <= up_to), _DEPTH_WEIGHTS[-1][1])]
    if key in _ALL_SYMBOLS_IDS and not params.get("symbol"):
        return _ALL_SYMBOLS_IDS[key]
    return key if key in ENDPOINT_WEIGHTS else DEFAULT_LIMIT_ID
