import re
from typing import Any, Dict, List, Pattern, Tuple

from hummingbot.core.api_throttler.data_types import RateLimit

EXCHANGE_NAME = "mexc_perpetual"
CCXT_EXCHANGE_ID = "mexc"

# MEXC contract (futures) API rate limits, as documented (mexcdevelop.github.io/apidocs/contract_v1_en): every endpoint
# has its own budget. Nearly all are 20 requests per 2 seconds, the exceptions are listed below.
DEFAULT_LIMIT = 20
DEFAULT_INTERVAL = 2
SPECIAL_LIMITS: Dict[str, Tuple[int, float]] = {
    "detail": (1, 5),               # all contracts, used once to load the markets
    "order/submit_batch": (1, 2),
}
# endpoint templates of the contract API, as named in the documentation ({x} is a part of the path)
ENDPOINT_TEMPLATES: List[str] = [
    "ping", "detail", "support_currencies", "depth/{symbol}", "depth_commits/{symbol}/{limit}",
    "index_price/{symbol}", "fair_price/{symbol}", "funding_rate/{symbol}", "kline/{symbol}",
    "kline/index_price/{symbol}", "kline/fair_price/{symbol}", "deals/{symbol}", "ticker", "risk_reverse",
    "risk_reverse/history", "funding_rate/history",
    "account/assets", "account/asset/{currency}", "account/transfer_record", "account/risk_limit",
    "account/tiered_fee_rate", "position/list/history_positions", "position/open_positions",
    "position/funding_records", "position/change_margin", "position/leverage", "position/change_leverage",
    "position/position_mode", "position/change_position_mode",
    "order/list/open_orders/{symbol}", "order/list/history_orders", "order/external/{symbol}/{external_oid}",
    "order/get/{order_id}", "order/deal_details/{order_id}", "order/list/order_deals", "order/submit",
    "order/submit_batch", "order/cancel", "order/cancel_with_external", "order/cancel_all",
    "planorder/list/orders", "planorder/place", "planorder/cancel", "planorder/cancel_all",
    "stoporder/list/orders", "stoporder/cancel", "stoporder/cancel_all", "stoporder/change_price",
    "stoporder/change_plan_price",
]
DEFAULT_LIMIT_ID = "other"  # an endpoint the documentation does not list shares one budget of the default size

RATE_LIMITS = [RateLimit(limit_id=template, limit=SPECIAL_LIMITS.get(template, (DEFAULT_LIMIT, DEFAULT_INTERVAL))[0],
                         time_interval=SPECIAL_LIMITS.get(template, (DEFAULT_LIMIT, DEFAULT_INTERVAL))[1])
               for template in ENDPOINT_TEMPLATES]
RATE_LIMITS.append(RateLimit(limit_id=DEFAULT_LIMIT_ID, limit=DEFAULT_LIMIT, time_interval=DEFAULT_INTERVAL))

# literal endpoints are tried before the ones with a part of the path, so funding_rate/history is not funding_rate/{symbol}
_PATTERNS: List[Tuple[Pattern, str]] = [
    (re.compile("^" + re.sub(r"\{[^}]+\}", "[^/]+", template) + "$"), template)
    for template in sorted(ENDPOINT_TEMPLATES, key=lambda template: ("{" in template, -len(template)))
]


def limit_id_for_request(path: str, api: Any, method: str, params: Dict[str, Any]) -> str:
    """Id of the rate limit an HTTP request of ccxt is charged to: the endpoint template it belongs to."""
    path = path.strip("/")
    return next((template for pattern, template in _PATTERNS if pattern.match(path)), DEFAULT_LIMIT_ID)


# MEXC "openType": 1 isolated, 2 cross. The connector only trades cross margin for now.
OPEN_TYPE_CROSS = 2
# MEXC "positionType": 1 long, 2 short. Leverage is set per position side.
POSITION_TYPE_LONG = 1
POSITION_TYPE_SHORT = 2

MAX_CLIENT_ORDER_ID_LENGTH = 32  # MEXC "externalOid"
