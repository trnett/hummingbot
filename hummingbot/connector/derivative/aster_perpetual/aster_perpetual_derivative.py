from decimal import Decimal
from typing import Any, Dict, List, Optional

from hummingbot.connector.ccxt_adapter import ccxt_utils
from hummingbot.connector.ccxt_adapter.ccxt_perpetual_derivative import CcxtPerpetualDerivative
from hummingbot.connector.derivative.aster_perpetual import aster_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_client import AsterV3Exchange
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType


class AsterPerpetualDerivative(CcxtPerpetualDerivative):
    """
    Aster perpetual futures connector on Aster API V3 (V1 is discontinued). All logic is in ``CcxtPerpetualDerivative``,
    this class holds the Aster quirks. Credentials are an API wallet: the master wallet address (``user``), the address
    of the API wallet (``signer``) and the private key of the API wallet, created on https://www.asterdex.com/en/api-wallet
    (Pro API). Requests are signed by ``AsterV3Exchange``.
    """

    # ccxt.pro 4.5.32 cannot stream the Aster order book: watch_order_book_for_symbols concatenates a str and an int
    # (``'@depth' + limit``) and raises TypeError. The REST order book is polled instead.
    USE_ORDER_BOOK_STREAM = False

    def __init__(self,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 aster_perpetual_user: Optional[str] = None,
                 aster_perpetual_signer: Optional[str] = None,
                 aster_perpetual_private_key: Optional[str] = None,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 ccxt_client: Optional[Any] = None):
        self._user = aster_perpetual_user
        self._signer = aster_perpetual_signer
        self._private_key = aster_perpetual_private_key
        super().__init__(
            api_key=aster_perpetual_signer,
            api_secret=aster_perpetual_private_key,
            trading_pairs=trading_pairs,
            trading_required=trading_required,
            ccxt_client=ccxt_client,
            balance_asset_limit=balance_asset_limit,
            rate_limits_share_pct=rate_limits_share_pct,
        )

    @property
    def name(self) -> str:
        return CONSTANTS.EXCHANGE_NAME

    @property
    def ccxt_exchange_id(self) -> str:
        return CONSTANTS.CCXT_EXCHANGE_ID

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return CONSTANTS.RATE_LIMITS

    def _ccxt_config(self) -> Dict[str, Any]:
        config = super()._ccxt_config()
        # V3 signs with the private key of the API wallet, there is no api key and no HMAC secret
        config.pop("apiKey")
        config.pop("secret")
        config.update({"walletAddress": self._user, "privateKey": self._private_key})
        config["options"] = {**config["options"], "signerAddress": self._signer}
        return config

    def _ccxt_exchange_class(self) -> type:
        return AsterV3Exchange

    def _limit_id_for_request(self, path: str, api: Any, method: str, params: Dict[str, Any]) -> str:
        return CONSTANTS.limit_id_for_request(path, api, method, params)

    def _build_ccxt_client(self) -> Any:
        client = super()._build_ccxt_client()

        async def no_spot_markets(params={}):
            return {"symbols": []}

        # ccxt's fetch_markets also downloads the spot markets, which are never traded here
        client.sapiPublicGetV1ExchangeInfo = no_spot_markets
        # the currency metadata would download the exchange info a second time and is not used
        client.has["fetchCurrencies"] = False
        return client

    def _finalize_order_params(self, params: Dict[str, Any], trade_type: TradeType, order_type: OrderType,
                               position_action: PositionAction) -> Dict[str, Any]:
        closing = position_action == PositionAction.CLOSE
        params.pop("reduceOnly", None)
        if self.position_mode == PositionMode.HEDGE:
            # hedge mode names the side of the position (and rejects reduceOnly): buying opens a long and sells
            # close it, selling opens a short and buying closes it
            is_long = (trade_type == TradeType.BUY) != closing
            params["positionSide"] = CONSTANTS.POSITION_SIDE_LONG if is_long else CONSTANTS.POSITION_SIDE_SHORT
        elif closing:
            params["reduceOnly"] = "true"  # ccxt expects the string for swaps
        return params

    async def _funding_rate_for_payment(self, symbol: str, entry: Dict[str, Any]) -> Decimal:
        # the funding payment (income) history has no rate, the funding rate history has the one of that settlement
        timestamp = entry["timestamp"]
        rates = await self.ccxt_call("fetch_funding_rate_history", symbol,
                                     timestamp - CONSTANTS.FUNDING_RATE_LOOKUP_WINDOW_MS, 5)
        if not rates:
            return Decimal("-1")
        nearest = min(rates, key=lambda rate: abs((rate.get("timestamp") or 0) - timestamp))
        return ccxt_utils.to_decimal(nearest.get("fundingRate"), Decimal("-1"))
