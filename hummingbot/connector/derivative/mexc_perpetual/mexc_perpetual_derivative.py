from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import ccxt

from hummingbot.connector.ccxt_adapter.ccxt_perpetual_derivative import CcxtPerpetualDerivative
from hummingbot.connector.derivative.mexc_perpetual import mexc_perpetual_constants as CONSTANTS
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType


class MexcPerpetualDerivative(CcxtPerpetualDerivative):
    """MEXC perpetual futures connector. All logic is in ``CcxtPerpetualDerivative``, this class holds the MEXC quirks."""

    MAX_CLIENT_ORDER_ID_LENGTH = CONSTANTS.MAX_CLIENT_ORDER_ID_LENGTH

    def __init__(self,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100"),
                 mexc_perpetual_api_key: Optional[str] = None,
                 mexc_perpetual_api_secret: Optional[str] = None,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 ccxt_client: Optional[Any] = None):
        super().__init__(
            api_key=mexc_perpetual_api_key,
            api_secret=mexc_perpetual_api_secret,
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

    def _limit_id_for_request(self, path: str, api: Any, method: str, params: Dict[str, Any]) -> str:
        return CONSTANTS.limit_id_for_request(path, api, method, params)

    def _leverage_params(self, trading_pair: str) -> List[Dict[str, Any]]:
        # MEXC sets leverage per margin type and position side, so both sides are updated together.
        return [
            {"openType": CONSTANTS.OPEN_TYPE_CROSS, "positionType": CONSTANTS.POSITION_TYPE_LONG},
            {"openType": CONSTANTS.OPEN_TYPE_CROSS, "positionType": CONSTANTS.POSITION_TYPE_SHORT},
        ]

    def _funding_rate_from_history_entry(self, entry: Dict[str, Any]) -> Decimal:
        rate = (entry.get("info") or {}).get("rate")
        return Decimal(str(rate)) if rate is not None else Decimal("-1")

    async def _load_ccxt_markets(self) -> Dict[str, Any]:
        # ccxt's load_markets also downloads the spot markets and the (private) currencies, which are never used
        swap_markets = await self.ccxt_call("fetch_swap_markets")
        return self._ccxt_client.set_markets(swap_markets)

    def funding_prices_from_ticker(self, ticker: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
        info = ticker.get("info") or {}
        return info.get("indexPrice"), info.get("fairPrice")

    @staticmethod
    def _is_order_not_found(exception: Exception) -> bool:
        # MEXC answers error 2040 "order not exist", which ccxt raises as a generic InvalidOrder or ExchangeError
        return isinstance(exception, ccxt.OrderNotFound) or "order not exist" in str(exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return self._is_order_not_found(cancelation_exception)

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return self._is_order_not_found(status_update_exception)

    def _parse_balances(self, balance: Dict[str, Any]) -> Tuple[Dict[str, Decimal], Dict[str, Decimal]]:
        """
        ccxt's total of a MEXC asset leaves the position margin out. The wallet balance is what is available plus
        what is frozen plus what is held as position margin, from the raw account (REST) or asset push (stream).
        """
        info = balance.get("info")
        raw_assets = None
        if isinstance(info, dict):
            if isinstance(info.get("data"), list):
                raw_assets = info["data"]
            elif "currency" in info:
                raw_assets = [info]
        if raw_assets is None:
            return super()._parse_balances(balance)
        total, available = {}, {}
        for asset in raw_assets:
            available[asset["currency"]] = Decimal(str(asset.get("availableBalance", 0)))
            total[asset["currency"]] = (available[asset["currency"]] + Decimal(str(asset.get("frozenBalance", 0)))
                                        + Decimal(str(asset.get("positionMargin", 0))))
        return total, available

    async def _fetch_order_by_client_id(self, symbol: str, client_order_id: str) -> Dict[str, Any]:
        # ccxt's fetch_order has no external order id lookup for MEXC contracts, its implicit API has
        market = self.ccxt_market(symbol)
        response = await self.ccxt_call("contractPrivateGetOrderExternalSymbolExternalOid",
                                        {"symbol": market["id"], "external_oid": client_order_id})
        if not response.get("data"):
            raise ccxt.OrderNotFound(f"mexc order {client_order_id} not exist")
        return self._ccxt_client.parse_order(response["data"], market)

    def _exchange_unrealized_pnl(self, position: Dict[str, Any]) -> Optional[Decimal]:
        pnl = super()._exchange_unrealized_pnl(position)
        if pnl is not None:
            return pnl
        info = position.get("info") or {}
        raw = info.get("unRealizedPnl", info.get("unrealizedPnl"))
        return Decimal(str(raw)) if raw is not None else None

    def _finalize_order_params(self, params: Dict[str, Any], trade_type: TradeType, order_type: OrderType,
                               position_action: PositionAction) -> Dict[str, Any]:
        # MEXC's order side already says whether the order opens or closes a position, whatever the position mode:
        # 1 open long, 2 close short, 3 open short, 4 close long. It is sent as is, ccxt's reduceOnly is not needed.
        closing = position_action == PositionAction.CLOSE
        params.pop("reduceOnly", None)
        if self.position_mode == PositionMode.HEDGE:
            params["hedged"] = True
        params["side"] = (2 if closing else 1) if trade_type == TradeType.BUY else (4 if closing else 3)
        return params
