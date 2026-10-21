import asyncio
import inspect
import re
import time
import weakref
from abc import abstractmethod
from decimal import Decimal
from typing import Any, Dict, List, Optional, Set, Tuple

import ccxt
from bidict import bidict

from hummingbot.connector.ccxt_adapter import ccxt_utils
from hummingbot.connector.ccxt_adapter.ccxt_order_book_data_source import CcxtPerpetualAPIOrderBookDataSource
from hummingbot.connector.ccxt_adapter.ccxt_throttle import throttled
from hummingbot.connector.ccxt_adapter.ccxt_user_stream_data_source import (
    CHANNEL_BALANCE,
    CHANNEL_ORDERS,
    CHANNEL_POSITIONS,
    CHANNEL_TRADES,
    CcxtPerpetualUserStreamDataSource,
)
from hummingbot.connector.ccxt_adapter.ccxt_utils import CCXT_RATE_LIMIT_ID
from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.derivative.position import Position
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.api_throttler.data_types import RateLimit
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.estimate_fee import build_trade_fee
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class CcxtPerpetualDerivative(PerpetualDerivativePyBase):
    """
    Generic Hummingbot perpetual connector on top of ccxt / ccxt.pro.

    All the exchange specific behaviour lives in a small subclass (see ``mexc_perpetual``) which sets the ccxt
    exchange id, the connector name, the rate limits and the few quirks of that exchange. Supporting another
    exchange means adding one such subclass, not rewriting the connector.

    Conventions:
    - Hummingbot works in base units, ccxt swaps in contracts. Amounts are converted with the market contract size.
    - ccxt's own rate limiter is disabled, Hummingbot's ``AsyncThrottler`` is the single rate limiter.
    - Streaming uses ccxt.pro, positions and funding are polled.
    """

    SHORT_POLL_INTERVAL = 5.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    LONG_POLL_INTERVAL = 120.0
    MAX_CLIENT_ORDER_ID_LENGTH = 32
    CLIENT_ORDER_ID_PREFIX = "HBOT"
    FUNDING_FEE_POLL_INTERVAL = 120

    # False for an exchange whose ccxt.pro order book stream is unusable: the REST order book is polled instead
    USE_ORDER_BOOK_STREAM = True

    # Only one-way mode until an exchange has been verified live in hedge mode, a wrong mapping would open the wrong
    # position. A subclass adds PositionMode.HEDGE to enable the native Hummingbot position mode handling.
    SUPPORTED_POSITION_MODES = [PositionMode.ONEWAY]

    # an order whose submission outcome is unknown is looked up by its client order id
    RECONCILE_ATTEMPTS = 3
    RECONCILE_DELAY = 1.0
    UNRESOLVED_ORDER_WATCH_ATTEMPTS = 24

    # recovery of fills when the exchange cannot list the trades of one order
    TRADES_PAGE_SIZE = 100
    TRADES_MAX_PAGES = 10
    TRADES_LOOKBACK_MS = 5_000
    TRADES_UNKNOWN_START_LOOKBACK_MS = 24 * 60 * 60 * 1000

    # Reads that a dropped request must not break (market loading at startup, balances...) are repeated. Writes never
    # are, and the order lookups have their own recovery.
    READ_RETRY_METHODS = frozenset({"load_markets", "fetch_swap_markets", "fetch_time", "fetch_balance",
                                    "fetch_positions", "fetch_funding_rate", "fetch_funding_history", "fetch_ticker",
                                    "fetch_tickers", "fetch_order_book"})
    # ccxt calls that work without markets. Every other call first loads the markets through the connector, otherwise
    # ccxt would load them itself, spot markets and spot currencies included
    MARKET_FREE_METHODS = frozenset({"load_markets", "fetch_swap_markets", "fetch_time"})
    READ_RETRIES = 2
    READ_RETRY_DELAY = 1.0

    web_utils = None  # ccxt owns the HTTP layer, the base class never builds a request

    def __init__(self,
                 api_key: Optional[str] = None,
                 api_secret: Optional[str] = None,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 ccxt_client: Optional[Any] = None,
                 balance_asset_limit: Optional[Dict[str, Dict[str, Decimal]]] = None,
                 rate_limits_share_pct: Decimal = Decimal("100")):
        self._api_key = api_key
        self._api_secret = api_secret
        self._trading_pairs = trading_pairs or []
        self._trading_required = trading_required
        self._ccxt_markets: Dict[str, Dict[str, Any]] = {}
        self._ccxt_client = ccxt_client or self._build_ccxt_client()
        self._markets_loaded = False
        self._markets_lock = asyncio.Lock()
        self._unresolved_order_tasks: Set[asyncio.Task] = set()
        self._pnl_gaps_logged: Set[str] = set()
        super().__init__(balance_asset_limit, rate_limits_share_pct)
        # signed requests are timestamped with Hummingbot's synchronized time, as the native connectors' auth does.
        # The client must not keep the connector alive, so the session can be closed when the connector is dropped.
        connector_reference = weakref.ref(self)

        def synchronized_milliseconds() -> int:
            connector = connector_reference()
            return int((connector._time_synchronizer.time() if connector else time.time()) * 1e3)

        self._ccxt_client.milliseconds = synchronized_milliseconds
        # every HTTP request of ccxt is rate limited by Hummingbot's throttler, with the limits of the exchange

        def limit_id_for_request(path: str, api: Any, method: str, params: Dict[str, Any]) -> str:
            connector = connector_reference()
            return connector._limit_id_for_request(path, api, method, params) if connector else CCXT_RATE_LIMIT_ID

        self._ccxt_client.hb_throttler = self._throttler
        self._ccxt_client.hb_limit_id_for = limit_id_for_request
        weakref.finalize(self, ccxt_utils.release_ccxt_client, self._ccxt_client)
        # without a balance stream the available balance is bridged between polls, as the poll-only native connectors do
        self.real_time_balance_update = bool(self._ccxt_client.has.get("watchBalance"))

    # ---- exchange specific hooks --------------------------------------------------------------------------------
    @property
    @abstractmethod
    def ccxt_exchange_id(self) -> str:
        """The ccxt exchange id, e.g. ``mexc``."""
        raise NotImplementedError

    def _ccxt_config(self) -> Dict[str, Any]:
        return {
            "apiKey": self._api_key,
            "secret": self._api_secret,
            "enableRateLimit": False,  # Hummingbot's AsyncThrottler is the single rate limiter
            "options": {"defaultType": "swap"},
        }

    def _finalize_order_params(self, params: Dict[str, Any], trade_type: TradeType, order_type: OrderType,
                               position_action: PositionAction) -> Dict[str, Any]:
        """Last chance for an exchange to adapt the ccxt ``params`` of ``create_order``."""
        return params

    def _leverage_params(self, trading_pair: str) -> List[Dict[str, Any]]:
        """One ccxt ``params`` dictionary per ``set_leverage`` call needed to set the leverage of a pair."""
        return [{}]

    def _funding_rate_from_history_entry(self, entry: Dict[str, Any]) -> Decimal:
        """Funding rate of a ccxt funding history entry, ``-1`` when the exchange does not provide it."""
        return Decimal("-1")

    async def _funding_rate_for_payment(self, symbol: str, entry: Dict[str, Any]) -> Decimal:
        """Funding rate of a funding payment. Exchanges whose history lacks it can query it here."""
        return self._funding_rate_from_history_entry(entry)

    def funding_prices_from_ticker(self, ticker: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
        """``(index_price, mark_price)`` from a ccxt ticker, for exchanges whose funding rate omits them."""
        return ticker.get("indexPrice"), ticker.get("markPrice")

    async def _fetch_order_by_client_id(self, symbol: str, client_order_id: str) -> Dict[str, Any]:
        """The ccxt order with this client order id. Raises when the exchange has no such order."""
        return await self.ccxt_call("fetch_order", None, symbol, {"clientOrderId": client_order_id})

    def _order_trades_params(self, exchange_order_id: str) -> Dict[str, Any]:
        """Extra ccxt ``params`` of ``fetch_my_trades`` when the exchange can narrow the trades to one order."""
        return {}

    def _exchange_unrealized_pnl(self, position: Dict[str, Any]) -> Optional[Decimal]:
        """Unrealized PnL of a ccxt position as the exchange reports it, ``None`` when it does not."""
        return ccxt_utils.to_decimal(position.get("unrealizedPnl"))

    # ---- plumbing -----------------------------------------------------------------------------------------------
    def _ccxt_exchange_class(self) -> type:
        """The ccxt class of the exchange, an exchange that ccxt does not fully support can derive its own."""
        import ccxt.pro as ccxt_pro
        return getattr(ccxt_pro, self.ccxt_exchange_id)

    def _build_ccxt_client(self) -> Any:
        return throttled(self._ccxt_exchange_class())(self._ccxt_config())

    def _limit_id_for_request(self, path: str, api: Any, method: str, params: Dict[str, Any]) -> str:
        """Id of the ``rate_limits_rules`` limit that an HTTP request of ccxt is charged to."""
        return CCXT_RATE_LIMIT_ID

    @property
    def ccxt_client(self) -> Any:
        return self._ccxt_client

    def ccxt_market(self, symbol: str) -> Optional[Dict[str, Any]]:
        return self._ccxt_markets.get(symbol)

    async def ccxt_call(self, method: str, *args, **kwargs) -> Any:
        """
        Calls a ccxt REST method (each of its HTTP requests goes through Hummingbot's throttler). As in the native connectors, a request rejected
        because of the timestamp resynchronizes the time and is repeated once. A read that a dropped request broke is
        repeated a couple of times. Signatures are removed from errors.
        """
        if method not in self.MARKET_FREE_METHODS:
            await self._ensure_markets_loaded()
        timestamp_retries = 1
        read_retries = self.READ_RETRIES if method in self.READ_RETRY_METHODS else 0
        while True:
            try:
                return await getattr(self._ccxt_client, method)(*args, **kwargs)  # throttled per HTTP request
            except ccxt.BaseError as error:
                if timestamp_retries > 0 and self._is_request_exception_related_to_time_synchronizer(error):
                    timestamp_retries -= 1
                    await self._update_time_synchronizer()
                elif read_retries > 0 and isinstance(error, ccxt.NetworkError) \
                        and self._is_submission_outcome_unknown(error):
                    read_retries -= 1
                    await self._sleep(self.READ_RETRY_DELAY)
                else:
                    raise self._without_signature(error) from None

    async def _ensure_markets_loaded(self):
        if self._markets_loaded:
            return
        async with self._markets_lock:
            if not self._markets_loaded:
                self._initialize_trading_pair_symbols_from_exchange_info(await self._load_ccxt_markets())

    @staticmethod
    def _without_signature(error: ccxt.BaseError) -> ccxt.BaseError:
        cleaned = ccxt_utils.sanitize_error_message(str(error))
        return error if cleaned == str(error) else type(error)(cleaned)

    @property
    def authenticator(self) -> Optional[AuthBase]:
        return None  # ccxt signs its own requests

    @property
    def rate_limits_rules(self) -> List[RateLimit]:
        return [RateLimit(limit_id=CCXT_RATE_LIMIT_ID, limit=20, time_interval=2)]

    @property
    def domain(self) -> str:
        return ""

    @property
    def client_order_id_max_length(self) -> int:
        return self.MAX_CLIENT_ORDER_ID_LENGTH

    @property
    def client_order_id_prefix(self) -> str:
        return self.CLIENT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self) -> str:
        return ""

    @property
    def trading_pairs_request_path(self) -> str:
        return ""

    @property
    def check_network_request_path(self) -> str:
        return ""

    @property
    def trading_pairs(self) -> List[str]:
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    @property
    def funding_fee_poll_interval(self) -> int:
        return self.FUNDING_FEE_POLL_INTERVAL

    def supported_order_types(self) -> List[OrderType]:
        return [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]

    def supported_position_modes(self) -> List[PositionMode]:
        return list(self.SUPPORTED_POSITION_MODES)

    def get_buy_collateral_token(self, trading_pair: str) -> str:
        rule = self._trading_rules.get(trading_pair)
        return rule.buy_order_collateral_token if rule else trading_pair.split("-")[1]

    def get_sell_collateral_token(self, trading_pair: str) -> str:
        rule = self._trading_rules.get(trading_pair)
        return rule.sell_order_collateral_token if rule else trading_pair.split("-")[1]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception) -> bool:
        return isinstance(request_exception, ccxt.InvalidNonce)

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return isinstance(status_update_exception, ccxt.OrderNotFound)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return isinstance(cancelation_exception, ccxt.OrderNotFound)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return WebAssistantsFactory(throttler=self._throttler)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return CcxtPerpetualAPIOrderBookDataSource(trading_pairs=self._trading_pairs, connector=self)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CcxtPerpetualUserStreamDataSource(connector=self)

    async def _make_network_check_request(self):
        await self.ccxt_call("fetch_time")

    async def _update_time_synchronizer(self, pass_on_non_cancelled_error: bool = False):
        await self._time_synchronizer.update_server_time_offset_with_time_provider(self._server_time_ms())

    async def _server_time_ms(self) -> float:
        return float(await self.ccxt_call("fetch_time"))

    def _get_poll_interval(self, timestamp: float) -> float:
        if not self._user_stream_tracker.data_source.streams_private_data:
            return self.SHORT_POLL_INTERVAL  # no private stream to rely on, poll often
        return super()._get_poll_interval(timestamp)

    async def start_network(self):
        await super().start_network()
        if self.is_trading_required:
            await self._check_account_position_mode()

    async def _check_account_position_mode(self):
        """
        Hummingbot syncs the position mode only for connectors with several modes. A connector with one mode would
        silently trade against an account in another mode, so the account is read and a mismatch is reported.
        """
        if len(self.supported_position_modes()) > 1:
            return
        try:
            response = await self.ccxt_call("fetch_position_mode")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().warning(f"Could not read the position mode of the {self.name_cap} account. "
                                  f"Make sure it is in one-way mode.", exc_info=True)
            return
        if response.get("hedged"):
            self.logger().error(f"The {self.name_cap} account is in hedge mode but this connector supports only "
                                f"one-way mode. Switch the account to one-way mode, orders may be rejected or "
                                f"open the wrong position.")

    async def stop_network(self):
        watchers = list(self._unresolved_order_tasks)
        for task in watchers:
            task.cancel()
        await asyncio.gather(*watchers, return_exceptions=True)
        self._unresolved_order_tasks.clear()
        await super().stop_network()
        closing = self._ccxt_client.close()
        if inspect.isawaitable(closing):
            await closing

    # ---- markets and trading rules ------------------------------------------------------------------------------
    async def _load_ccxt_markets(self) -> Dict[str, Any]:
        """Loads the ccxt markets of the exchange. Subclasses can override it to skip the markets they do not trade."""
        return await self.ccxt_call("load_markets", True)

    async def _make_trading_rules_request(self) -> Any:
        return await self._load_ccxt_markets()

    async def _make_trading_pairs_request(self) -> Any:
        return await self._load_ccxt_markets()

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        mapping = bidict()
        self._ccxt_markets = {}
        for symbol, market in exchange_info.items():
            if ccxt_utils.is_exchange_information_valid(market):
                mapping[symbol] = ccxt_utils.ccxt_symbol_to_trading_pair(symbol)
                self._ccxt_markets[symbol] = market
        self._set_trading_pair_symbol_map(mapping)
        self._markets_loaded = True

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        rules = []
        for symbol, market in exchange_info_dict.items():
            if not ccxt_utils.is_exchange_information_valid(market):
                continue
            try:
                size = ccxt_utils.contract_size(market)
                precision, limits = market["precision"], market["limits"]
                amount_limits, cost_limits = limits["amount"], limits.get("cost") or {}
                settle = market.get("settle") or market.get("quote")
                max_amount = ccxt_utils.to_decimal(amount_limits.get("max"))
                rules.append(TradingRule(
                    trading_pair=ccxt_utils.ccxt_symbol_to_trading_pair(symbol),
                    min_order_size=ccxt_utils.clean_decimal(ccxt_utils.to_decimal(amount_limits["min"]) * size),
                    max_order_size=(ccxt_utils.clean_decimal(max_amount * size) if max_amount is not None
                                    else Decimal("1e12")),
                    min_price_increment=self._step(precision["price"]),
                    min_base_amount_increment=ccxt_utils.clean_decimal(self._step(precision["amount"]) * size),
                    min_notional_size=ccxt_utils.to_decimal(cost_limits.get("min"), Decimal("0")),
                    buy_order_collateral_token=settle,
                    sell_order_collateral_token=settle,
                ))
            except Exception:
                self.logger().exception(f"Error parsing the trading rule for {symbol}. Skipping.")
        return rules

    def _step(self, precision: Any) -> Decimal:
        """Converts a ccxt precision value to a step size, whatever the precision mode of the exchange."""
        value = ccxt_utils.to_decimal(precision)
        if value is None:
            raise ValueError("The market does not define its precision")
        if self._ccxt_client.precisionMode == ccxt.DECIMAL_PLACES:
            return Decimal(1).scaleb(-int(value))
        return value

    async def _update_trading_fees(self):
        """Fees are the connector defaults, ccxt exposes no account fee endpoint common to all exchanges."""
        return

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 position_action: PositionAction,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        return build_trade_fee(
            self.name,
            is_maker or False,
            base_currency=base_currency,
            quote_currency=quote_currency,
            order_type=order_type,
            order_side=order_side,
            amount=amount,
            price=price,
        )

    # ---- orders -------------------------------------------------------------------------------------------------
    def contract_size(self, symbol: str) -> Decimal:
        """Size of one contract in base units. Raises when the market is unknown, sizing must never be guessed."""
        market = self.ccxt_market(symbol)
        if market is None:
            raise ValueError(f"Unknown {self.name} market {symbol}, its contract size cannot be determined.")
        return ccxt_utils.contract_size(market)

    def _to_contracts(self, symbol: str, amount: Decimal) -> float:
        return float(amount / self.contract_size(symbol))

    def _to_base(self, symbol: str, contracts: Any) -> Decimal:
        return ccxt_utils.clean_decimal(
            ccxt_utils.to_decimal(contracts, Decimal("0")) * self.contract_size(symbol))

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           position_action: PositionAction = PositionAction.NIL,
                           **kwargs) -> Tuple[str, float]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        params: Dict[str, Any] = {"clientOrderId": order_id}
        if position_action == PositionAction.CLOSE:
            params["reduceOnly"] = True
        if order_type == OrderType.LIMIT_MAKER:
            params["postOnly"] = True
        params = self._finalize_order_params(params, trade_type, order_type, position_action)
        is_market = order_type == OrderType.MARKET
        try:
            order = await self.ccxt_call(
                "create_order",
                symbol,
                "market" if is_market else "limit",
                "buy" if trade_type == TradeType.BUY else "sell",
                self._to_contracts(symbol, amount),
                None if is_market else float(price),
                params,
            )
        except ccxt.NetworkError as error:
            if not self._is_submission_outcome_unknown(error):
                raise
            order = await self._reconcile_unconfirmed_order(order_id, symbol, error)
        timestamp = order.get("timestamp")
        return str(order["id"]), (timestamp / 1e3 if timestamp else self.current_timestamp)

    @staticmethod
    def _is_submission_outcome_unknown(error: ccxt.NetworkError) -> bool:
        """
        A timeout or a lost connection may mean the exchange accepted the order. An HTTP 4xx answer, a rate limit
        and a timestamp error prove it did not.
        """
        if isinstance(error, (ccxt.DDoSProtection, ccxt.InvalidNonce)):
            return False
        return re.search(r"\s4\d\d\s", str(error)) is None

    async def _reconcile_unconfirmed_order(self, order_id: str, symbol: str, error: ccxt.NetworkError) -> Dict[str, Any]:
        """
        Looks the order up by its client order id after a submission whose outcome is unknown. Returns the exchange
        order when it exists, raises the original error when the exchange confirms it does not, and watches the order
        in the background when the exchange could not be asked.
        """
        confirmed_absent = False
        for attempt in range(self.RECONCILE_ATTEMPTS):
            try:
                order = await self._fetch_order_by_client_id(symbol, order_id)
                if order:
                    self.logger().warning(f"The response to the submission of order {order_id} was lost, it was "
                                          f"recovered from the exchange as order {order['id']}.")
                    return order
                confirmed_absent = True
            except asyncio.CancelledError:
                raise
            except Exception as lookup_error:
                confirmed_absent = self._is_order_not_found_during_status_update_error(lookup_error)
            if attempt < self.RECONCILE_ATTEMPTS - 1:
                await self._sleep(self.RECONCILE_DELAY)
        if not confirmed_absent:
            self.logger().error(f"The submission of order {order_id} failed ({error}) and its outcome could not be "
                                f"confirmed: it may exist on the exchange. It is being watched and will be cancelled "
                                f"if it shows up.")
            task = asyncio.ensure_future(self._watch_unresolved_order(order_id, symbol))
            self._unresolved_order_tasks.add(task)
            task.add_done_callback(self._unresolved_order_tasks.discard)
        raise error

    async def _watch_unresolved_order(self, order_id: str, symbol: str):
        """Hummingbot reported the order as failed: if the exchange has it after all, it must not stay open."""
        for _ in range(self.UNRESOLVED_ORDER_WATCH_ATTEMPTS):
            await self._sleep(self.SHORT_POLL_INTERVAL)
            try:
                order = await self._fetch_order_by_client_id(symbol, order_id)
            except asyncio.CancelledError:
                raise
            except Exception as lookup_error:
                if self._is_order_not_found_during_status_update_error(lookup_error):
                    self.logger().info(f"Order {order_id} does not exist on the exchange, nothing to cancel.")
                    return
                continue
            if self._order_state_from_ccxt(order.get("status"), order.get("filled")) in (
                    OrderState.OPEN, OrderState.PARTIALLY_FILLED):
                await self.ccxt_call("cancel_order", str(order["id"]), symbol)
                self.logger().warning(f"Order {order_id} was reported as failed but existed on the exchange as "
                                      f"{order['id']}, it was cancelled.")
            else:
                self.logger().error(f"Order {order_id} was executed although its submission was reported as failed "
                                    f"({order.get('status')}). Check your positions on the exchange.")
            return
        self.logger().error(f"Order {order_id}: gave up looking for it on the exchange. Check your open orders and "
                            f"positions.")

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        exchange_order_id = await tracked_order.get_exchange_order_id()
        await self.ccxt_call("cancel_order", exchange_order_id, symbol)
        return True

    @staticmethod
    def _order_state_from_ccxt(status: str, filled: Any) -> OrderState:
        if status == "open":
            return OrderState.PARTIALLY_FILLED if filled and Decimal(str(filled)) > 0 else OrderState.OPEN
        return {
            "closed": OrderState.FILLED,
            "canceled": OrderState.CANCELED,
            "expired": OrderState.CANCELED,
            "rejected": OrderState.FAILED,
        }.get(status, OrderState.OPEN)

    def _order_update_from_ccxt(self, order: Dict[str, Any], tracked_order: InFlightOrder) -> OrderUpdate:
        timestamp = order.get("lastUpdateTimestamp") or order.get("timestamp")
        return OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(order["id"]),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=timestamp / 1e3 if timestamp else self.current_timestamp,
            new_state=self._order_state_from_ccxt(order.get("status"), order.get("filled")),
        )

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        exchange_order_id = await tracked_order.get_exchange_order_id()
        order = await self.ccxt_call("fetch_order", exchange_order_id, symbol)
        return self._order_update_from_ccxt(order, tracked_order)

    def _trade_update_from_ccxt(self, trade: Dict[str, Any], order: InFlightOrder) -> TradeUpdate:
        symbol = ccxt_utils.trading_pair_to_ccxt_symbol(order.trading_pair)
        base_amount = self._to_base(symbol, trade["amount"])
        price = ccxt_utils.to_decimal(trade["price"])
        fee_info = trade.get("fee") or {}
        fee_token = fee_info.get("currency") or self.get_buy_collateral_token(order.trading_pair)
        # an order without position action (NIL) is an opening order, as in _place_order
        position_action = PositionAction.CLOSE if order.position == PositionAction.CLOSE else PositionAction.OPEN
        fee = TradeFeeBase.new_perpetual_fee(
            fee_schema=self.trade_fee_schema(),
            position_action=position_action,
            percent_token=fee_token,
            flat_fees=[TokenAmount(amount=ccxt_utils.to_decimal(fee_info.get("cost"), Decimal("0")),
                                   token=fee_token)],
        )
        return TradeUpdate(
            trade_id=str(trade["id"]),
            client_order_id=order.client_order_id,
            exchange_order_id=order.exchange_order_id or str(trade.get("order")),
            trading_pair=order.trading_pair,
            fill_timestamp=(trade.get("timestamp") or self.current_timestamp * 1e3) / 1e3,
            fill_price=price,
            fill_base_amount=base_amount,
            fill_quote_amount=base_amount * price,
            fee=fee,
        )

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        exchange_order_id = order.exchange_order_id
        if exchange_order_id is None:
            return []
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        return [self._trade_update_from_ccxt(trade, order)
                for trade in await self._fetch_order_trades(order, symbol, exchange_order_id)]

    def _trades_start_ms(self, order: InFlightOrder) -> int:
        """Where the search for the fills of an order starts: its creation, or a day back when that is unknown."""
        created = order.creation_timestamp
        if created is None or created != created or created <= 0:  # missing, NaN or the -1 of a restored order
            return int(self._time_synchronizer.time() * 1e3) - self.TRADES_UNKNOWN_START_LOOKBACK_MS
        return int(created * 1e3) - self.TRADES_LOOKBACK_MS

    async def _fetch_order_trades(self, order: InFlightOrder, symbol: str, exchange_order_id: str) -> List[Dict]:
        """
        The trades of one order. An exchange that can list them is asked directly. Otherwise the account trades are
        read page by page from the order's creation time, so a fill is not missed because it fell off the first page.
        """
        if self._ccxt_client.has.get("fetchOrderTrades"):
            return await self.ccxt_call("fetch_order_trades", exchange_order_id, symbol)
        trades, seen = [], set()
        since = self._trades_start_ms(order)
        params = self._order_trades_params(exchange_order_id)
        for _ in range(self.TRADES_MAX_PAGES):
            page = await self.ccxt_call("fetch_my_trades", symbol, since, self.TRADES_PAGE_SIZE, params)
            fresh = [trade for trade in page if str(trade["id"]) not in seen]
            seen.update(str(trade["id"]) for trade in fresh)
            trades.extend(trade for trade in fresh if str(trade.get("order")) == exchange_order_id)
            if len(page) < self.TRADES_PAGE_SIZE or not fresh:
                break
            since = max(trade.get("timestamp") or since for trade in page)
        return trades

    # ---- account ------------------------------------------------------------------------------------------------
    async def _update_balances(self):
        balance = await self.ccxt_call("fetch_balance")
        self._apply_balance(balance)

    def _apply_balance(self, balance: Dict[str, Any]):
        """A REST balance is the whole account: assets it does not list are gone."""
        total, available = self._parse_balances(balance)
        self._account_balances = total
        self._account_available_balances = available

    def _merge_balance(self, balance: Dict[str, Any]):
        """A stream balance carries the assets that changed, the others keep their value."""
        total, available = self._parse_balances(balance)
        self._account_balances.update(total)
        self._account_available_balances.update(available)

    def _parse_balances(self, balance: Dict[str, Any]) -> Tuple[Dict[str, Decimal], Dict[str, Decimal]]:
        """``(total, available)`` balances by asset from a ccxt balance structure."""
        total, available = {}, {}
        for asset, entry in balance.items():
            if asset in ("info", "free", "used", "total") or not isinstance(entry, dict) or "total" not in entry:
                continue
            total[asset] = ccxt_utils.to_decimal(entry.get("total"), Decimal("0"))
            available[asset] = ccxt_utils.to_decimal(entry.get("free"), Decimal("0"))
        return total, available

    async def _update_positions(self):
        positions = await self.ccxt_call("fetch_positions")
        seen = set()
        for position in positions:
            key = await self._apply_position(position)
            if key is not None:
                seen.add(key)
        for key in list(self._perpetual_trading.account_positions.keys()):
            if key not in seen:
                self._perpetual_trading.remove_position(key)

    async def _apply_position(self, position: Dict[str, Any]) -> Optional[str]:
        """Stores a ccxt position, returns its key (``None`` for untracked symbols and empty positions)."""
        try:
            trading_pair = await self.trading_pair_associated_to_exchange_symbol(position["symbol"])
        except KeyError:
            return None  # symbol not tracked by the connector
        side = PositionSide.LONG if position.get("side") == "long" else PositionSide.SHORT
        key = self._perpetual_trading.position_key(trading_pair, side)
        contracts = ccxt_utils.to_decimal(position.get("contracts"), Decimal("0"))
        if contracts == 0:
            self._perpetual_trading.remove_position(key)
            return None
        amount = self._to_base(position["symbol"], contracts)
        entry_price = ccxt_utils.to_decimal(position.get("entryPrice"), Decimal("0"))
        self._perpetual_trading.set_position(key, Position(
            trading_pair=trading_pair,
            position_side=side,
            unrealized_pnl=self._unrealized_pnl(trading_pair, side, amount, entry_price, position),
            entry_price=entry_price,
            amount=amount if side == PositionSide.LONG else -amount,
            leverage=ccxt_utils.to_decimal(position.get("leverage"), Decimal("1")),
        ))
        return key

    def _unrealized_pnl(self, trading_pair: str, side: PositionSide, amount: Decimal, entry_price: Decimal,
                        position: Dict[str, Any]) -> Decimal:
        """The exchange's unrealized PnL, else computed from the mark price, else zero (and said so once)."""
        pnl = self._exchange_unrealized_pnl(position)
        if pnl is not None:
            return pnl
        try:
            mark_price = self._perpetual_trading.get_funding_info(trading_pair).mark_price
        except KeyError:
            mark_price = Decimal("0")
        if mark_price > 0:
            difference = mark_price - entry_price if side == PositionSide.LONG else entry_price - mark_price
            return ccxt_utils.clean_decimal(difference * amount)
        if trading_pair not in self._pnl_gaps_logged:
            self._pnl_gaps_logged.add(trading_pair)
            self.logger().warning(f"The unrealized PnL of the {trading_pair} position is unknown (the exchange does "
                                  f"not report it and there is no mark price yet), it is shown as 0.")
        return Decimal("0")

    async def _set_trading_pair_leverage(self, trading_pair: str, leverage: int) -> Tuple[bool, str]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        try:
            for params in self._leverage_params(trading_pair):
                if params:
                    await self.ccxt_call("set_leverage", leverage, symbol, params)
                else:
                    await self.ccxt_call("set_leverage", leverage, symbol)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            return False, str(error)
        return True, ""

    async def _trading_pair_position_mode_set(self, mode: PositionMode, trading_pair: str) -> Tuple[bool, str]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        try:
            await self.ccxt_call("set_position_mode", mode == PositionMode.HEDGE, symbol)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            return False, str(error)
        return True, ""

    async def _fetch_account_position_mode(self) -> Optional[PositionMode]:
        try:
            response = await self.ccxt_call("fetch_position_mode")
            return PositionMode.HEDGE if response.get("hedged") else PositionMode.ONEWAY
        except asyncio.CancelledError:
            raise
        except Exception:
            return None

    async def _fetch_last_fee_payment(self, trading_pair: str) -> Tuple[float, Decimal, Decimal]:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        history = await self.ccxt_call("fetch_funding_history", symbol)
        if not history:
            return 0, Decimal("-1"), Decimal("-1")
        last = max(history, key=lambda entry: entry.get("timestamp") or 0)
        return (
            last["timestamp"] / 1e3,
            await self._funding_rate_for_payment(symbol, last),
            ccxt_utils.to_decimal(last.get("amount"), Decimal("0")),
        )

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        ticker = await self.ccxt_call("fetch_ticker", symbol)
        return float(ticker["last"])

    # ---- user stream --------------------------------------------------------------------------------------------
    async def _user_stream_event_listener(self):
        async for event in self._iter_user_event_queue():
            try:
                channel, data = event["channel"], event["data"]
                if channel == CHANNEL_ORDERS:
                    self._process_order_event(data)
                elif channel == CHANNEL_TRADES:
                    self._process_trade_event(data)
                elif channel == CHANNEL_BALANCE:
                    self._merge_balance(data)
                elif channel == CHANNEL_POSITIONS:
                    await self._apply_position(data)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    def _process_order_event(self, order: Dict[str, Any]):
        tracker = self._order_tracker
        tracked = tracker.all_updatable_orders_by_exchange_order_id.get(str(order["id"]))
        if tracked is None and order.get("clientOrderId"):
            # the response that carried the exchange order id may have been lost, the client id still identifies it
            tracked = tracker.all_updatable_orders.get(str(order["clientOrderId"]))
        if tracked is not None:
            tracker.process_order_update(self._order_update_from_ccxt(order, tracked))

    def _process_trade_event(self, trade: Dict[str, Any]):
        tracked = self._order_tracker.all_fillable_orders_by_exchange_order_id.get(str(trade.get("order")))
        if tracked is not None:
            self._order_tracker.process_trade_update(self._trade_update_from_ccxt(trade, tracked))
