"""
Hummingbot's native perpetual connector test suite (``AbstractPerpetualDerivativeTests``) run against the Aster
perpetual connector.

The connector talks to Aster through ccxt, so these tests mock the real Aster futures API V3 (fapi.asterdex.com, V1
is discontinued) at the HTTP level with ``aioresponses`` and ccxt really parses and signs the requests (API wallet
EIP-712 signatures). Aster has no private stream in ccxt, the stream events used by the native suite are injected
through the user stream queue and exercise the shared event handling.
"""
import asyncio
import json
import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Callable, List, Optional, Tuple
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import ccxt
from aioresponses import aioresponses
from aioresponses.core import RequestCall
from eth_account import Account
from eth_account.messages import encode_typed_data
from yarl import URL

from hummingbot.connector.derivative.aster_perpetual import aster_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_derivative import AsterPerpetualDerivative
from hummingbot.connector.test_support.perpetual_derivative_test import AbstractPerpetualDerivativeTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.event.events import FundingPaymentCompletedEvent

BASE_URL = "https://fapi.asterdex.com/fapi/"


def url_regex(endpoint: str) -> re.Pattern:
    return re.compile(f"^{BASE_URL}{endpoint}".replace(".", r"\.").replace("?", r"\?") + ".*")


def form_body(request_call: RequestCall) -> dict:
    """Aster takes the signed parameters of POST and DELETE requests form encoded in the body."""
    return {key: values[0] for key, values in parse_qs(request_call.kwargs["data"].decode()).items()}


def signed_message(request_call: RequestCall, url: str) -> str:
    """The url encoded parameters a V3 request signs: the body of a POST/DELETE, the query string of a GET."""
    data = request_call.kwargs.get("data")
    raw = data.decode() if data else urlsplit(url).query
    return raw.rsplit("&signature=", 1)[0]


def recovered_signer(message: str, signature: str) -> str:
    typed_data = {
        "types": {"EIP712Domain": [{"name": "name", "type": "string"}, {"name": "version", "type": "string"},
                                   {"name": "chainId", "type": "uint256"},
                                   {"name": "verifyingContract", "type": "address"}],
                  "Message": [{"name": "msg", "type": "string"}]},
        "primaryType": "Message",
        "domain": {"name": "AsterSignTransaction", "version": "1", "chainId": 1666,
                   "verifyingContract": "0x0000000000000000000000000000000000000000"},
        "message": {"msg": message}}
    return Account.recover_message(encode_typed_data(full_message=typed_data), signature=signature)


class AsterPerpetualDerivativeTests(
    AbstractPerpetualDerivativeTests.PerpetualDerivativeTests,
    IsolatedAsyncioWrapperTestCase,
):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        signer = Account.from_key("0x" + "22" * 32)  # throwaway API wallet
        cls.user = "0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e"
        cls.signer = signer.address
        cls.private_key = signer.key.hex()
        cls.exchange_order_id = "22542179"

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        # load the markets the way the connector does at startup: ccxt parses the real Aster exchange info payload
        with aioresponses() as mock_api:
            mock_api.get(self.all_symbols_url, body=json.dumps(self.all_symbols_request_mock_response))
            markets = await self.exchange._load_ccxt_markets()
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(markets)

    async def asyncTearDown(self) -> None:
        await self.exchange._ccxt_client.close()
        await super().asyncTearDown()

    # ---- helpers ---------------------------------------------------------------------------------------------
    @property
    def ccxt_symbol(self) -> str:
        return f"{self.base_asset}/{self.quote_asset}:{self.quote_asset}"

    @property
    def aster_symbol(self) -> str:
        return f"{self.base_asset}{self.quote_asset}"

    def _symbol_info(self, symbol: Optional[str] = None, status: str = "TRADING", filters: bool = True) -> dict:
        symbol = symbol or self.aster_symbol
        info = {
            "symbol": symbol, "pair": symbol, "contractType": "PERPETUAL", "deliveryDate": 4133404800000,
            "onboardDate": 1569398400000, "status": status, "maintMarginPercent": "2.5",
            "requiredMarginPercent": "5", "baseAsset": symbol.replace(self.quote_asset, ""),
            "quoteAsset": self.quote_asset, "marginAsset": self.quote_asset, "pricePrecision": 2,
            "quantityPrecision": 3, "baseAssetPrecision": 8, "quotePrecision": 8, "underlyingType": "COIN",
            "settlePlan": 0, "triggerProtect": "0.15",
            "filters": [
                {"filterType": "PRICE_FILTER", "maxPrice": "1000000", "minPrice": "0.01", "tickSize": "0.01"},
                {"filterType": "LOT_SIZE", "maxQty": "1000", "minQty": "0.001", "stepSize": "0.001"},
                {"filterType": "MARKET_LOT_SIZE", "maxQty": "500", "minQty": "0.001", "stepSize": "0.001"},
                {"filterType": "MAX_NUM_ORDERS", "limit": 200},
                {"filterType": "MIN_NOTIONAL", "notional": "5"},
            ],
            "orderTypes": ["LIMIT", "MARKET"], "timeInForce": ["GTC", "IOC", "FOK", "GTX"],
        }
        if not filters:
            info["filters"] = []
            del info["pricePrecision"]
            del info["quantityPrecision"]
        return info

    def _exchange_info(self, *symbols: dict) -> dict:
        return {"timezone": "UTC", "serverTime": 1640780000000, "rateLimits": [], "exchangeFilters": [],
                "assets": [], "symbols": list(symbols)}

    def _order_data(self, order: InFlightOrder, status: str, executed: Decimal = Decimal("0")) -> dict:
        return {
            "orderId": int(order.exchange_order_id or self.exchange_order_id), "symbol": self.aster_symbol,
            "status": status, "clientOrderId": order.client_order_id, "price": str(order.price),
            "avgPrice": str(order.price) if executed > 0 else "0", "origQty": str(order.amount),
            "executedQty": str(executed), "cumQuote": str(executed * order.price), "timeInForce": "GTC",
            "type": "LIMIT", "reduceOnly": False, "closePosition": False,
            "side": order.trade_type.name, "positionSide": "BOTH", "stopPrice": "0", "workingType": "CONTRACT_PRICE",
            "priceProtect": False, "origType": "LIMIT", "time": 1640780000000, "updateTime": 1640780000100}

    def _trade_data(self, order: InFlightOrder, amount: Decimal) -> dict:
        return {
            "symbol": self.aster_symbol, "id": int(self.expected_fill_trade_id),
            "orderId": int(order.exchange_order_id or self.exchange_order_id), "side": order.trade_type.name,
            "price": str(order.price), "qty": str(amount), "realizedPnl": "0", "marginAsset": self.quote_asset,
            "quoteQty": str(amount * order.price), "commission": "0.1", "commissionAsset": self.quote_asset,
            "time": 1640780000000, "positionSide": "BOTH", "maker": True, "buyer": order.trade_type == TradeType.BUY}

    def _mock_json(self, mock_api, method, url, payload, **kwargs):
        getattr(mock_api, method)(url, body=json.dumps(payload), **kwargs)

    # ---- URLs ------------------------------------------------------------------------------------------------
    @property
    def all_symbols_url(self):
        return url_regex("v3/exchangeInfo")

    @property
    def latest_prices_url(self):
        return url_regex("v3/ticker/24hr")

    @property
    def network_status_url(self):
        return url_regex("v3/time")

    @property
    def trading_rules_url(self):
        return url_regex("v3/exchangeInfo")

    @property
    def order_creation_url(self):
        return url_regex("v3/order")

    @property
    def balance_url(self):
        return f"{BASE_URL}v3/account"

    @property
    def funding_info_url(self):
        return url_regex("v3/premiumIndex")

    @property
    def funding_payment_url(self):
        return url_regex("v3/income")

    @property
    def funding_rate_history_url(self):
        return url_regex("v3/fundingRate")

    @property
    def order_url(self):
        return url_regex("v3/order")

    @property
    def trades_url(self):
        return url_regex("v3/userTrades")

    @property
    def set_leverage_url(self):
        return url_regex("v3/leverage")

    @property
    def set_position_mode_url(self):
        return url_regex("v3/positionSide/dual")

    # ---- market data fixtures --------------------------------------------------------------------------------
    @property
    def all_symbols_request_mock_response(self):
        return self._exchange_info(self._symbol_info())

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        response = self._exchange_info(
            self._symbol_info(),
            self._symbol_info(symbol="INVALIDHBOT", status="PENDING_TRADING"))  # not trading
        return "INVALID-HBOT", response

    @property
    def trading_rules_request_mock_response(self):
        return self._exchange_info(self._symbol_info())

    @property
    def trading_rules_request_erroneous_mock_response(self):
        return self._exchange_info(self._symbol_info(filters=False))

    @property
    def latest_prices_request_mock_response(self):
        return {"symbol": self.aster_symbol, "priceChange": "0", "priceChangePercent": "0", "weightedAvgPrice": "0",
                "lastPrice": "101.5", "lastQty": "1", "openPrice": "1", "highPrice": "1", "lowPrice": "1",
                "volume": "1", "quoteVolume": "1", "openTime": 1, "closeTime": 1640780000000, "firstId": 1,
                "lastId": 2, "count": 1}

    @property
    def network_status_request_successful_mock_response(self):
        return {"serverTime": 1640780000000}

    @property
    def order_creation_request_successful_mock_response(self):
        return {"orderId": int(self.exchange_order_id), "symbol": self.aster_symbol, "status": "NEW",
                "clientOrderId": "x", "price": "10000", "avgPrice": "0", "origQty": "100", "executedQty": "0",
                "cumQty": "0", "cumQuote": "0", "timeInForce": "GTC", "type": "LIMIT", "reduceOnly": False,
                "closePosition": False, "side": "BUY", "positionSide": "BOTH", "stopPrice": "0",
                "workingType": "CONTRACT_PRICE", "priceProtect": False, "origType": "LIMIT",
                "updateTime": 1640780000000}

    @property
    def expected_latest_price(self):
        return 101.5

    @property
    def expected_supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET, OrderType.LIMIT_MAKER]

    @property
    def expected_trading_rule(self):
        return TradingRule(
            trading_pair=self.trading_pair,
            min_order_size=Decimal("0.001"),
            max_order_size=Decimal("1000"),
            min_price_increment=Decimal("0.01"),
            min_base_amount_increment=Decimal("0.001"),
            min_notional_size=Decimal("5"),
            buy_order_collateral_token=self.quote_asset,
            sell_order_collateral_token=self.quote_asset,
        )

    @property
    def expected_logged_error_for_erroneous_trading_rule(self):
        return f"Error parsing the trading rule for {self.ccxt_symbol}. Skipping."

    @property
    def expected_exchange_order_id(self):
        return self.exchange_order_id

    @property
    def is_order_fill_http_update_included_in_status_update(self) -> bool:
        return True

    @property
    def is_order_fill_http_update_executed_during_websocket_order_event_processing(self) -> bool:
        return False

    @property
    def expected_partial_fill_price(self) -> Decimal:
        return Decimal("10000")

    @property
    def expected_partial_fill_amount(self) -> Decimal:
        return Decimal("0.5")

    @property
    def expected_fill_fee(self) -> TradeFeeBase:
        return AddedToCostTradeFee(
            percent_token=self.quote_asset,
            flat_fees=[TokenAmount(token=self.quote_asset, amount=Decimal("0.1"))])

    @property
    def expected_fill_trade_id(self) -> str:
        return "698759"

    # ---- account fixtures ------------------------------------------------------------------------------------
    def _account(self, *assets: dict) -> dict:
        return {"feeTier": 0, "canTrade": True, "canDeposit": True, "canWithdraw": True, "updateTime": 0,
                "totalInitialMargin": "0", "totalMaintMargin": "0", "totalWalletBalance": "0",
                "totalUnrealizedProfit": "0", "totalMarginBalance": "0", "availableBalance": "0",
                "assets": list(assets), "positions": []}

    def _account_asset(self, asset: str, wallet: str, available: str) -> dict:
        return {"asset": asset, "walletBalance": wallet, "unrealizedProfit": "0", "marginBalance": wallet,
                "maintMargin": "0", "initialMargin": "0", "positionInitialMargin": "0",
                "openOrderInitialMargin": "0", "crossWalletBalance": wallet, "crossUnPnl": "0",
                "availableBalance": available, "maxWithdrawAmount": available, "marginAvailable": True,
                "updateTime": 1640780000000}

    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return self._account(self._account_asset(self.base_asset, "15", "10"),
                             self._account_asset(self.quote_asset, "2000", "2000"))

    @property
    def balance_request_mock_response_only_base(self):
        return self._account(self._account_asset(self.base_asset, "15", "10"))

    @property
    def balance_event_websocket_update(self):
        return {"channel": "balance", "data": {
            self.base_asset: {"free": 10.0, "used": 5.0, "total": 15.0}, "info": {}}}

    @property
    def expected_supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]  # hedge mode stays off until it has been verified on the real exchange

    # ---- funding fixtures ------------------------------------------------------------------------------------
    @property
    def funding_info_mock_response(self):
        return {"symbol": self.aster_symbol, "markPrice": "2", "indexPrice": "1", "estimatedSettlePrice": "1",
                "lastFundingRate": "3", "interestRate": "0.0001",
                "nextFundingTime": self.target_funding_info_next_funding_utc_timestamp * 1000,
                "time": 1640780000000}

    @property
    def empty_funding_payment_mock_response(self):
        return []

    @property
    def funding_payment_mock_response(self):
        return [{"symbol": self.aster_symbol, "incomeType": "FUNDING_FEE", "income": "200", "asset": self.quote_asset,
                 "info": "FUNDING_FEE", "time": self.target_funding_payment_timestamp * 1000, "tranId": "1",
                 "tradeId": ""}]

    @property
    def funding_rate_history_mock_response(self):
        return [{"symbol": self.aster_symbol, "fundingRate": "100",
                 "fundingTime": self.target_funding_payment_timestamp * 1000}]

    def funding_info_event_for_websocket_update(self):
        return {
            "symbol": self.ccxt_symbol,
            "indexPrice": 10.0,
            "markPrice": 20.0,
            "fundingRate": 30.0,
            "nextFundingTimestamp": self.target_funding_info_next_funding_utc_timestamp_ws_updated * 1000,
        }

    # ---- instance --------------------------------------------------------------------------------------------
    def exchange_symbol_for_tokens(self, base_token: str, quote_token: str) -> str:
        return f"{base_token}/{quote_token}:{quote_token}"

    def create_exchange_instance(self):
        return AsterPerpetualDerivative(
            aster_perpetual_user=self.user,
            aster_perpetual_signer=self.signer,
            aster_perpetual_private_key=self.private_key,
            trading_pairs=[self.trading_pair],
        )

    # ---- request validation ----------------------------------------------------------------------------------
    def validate_auth_credentials_present(self, request_call: RequestCall):
        # no V1 api key header any more. GET requests carry the signature in the URL, which a recorded call does not
        # keep: test_every_private_request_is_a_signed_v3_request_and_none_is_v1 verifies those
        self.assertNotIn("X-MBX-APIKEY", request_call.kwargs.get("headers") or {})
        data = request_call.kwargs.get("data")
        if data:
            fields = {key: values[0] for key, values in parse_qs(data.decode()).items()}
            self.assertEqual(self.user, fields["user"])
            self.assertEqual(self.signer, fields["signer"])
            self.assertIn("nonce", fields)
            self.assertEqual(self.signer, recovered_signer(data.decode().rsplit("&signature=", 1)[0],
                                                           fields["signature"]))

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        body = form_body(request_call)
        self.assertEqual(self.signer, recovered_signer(signed_message(request_call, ""), body["signature"]))
        self.assertEqual(self.aster_symbol, body["symbol"])
        self.assertEqual(order.trade_type.name, body["side"])
        self.assertEqual("LIMIT", body["type"])
        self.assertEqual(order.amount, Decimal(body["quantity"]))
        self.assertEqual(Decimal("10000"), Decimal(body["price"]))
        self.assertEqual(order.client_order_id, body["newClientOrderId"])
        self.assertEqual("GTC", body["timeInForce"])
        if order.position == PositionAction.CLOSE:
            self.assertEqual("true", body["reduceOnly"])
        else:
            self.assertNotIn("reduceOnly", body)

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        body = form_body(request_call)
        self.assertEqual(self.aster_symbol, body["symbol"])
        self.assertEqual(order.exchange_order_id, body["orderId"])

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        self.assertIsNone(request_call.kwargs.get("data"))  # GET request, the parameters travel in the URL

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        self.assertIsNone(request_call.kwargs.get("data"))

    # ---- cancelation -----------------------------------------------------------------------------------------
    def configure_successful_cancelation_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        self._mock_json(mock_api, "delete", self.order_url, self._order_data(order, "CANCELED"), callback=callback)
        return self.order_url

    def configure_erroneous_cancelation_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        self._mock_json(mock_api, "delete", self.order_url, {"code": -1000, "msg": "Some problem"}, status=400,
                        callback=callback)
        return self.order_url

    def configure_order_not_found_error_cancelation_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        self._mock_json(mock_api, "delete", self.order_url, {"code": -2011, "msg": "Unknown order sent."},
                        status=400, callback=callback)
        return self.order_url

    def configure_one_successful_one_erroneous_cancel_all_response(
            self, successful_order: InFlightOrder, erroneous_order: InFlightOrder,
            mock_api: aioresponses) -> List[str]:
        self.configure_successful_cancelation_response(successful_order, mock_api)
        self.configure_erroneous_cancelation_response(erroneous_order, mock_api)
        return [self.order_url]

    # ---- order status and fills ------------------------------------------------------------------------------
    def _configure_order_status(self, order, mock_api, status, executed=Decimal("0"), callback=None):
        self._mock_json(mock_api, "get", self.order_url, self._order_data(order, status, executed),
                        callback=callback)
        return self.order_url

    def configure_completely_filled_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        return self._configure_order_status(order, mock_api, "FILLED", order.amount, callback)

    def configure_canceled_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        return self._configure_order_status(order, mock_api, "CANCELED", callback=callback)

    def configure_open_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        return self._configure_order_status(order, mock_api, "NEW", callback=callback)

    def configure_http_error_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        mock_api.get(self.order_url, status=401, callback=callback)
        return self.order_url

    def configure_partially_filled_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        return self._configure_order_status(
            order, mock_api, "PARTIALLY_FILLED", self.expected_partial_fill_amount, callback)

    def configure_order_not_found_error_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        self._mock_json(mock_api, "get", self.order_url, {"code": -2013, "msg": "Order does not exist."},
                        status=400, callback=callback)
        return self.order_url

    def configure_partial_fill_trade_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        self._mock_json(mock_api, "get", self.trades_url,
                        [self._trade_data(order, self.expected_partial_fill_amount)], callback=callback)
        return self.trades_url

    def configure_erroneous_http_fill_trade_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        mock_api.get(self.trades_url, status=400, callback=callback)
        return self.trades_url

    def configure_full_fill_trade_response(
            self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = None) -> str:
        self._mock_json(mock_api, "get", self.trades_url, [self._trade_data(order, order.amount)],
                        callback=callback)
        return self.trades_url

    # ---- position mode and leverage --------------------------------------------------------------------------
    def configure_successful_set_position_mode(
            self, position_mode: PositionMode, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None):
        self._mock_json(mock_api, "post", self.set_position_mode_url, {"code": 200, "msg": "success"},
                        callback=callback)
        return self.set_position_mode_url

    def configure_failed_set_position_mode(
            self, position_mode: PositionMode, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> Tuple[str, str]:
        body = json.dumps({"code": -1000, "msg": "Some problem"})
        mock_api.post(self.set_position_mode_url, status=400, body=body, callback=callback)
        return self.set_position_mode_url, f"aster {body}"  # the message ccxt raises with

    def configure_successful_set_leverage(
            self, leverage: int, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None):
        self._mock_json(mock_api, "post", self.set_leverage_url,
                        {"leverage": leverage, "maxNotionalValue": "1000000", "symbol": self.aster_symbol},
                        callback=callback)
        return self.set_leverage_url

    def configure_failed_set_leverage(
            self, leverage: int, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> Tuple[str, str]:
        body = json.dumps({"code": -1000, "msg": "Some problem"})
        mock_api.post(self.set_leverage_url, status=400, body=body, callback=callback)
        return self.set_leverage_url, f"aster {body}"  # the message ccxt raises with

    # ---- private stream events (ccxt unified structures) -----------------------------------------------------
    def _ccxt_order(self, order: InFlightOrder, status: str, filled: Decimal) -> dict:
        return {"id": order.exchange_order_id, "clientOrderId": order.client_order_id, "symbol": self.ccxt_symbol,
                "status": status, "filled": float(filled), "amount": float(order.amount),
                "lastUpdateTimestamp": int(self.exchange.current_timestamp * 1e3)}

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        return {"channel": "orders", "data": self._ccxt_order(order, "open", Decimal("0"))}

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        return {"channel": "orders", "data": self._ccxt_order(order, "canceled", Decimal("0"))}

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {"channel": "orders", "data": self._ccxt_order(order, "closed", order.amount)}

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {"channel": "trades", "data": {
            "id": self.expected_fill_trade_id, "order": order.exchange_order_id, "symbol": self.ccxt_symbol,
            "timestamp": int(self.exchange.current_timestamp * 1e3), "side": order.trade_type.name.lower(),
            "price": float(order.price), "amount": float(order.amount),
            "fee": {"currency": self.quote_asset, "cost": 0.1}}}

    def position_event_for_full_fill_websocket_update(self, order: InFlightOrder, unrealized_pnl: float):
        # Aster has no private stream in ccxt (everything is polled), the generic positions channel is what this
        # event exercises
        return {"channel": "positions", "data": {
            "symbol": self.ccxt_symbol, "side": "short" if order.trade_type == TradeType.SELL else "long",
            "contracts": float(order.amount), "contractSize": 1.0, "entryPrice": float(order.price),
            "unrealizedPnl": unrealized_pnl, "leverage": order.leverage}}

    # ---- overrides of the native tests that need two Aster endpoints -----------------------------------------
    @aioresponses()
    def test_funding_payment_polling_loop_sends_update_event(self, mock_api):
        """Aster's funding payments (income) carry no rate, the rate comes from the funding rate history."""
        def callback(*args, **kwargs):
            request_sent_event.set()

        self._simulate_trading_rules_initialized()
        request_sent_event = asyncio.Event()

        async def run_test():
            self._mock_json(mock_api, "get", self.funding_payment_url, self.empty_funding_payment_mock_response,
                            callback=callback)
            _ = asyncio.create_task(self.exchange._funding_payment_polling_loop())
            await asyncio.sleep(0.1)  # first pass only initializes the timestamps, no event
            self.assertEqual(0, len(self.funding_payment_logger.event_log))

            self._mock_json(mock_api, "get", self.funding_payment_url, self.funding_payment_mock_response,
                            callback=callback, repeat=True)
            self._mock_json(mock_api, "get", self.funding_rate_history_url,
                            self.funding_rate_history_mock_response, repeat=True)
            request_sent_event.clear()
            self.exchange._funding_fee_poll_notifier.set()
            await request_sent_event.wait()
            await asyncio.sleep(0.1)  # the rate lookup follows the payment request
            self.assertEqual(1, len(self.funding_payment_logger.event_log))

            request_sent_event.clear()
            self.exchange._funding_fee_poll_notifier.set()
            await request_sent_event.wait()

        self.async_run_with_timeout(run_test())

        self.assertEqual(1, len(self.funding_payment_logger.event_log))
        event: FundingPaymentCompletedEvent = self.funding_payment_logger.event_log[0]
        self.assertEqual(self.target_funding_payment_timestamp, event.timestamp)
        self.assertEqual(self.exchange.name, event.market)
        self.assertEqual(self.trading_pair, event.trading_pair)
        self.assertEqual(self.target_funding_payment_payment_amount, event.amount)
        self.assertEqual(self.target_funding_payment_funding_rate, event.funding_rate)

    # ---- Aster specific --------------------------------------------------------------------------------------
    def test_get_buy_and_sell_collateral_tokens(self):
        self._simulate_trading_rules_initialized()
        self.assertEqual(self.quote_asset, self.exchange.get_buy_collateral_token(self.trading_pair))
        self.assertEqual(self.quote_asset, self.exchange.get_sell_collateral_token(self.trading_pair))

    def test_supported_position_modes(self):
        self.assertEqual(self.expected_supported_position_modes, self.exchange.supported_position_modes())

    @aioresponses()
    async def test_every_private_request_is_a_signed_v3_request_and_none_is_v1(self, mock_api):
        order = self._tracked_order()
        self._mock_json(mock_api, "get", self.order_url, self._order_data(order, "NEW"))
        self._mock_json(mock_api, "get", self.trades_url, [])
        self._mock_json(mock_api, "get", url_regex("v3/account"), self.balance_request_mock_response_for_base_and_quote)
        self._mock_json(mock_api, "post", self.set_leverage_url,
                        {"leverage": 5, "maxNotionalValue": "1", "symbol": self.aster_symbol})
        self._mock_json(mock_api, "delete", self.order_url, self._order_data(order, "CANCELED"))

        await self.exchange._update_balances()
        await self.exchange._request_order_status(order)
        await self.exchange._all_trade_updates_for_order(order)
        await self.exchange._execute_set_leverage(self.trading_pair, 5)
        await self.exchange._place_cancel("HBOT-1", order)

        calls = [(key, calls[0]) for key, calls in mock_api.requests.items()]
        self.assertGreaterEqual(len(calls), 5)
        for (method, url), call in calls:
            self.assertEqual("fapi.asterdex.com", url.host)
            self.assertTrue(url.path.startswith("/fapi/v3/"), str(url))
            raw = call.kwargs["data"].decode() if call.kwargs.get("data") else url.raw_query_string
            fields = {key: values[0] for key, values in parse_qs(raw).items()}
            self.assertEqual({self.user, self.signer}, {fields["user"], fields["signer"]})
            if call.kwargs.get("data"):  # aioresponses sorts the query of a recorded GET, its signed order is lost
                self.assertEqual(self.signer, recovered_signer(raw.rsplit("&signature=", 1)[0], fields["signature"]))
            self.assertTrue(fields["signature"].startswith("0x"))
            self.assertNotIn("timestamp", fields)
            self.assertNotIn("recvWindow", fields)
            self.assertNotIn("X-MBX-APIKEY", call.kwargs.get("headers") or {})

    def test_the_private_key_and_signatures_are_never_logged_or_shown(self):
        message = f"aster POST https://fapi.asterdex.com/fapi/v3/order?x=1&signature=0x{'ab' * 65}"
        from hummingbot.connector.ccxt_adapter import ccxt_utils
        cleaned = ccxt_utils.sanitize_error_message(message)
        self.assertNotIn("ab" * 20, cleaned)

    def test_spot_markets_are_never_downloaded(self):
        spot_stub = self.exchange._ccxt_client.sapiPublicGetV1ExchangeInfo
        self.assertEqual({"symbols": []}, self.async_run_with_timeout(spot_stub()))

    @aioresponses()
    async def test_update_positions_polls_position_risk(self, mock_api):
        bracket = {"bracket": 1, "initialLeverage": 125, "notionalCap": 50000, "notionalFloor": 0,
                   "maintMarginRatio": 0.004, "cum": 0}
        brackets = [{"symbol": self.aster_symbol, "brackets": [bracket]}]
        self._mock_json(mock_api, "get", url_regex("v3/leverageBrackets"), brackets, repeat=True)
        positions = [{"symbol": self.aster_symbol, "positionAmt": "-0.5", "entryPrice": "100", "markPrice": "101",
                      "unRealizedProfit": "-0.5", "liquidationPrice": "150", "leverage": "10",
                      "maxNotionalValue": "1000000", "marginType": "cross", "isolatedMargin": "0",
                      "isAutoAddMargin": "false", "positionSide": "BOTH", "notional": "-50.5",
                      "isolatedWallet": "0", "updateTime": 1640780000000}]
        self._mock_json(mock_api, "get", url_regex("v3/positionRisk"), positions)

        await self.exchange._update_positions()

        position = list(self.exchange.account_positions.values())[0]
        self.assertEqual(self.trading_pair, position.trading_pair)
        self.assertEqual(Decimal("-0.5"), position.amount)
        self.assertEqual(Decimal("100"), position.entry_price)
        self.assertEqual(Decimal("-0.5"), position.unrealized_pnl)  # Aster provides it, unlike MEXC
        self.assertEqual(Decimal("10"), position.leverage)

    # ---- order parameters ------------------------------------------------------------------------------------
    async def _place_and_capture(self, mock_api, trade_type, order_type, position_action, price=Decimal("100")):
        bodies = []
        self._mock_json(mock_api, "post", self.order_creation_url, self.order_creation_request_successful_mock_response,
                        callback=lambda url, **kwargs: bodies.append(
                            {k: v[0] for k, v in parse_qs(kwargs["data"].decode()).items()}))
        exchange_order_id, _ = await self.exchange._place_order(
            "HBOT-1", self.trading_pair, Decimal("1"), trade_type, order_type, price, position_action)
        self.assertEqual(self.exchange_order_id, exchange_order_id)
        return bodies[0]

    @aioresponses()
    async def test_place_market_order_has_no_price(self, mock_api):
        body = await self._place_and_capture(mock_api, TradeType.SELL, OrderType.MARKET, PositionAction.OPEN,
                                             Decimal("NaN"))
        self.assertEqual("MARKET", body["type"])
        self.assertNotIn("price", body)
        self.assertNotIn("timeInForce", body)

    @aioresponses()
    async def test_place_limit_maker_order_is_post_only(self, mock_api):
        body = await self._place_and_capture(mock_api, TradeType.BUY, OrderType.LIMIT_MAKER, PositionAction.OPEN)
        self.assertEqual("GTX", body["timeInForce"])

    @aioresponses()
    async def test_one_way_close_orders_are_reduce_only(self, mock_api):
        for trade_type in (TradeType.BUY, TradeType.SELL):
            body = await self._place_and_capture(mock_api, trade_type, OrderType.LIMIT, PositionAction.CLOSE)
            self.assertEqual("true", body["reduceOnly"])
            self.assertNotIn("positionSide", body)

    @aioresponses()
    async def test_hedge_mode_orders_name_the_position_side_and_are_not_reduce_only(self, mock_api):
        self.exchange._perpetual_trading.set_position_mode(PositionMode.HEDGE)
        expected = {
            (TradeType.BUY, PositionAction.OPEN): "LONG", (TradeType.SELL, PositionAction.CLOSE): "LONG",
            (TradeType.SELL, PositionAction.OPEN): "SHORT", (TradeType.BUY, PositionAction.CLOSE): "SHORT"}
        for (trade_type, position_action), position_side in expected.items():
            with self.subTest(trade_type=trade_type, position_action=position_action):
                body = await self._place_and_capture(mock_api, trade_type, OrderType.LIMIT, position_action)
                self.assertEqual(position_side, body["positionSide"])
                self.assertNotIn("reduceOnly", body)

    def test_order_state_mapping(self):
        cases = [("open", 0, OrderState.OPEN), ("open", 3, OrderState.PARTIALLY_FILLED),
                 ("closed", 10, OrderState.FILLED), ("canceled", 0, OrderState.CANCELED),
                 ("expired", 0, OrderState.CANCELED), ("rejected", 0, OrderState.FAILED)]
        for status, filled, expected in cases:
            with self.subTest(status=status, filled=filled):
                self.assertEqual(expected, self.exchange._order_state_from_ccxt(status, filled))

    def test_error_classification(self):
        self.assertTrue(self.exchange._is_request_exception_related_to_time_synchronizer(ccxt.InvalidNonce("t")))
        self.assertFalse(self.exchange._is_request_exception_related_to_time_synchronizer(ccxt.NetworkError("x")))
        for check in (self.exchange._is_order_not_found_during_cancelation_error,
                      self.exchange._is_order_not_found_during_status_update_error):
            self.assertTrue(check(ccxt.OrderNotFound("gone")))
            self.assertFalse(check(ccxt.NetworkError("down")))

    @aioresponses()
    async def test_funding_rate_lookup_picks_the_settlement_nearest_to_the_payment(self, mock_api):
        history = [{"symbol": self.aster_symbol, "fundingRate": "0.5", "fundingTime": 1_700_000_000_000 - 40_000},
                   {"symbol": self.aster_symbol, "fundingRate": "0.7", "fundingTime": 1_700_000_000_000 + 1_000}]
        self._mock_json(mock_api, "get", self.funding_rate_history_url, history)
        rate = await self.exchange._funding_rate_for_payment(self.ccxt_symbol, {"timestamp": 1_700_000_000_000})
        self.assertEqual(Decimal("0.7"), rate)

    @aioresponses()
    async def test_funding_rate_lookup_without_history_is_unknown(self, mock_api):
        self._mock_json(mock_api, "get", self.funding_rate_history_url, [])
        rate = await self.exchange._funding_rate_for_payment(self.ccxt_symbol, {"timestamp": 1_700_000_000_000})
        self.assertEqual(Decimal("-1"), rate)

    @aioresponses()
    async def test_ccxt_calls_go_through_the_hummingbot_throttler(self, mock_api):
        self._mock_json(mock_api, "get", self.network_status_url, self.network_status_request_successful_mock_response)
        with patch.object(self.exchange._throttler, "execute_task", wraps=self.exchange._throttler.execute_task) as spy:
            await self.exchange.check_network()
        spy.assert_called_once()
        self.assertEqual("GET v3/time", spy.call_args.args[0] if spy.call_args.args else spy.call_args.kwargs["limit_id"])

    @aioresponses()
    async def test_every_http_request_is_throttled_with_its_own_weight_even_inside_one_ccxt_call(self, mock_api):
        """fetch_positions sends the position risk and the leverage brackets requests: both must be paid for."""
        bracket = {"bracket": 1, "initialLeverage": 125, "notionalCap": 50000, "notionalFloor": 0,
                   "maintMarginRatio": 0.004, "cum": 0}
        self._mock_json(mock_api, "get", url_regex("v3/leverageBrackets"),
                        [{"symbol": self.aster_symbol, "brackets": [bracket]}], repeat=True)
        self._mock_json(mock_api, "get", url_regex("v3/positionRisk"), [])
        with patch.object(self.exchange._throttler, "execute_task", wraps=self.exchange._throttler.execute_task) as spy:
            await self.exchange._update_positions()
        used = [call.args[0] if call.args else call.kwargs["limit_id"] for call in spy.call_args_list]
        self.assertIn("GET v3/positionRisk", used)
        self.assertGreaterEqual(len(used), 2)

    async def test_the_documented_account_limits_are_the_ones_of_the_throttler(self):
        limits = self.exchange._throttler._id_to_limit_map
        self.assertEqual(2400, limits[CONSTANTS.REQUEST_WEIGHT_LIMIT_ID].limit)
        self.assertEqual(300, limits[CONSTANTS.ORDERS_10S_LIMIT_ID].limit)

    @aioresponses()
    async def test_a_rate_limit_answer_pauses_the_connector_and_the_order_is_not_repeated(self, mock_api):
        order = self._tracked_order()
        self._mock_json(mock_api, "post", self.order_creation_url, {"code": -1003, "msg": "Too many requests"},
                        status=429)
        self.exchange._ccxt_client.BACKOFF_BASE = 0.05
        with self.assertRaises(ccxt.RateLimitExceeded):
            await self.exchange._place_order(order.client_order_id, self.trading_pair, Decimal("1"), TradeType.BUY,
                                             OrderType.LIMIT, Decimal("100"), PositionAction.OPEN)
        self.assertEqual(1, len(mock_api.requests[("POST", URL(f"{BASE_URL}v3/order"))]))
        self.assertGreater(self.exchange._ccxt_client._hb_pause_until, 0)

    def test_private_data_is_polled_because_ccxt_has_no_private_stream(self):
        """No watchOrders/watchMyTrades/watchBalance/watchPositions for Aster: the connector polls every 5 seconds."""
        self.assertFalse(self.exchange._user_stream_tracker.data_source.streams_private_data)
        self.assertEqual(self.exchange.SHORT_POLL_INTERVAL, self.exchange._get_poll_interval(1_000_000))

    # ---- one-way mode only until hedge mode is verified ------------------------------------------------------
    @aioresponses()
    def test_set_position_mode_failure(self, mock_api):
        request_sent_event = asyncio.Event()
        _, error_msg = self.configure_failed_set_position_mode(
            position_mode=PositionMode.ONEWAY, mock_api=mock_api,
            callback=lambda *args, **kwargs: request_sent_event.set())
        self.exchange.set_position_mode(PositionMode.ONEWAY)
        self.async_run_with_timeout(request_sent_event.wait())
        self.assertTrue(self.is_logged("ERROR", f"Failed to set position mode to {PositionMode.ONEWAY}: {error_msg}"))

    @aioresponses()
    def test_set_position_mode_success(self, mock_api):
        request_sent_event = asyncio.Event()
        self.configure_successful_set_position_mode(
            position_mode=PositionMode.ONEWAY, mock_api=mock_api,
            callback=lambda *args, **kwargs: request_sent_event.set())
        self.exchange.set_position_mode(PositionMode.ONEWAY)
        self.async_run_with_timeout(request_sent_event.wait())
        self.assertTrue(self.is_logged("INFO", f"Position mode switched to {PositionMode.ONEWAY}."))

    def test_hedge_mode_is_refused_until_it_is_verified(self):
        self.exchange.set_position_mode(PositionMode.HEDGE)
        self.assertTrue(self.is_logged("ERROR", f"Position mode {PositionMode.HEDGE} is not supported. Mode not set."))

    @aioresponses()
    async def test_an_account_in_hedge_mode_is_reported_at_start(self, mock_api):
        self._mock_json(mock_api, "get", self.set_position_mode_url, {"dualSidePosition": True})
        await self.exchange._check_account_position_mode()
        self.assertTrue(self.is_logged(
            "ERROR", "The Aster_perpetual account is in hedge mode but this connector supports only one-way mode. "
                     "Switch the account to one-way mode, orders may be rejected or open the wrong position."))

    @aioresponses()
    async def test_an_account_in_one_way_mode_is_not_reported(self, mock_api):
        self._mock_json(mock_api, "get", self.set_position_mode_url, {"dualSidePosition": False})
        await self.exchange._check_account_position_mode()
        self.assertFalse(any(record.levelname == "ERROR" for record in self.log_records))

    # ---- hardening found by review ---------------------------------------------------------------------------
    @aioresponses()
    async def test_an_order_is_recovered_by_its_client_order_id(self, mock_api):
        order = InFlightOrder(client_order_id="HBOT-1", trading_pair=self.trading_pair, order_type=OrderType.LIMIT,
                              trade_type=TradeType.BUY, amount=Decimal("1"), price=Decimal("100"),
                              creation_timestamp=1, exchange_order_id=self.exchange_order_id)
        self._mock_json(mock_api, "get", self.order_url, self._order_data(order, "NEW"))

        found = await self.exchange._fetch_order_by_client_id(self.ccxt_symbol, "HBOT-1")

        self.assertEqual(self.exchange_order_id, found["id"])
        self.assertEqual("open", found["status"])
        request = list(mock_api.requests.values())[-1][0]
        self.assertIsNone(request.kwargs.get("data"))

    @aioresponses()
    async def test_fills_are_recovered_page_by_page_from_the_order_creation_time(self, mock_api):
        self.exchange.TRADES_PAGE_SIZE = 2
        order = self._tracked_order()
        other = {**self._trade_data(order, Decimal("1")), "orderId": 1, "id": 1, "time": 1640780000010}
        mine = [{**self._trade_data(order, Decimal("0.5")), "id": 2, "time": 1640780000020},
                {**self._trade_data(order, Decimal("0.5")), "id": 3, "time": 1640780000030}]
        self._mock_json(mock_api, "get", self.trades_url, [other, mine[0]])  # a full page, one fill is mine
        self._mock_json(mock_api, "get", self.trades_url, [mine[1]])  # the next page

        updates = await self.exchange._all_trade_updates_for_order(order)

        self.assertEqual(["2", "3"], [update.trade_id for update in updates])

    def _tracked_order(self) -> InFlightOrder:
        self.exchange.start_tracking_order(
            order_id="HBOT-1", exchange_order_id=self.exchange_order_id, trading_pair=self.trading_pair,
            trade_type=TradeType.BUY, price=Decimal("100"), amount=Decimal("1"), order_type=OrderType.LIMIT,
            position_action=PositionAction.OPEN)
        return self.exchange.in_flight_orders["HBOT-1"]

    @aioresponses()
    async def test_a_perpetual_connector_never_touches_the_spot_api(self, mock_api):
        exchange = self.create_exchange_instance()
        try:
            mock_api.get(self.all_symbols_url, body=json.dumps(self.all_symbols_request_mock_response))
            self._mock_json(mock_api, "get", url_regex("v3/account"), self.balance_request_mock_response_for_base_and_quote)

            await exchange._update_balances()

            self.assertEqual({"fapi.asterdex.com"}, {key[1].host for key in mock_api.requests})
        finally:
            await exchange._ccxt_client.close()
