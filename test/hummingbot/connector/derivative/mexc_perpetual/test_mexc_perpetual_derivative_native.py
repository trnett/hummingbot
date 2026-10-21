"""
Hummingbot's native perpetual connector test suite (``AbstractPerpetualDerivativeTests``) run against the MEXC
perpetual connector.

The connector talks to MEXC through ccxt, so these tests mock the real MEXC contract API (contract.mexc.com) at
the HTTP level with ``aioresponses`` and ccxt really parses and signs the requests. The private streams are
injected through the user stream queue, exactly as the native suite does for every other connector.
"""
import asyncio
import json
import re
from decimal import Decimal
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Callable, List, Optional, Tuple
from unittest.mock import AsyncMock, patch

import ccxt
from aioresponses import aioresponses
from aioresponses.core import RequestCall

from hummingbot.connector.derivative.mexc_perpetual import mexc_perpetual_constants as CONSTANTS
from hummingbot.connector.derivative.mexc_perpetual.mexc_perpetual_derivative import MexcPerpetualDerivative
from hummingbot.connector.test_support.perpetual_derivative_test import AbstractPerpetualDerivativeTests
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType
from hummingbot.core.data_type.funding_info import FundingInfo
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase

BASE_URL = "https://contract.mexc.com/api/v1/"


def url_regex(endpoint: str) -> re.Pattern:
    return re.compile(f"^{BASE_URL}{endpoint}".replace(".", r"\.").replace("?", r"\?") + ".*")


class MexcPerpetualDerivativeTests(
    AbstractPerpetualDerivativeTests.PerpetualDerivativeTests,
    IsolatedAsyncioWrapperTestCase,
):
    CONTRACT_SIZE = Decimal("0.1")  # 1 contract == 0.1 COINALPHA

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.api_key = "someKey"
        cls.api_secret = "someSecret"
        cls.exchange_order_id = "264995729269765120"

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        # load the markets the way the connector does at startup: ccxt parses the real MEXC contract detail payload
        with aioresponses() as mock_api:
            mock_api.get(self.all_symbols_url, body=json.dumps(self.all_symbols_request_mock_response))
            markets = await self.exchange._load_ccxt_markets()
        self.exchange._initialize_trading_pair_symbols_from_exchange_info(markets)
        # MEXC allows the contract detail request once per 5 seconds: the setup load must not delay the tested one
        self.exchange._throttler._task_logs.clear()

    async def asyncTearDown(self) -> None:
        await self.exchange._ccxt_client.close()
        await super().asyncTearDown()

    # ---- helpers ---------------------------------------------------------------------------------------------
    @property
    def ccxt_symbol(self) -> str:
        return f"{self.base_asset}/{self.quote_asset}:{self.quote_asset}"

    @property
    def mexc_symbol(self) -> str:
        return f"{self.base_asset}_{self.quote_asset}"

    def _to_contracts(self, amount: Decimal) -> Decimal:
        return amount / self.CONTRACT_SIZE

    def _contract_detail(self, symbol: Optional[str] = None, state: int = 0, **overrides) -> dict:
        detail = {
            "symbol": symbol or self.mexc_symbol,
            "displayName": f"{symbol or self.mexc_symbol} SWAP",
            "displayNameEn": f"{symbol or self.mexc_symbol} SWAP",
            "positionOpenType": 3,
            "baseCoin": (symbol or self.mexc_symbol).split("_")[0],
            "quoteCoin": (symbol or self.mexc_symbol).split("_")[1],
            "settleCoin": (symbol or self.mexc_symbol).split("_")[1],
            "contractSize": float(self.CONTRACT_SIZE),
            "minLeverage": 1,
            "maxLeverage": 125,
            "priceScale": 2,
            "volScale": 0,
            "amountScale": 4,
            "priceUnit": 0.01,
            "volUnit": 1,
            "minVol": 1,
            "maxVol": 1000000,
            "takerFeeRate": 0.0006,
            "makerFeeRate": 0.0002,
            "state": state,
            "isHidden": False,
        }
        detail.update(overrides)
        return detail

    def _order_data(self, order: InFlightOrder, state: int, deal_volume: Decimal = Decimal("0")) -> dict:
        return {
            "orderId": order.exchange_order_id or self.exchange_order_id,
            "symbol": self.mexc_symbol,
            "positionId": "0",
            "price": str(order.price),
            "vol": str(self._to_contracts(order.amount)),
            "leverage": "2",
            "side": "1" if order.trade_type == TradeType.BUY else "3",
            "category": "1",
            "orderType": "1",
            "dealAvgPrice": str(order.price) if deal_volume > 0 else "0",
            "dealVol": str(deal_volume),
            "takerFee": "0",
            "makerFee": "0.01",
            "feeCurrency": self.quote_asset,
            "openType": "2",
            "state": str(state),
            "externalOid": order.client_order_id,
            "errorCode": "0",
            "createTime": "1640780000000",
            "updateTime": "1640780000100",
            "positionMode": "2",
        }

    def _deal_data(self, order: InFlightOrder, amount: Decimal, trade_id: Optional[str] = None) -> dict:
        return {
            "id": trade_id or self.expected_fill_trade_id,
            "symbol": self.mexc_symbol,
            "side": "1" if order.trade_type == TradeType.BUY else "3",
            "vol": str(self._to_contracts(amount)),
            "price": str(order.price),
            "feeCurrency": self.quote_asset,
            "fee": "0.1",
            "timestamp": "1640780000000",
            "profit": "0",
            "category": "1",
            "orderId": order.exchange_order_id or self.exchange_order_id,
            "positionMode": "2",
            "taker": False,
        }

    # ---- URLs ------------------------------------------------------------------------------------------------
    @property
    def all_symbols_url(self):
        return url_regex("contract/detail")

    @property
    def latest_prices_url(self):
        return url_regex("contract/ticker")

    @property
    def network_status_url(self):
        return url_regex("contract/ping")

    @property
    def trading_rules_url(self):
        return url_regex("contract/detail")

    @property
    def order_creation_url(self):
        return url_regex("private/order/submit")

    @property
    def balance_url(self):
        return f"{BASE_URL}private/account/assets"

    @property
    def funding_info_url(self):
        return url_regex("contract/funding_rate")

    @property
    def funding_payment_url(self):
        return url_regex("private/position/funding_records")

    @property
    def cancel_url(self):
        return url_regex("private/order/cancel")

    @property
    def order_status_url(self):
        return url_regex("private/order/get/")

    @property
    def trades_url(self):
        return url_regex("private/order/deal_details/")  # trades of one order

    @property
    def ticker_url(self):
        return url_regex("contract/ticker")

    @property
    def set_leverage_url(self):
        return url_regex("private/position/change_leverage")

    @property
    def set_position_mode_url(self):
        return url_regex("private/position/change_position_mode")

    # ---- market data fixtures --------------------------------------------------------------------------------
    @property
    def all_symbols_request_mock_response(self):
        return {"success": True, "code": 0, "data": [self._contract_detail()]}

    @property
    def all_symbols_including_invalid_pair_mock_response(self) -> Tuple[str, Any]:
        response = {"success": True, "code": 0, "data": [
            self._contract_detail(),
            self._contract_detail(symbol="INVALID_PAIR", state=3),  # offline contract
            self._contract_detail(symbol="COINALPHA_USD", settleCoin="COINALPHA"),  # inverse contract
        ]}
        return "INVALID-PAIR", response

    @property
    def trading_rules_request_mock_response(self):
        return {"success": True, "code": 0, "data": [self._contract_detail()]}

    @property
    def trading_rules_request_erroneous_mock_response(self):
        detail = self._contract_detail()
        del detail["priceUnit"]
        del detail["volUnit"]
        return {"success": True, "code": 0, "data": [detail]}

    @property
    def latest_prices_request_mock_response(self):
        return {"success": True, "code": 0, "data": {
            "symbol": self.mexc_symbol, "lastPrice": 101.5, "bid1": 101.4, "ask1": 101.6, "volume24": 1,
            "amount24": 1, "holdVol": 1, "lower24Price": 1, "high24Price": 1, "riseFallRate": 0,
            "riseFallValue": 0, "indexPrice": 100.1, "fairPrice": 100.2, "fundingRate": 0.000063,
            "maxBidPrice": 1, "minAskPrice": 1, "timestamp": 1640780000000}}

    @property
    def order_creation_request_successful_mock_response(self):
        return {"success": True, "code": 0, "data": self.exchange_order_id}

    @property
    def network_status_request_successful_mock_response(self):
        return {"success": True, "code": 0, "data": 1640780000000}

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
            min_order_size=Decimal("0.1"),
            max_order_size=Decimal("100000"),
            min_price_increment=Decimal("0.01"),
            min_base_amount_increment=Decimal("0.1"),
            min_notional_size=Decimal("0"),
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
        return "299444585"

    # ---- account fixtures ------------------------------------------------------------------------------------
    @property
    def balance_request_mock_response_for_base_and_quote(self):
        return {"success": True, "code": 0, "data": [
            {"currency": self.base_asset, "positionMargin": 3, "availableBalance": 10, "cashBalance": 15,
             "frozenBalance": 2, "equity": 15, "unrealized": 0, "bonus": 0},
            {"currency": self.quote_asset, "positionMargin": 0, "availableBalance": 2000, "cashBalance": 2000,
             "frozenBalance": 0, "equity": 2000, "unrealized": 0, "bonus": 0},
        ]}

    @property
    def balance_request_mock_response_only_base(self):
        return {"success": True, "code": 0, "data": [
            {"currency": self.base_asset, "positionMargin": 3, "availableBalance": 10, "cashBalance": 15,
             "frozenBalance": 2, "equity": 15, "unrealized": 0, "bonus": 0},
        ]}

    @property
    def balance_event_websocket_update(self):
        return {"channel": "balance", "data": {
            self.base_asset: {"free": 10.0, "used": 5.0, "total": 15.0},
            "info": {},
        }}

    @property
    def expected_supported_position_modes(self) -> List[PositionMode]:
        return [PositionMode.ONEWAY]  # hedge mode stays off until it has been verified on the real exchange

    # ---- funding fixtures ------------------------------------------------------------------------------------
    @property
    def funding_info_mock_response(self):
        return {"success": True, "code": 0, "data": {
            "symbol": self.mexc_symbol, "fundingRate": 3, "maxFundingRate": 0.003, "minFundingRate": -0.003,
            "collectCycle": 8, "nextSettleTime": self.target_funding_info_next_funding_utc_timestamp * 1000,
            "timestamp": 1640780000000}}

    @property
    def funding_info_ticker_mock_response(self):
        return {"success": True, "code": 0, "data": {
            "symbol": self.mexc_symbol, "lastPrice": 1, "indexPrice": 1, "fairPrice": 2, "fundingRate": 3,
            "timestamp": 1640780000000}}

    @property
    def empty_funding_payment_mock_response(self):
        return {"success": True, "code": 0, "data": {
            "pageSize": 20, "totalCount": 0, "totalPage": 0, "currentPage": 1, "resultList": []}}

    @property
    def funding_payment_mock_response(self):
        return {"success": True, "code": 0, "data": {
            "pageSize": 20, "totalCount": 1, "totalPage": 1, "currentPage": 1, "resultList": [
                {"id": 7423910, "symbol": self.mexc_symbol, "positionType": 1, "positionValue": 29.3,
                 "funding": 200, "rate": 100, "settleTime": self.target_funding_payment_timestamp * 1000}]}}

    @property
    def target_funding_info_next_funding_utc_timestamp(self):
        return 1657099053

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
        return MexcPerpetualDerivative(
            mexc_perpetual_api_key=self.api_key,
            mexc_perpetual_api_secret=self.api_secret,
            trading_pairs=[self.trading_pair],
        )

    # ---- request validation ----------------------------------------------------------------------------------
    def validate_auth_credentials_present(self, request_call: RequestCall):
        headers = request_call.kwargs["headers"]
        self.assertEqual(self.api_key, headers["ApiKey"])
        self.assertIn("Request-Time", headers)
        self.assertIn("Signature", headers)

    def validate_order_creation_request(self, order: InFlightOrder, request_call: RequestCall):
        request_data = json.loads(request_call.kwargs["data"])
        self.assertEqual(self.mexc_symbol, request_data["symbol"])
        self.assertEqual(order.client_order_id, request_data["externalOid"])
        self.assertEqual(self._to_contracts(order.amount), Decimal(str(request_data["vol"])))
        self.assertEqual(Decimal("10000"), Decimal(str(request_data["price"])))
        self.assertEqual(1, request_data["type"])  # limit
        self.assertEqual(2, request_data["openType"])  # cross margin
        expected_side = {
            (TradeType.BUY, PositionAction.OPEN): 1,
            (TradeType.SELL, PositionAction.OPEN): 3,
            (TradeType.BUY, PositionAction.NIL): 1,  # no position action is an opening order
            (TradeType.SELL, PositionAction.NIL): 3,
            (TradeType.BUY, PositionAction.CLOSE): 2,
            (TradeType.SELL, PositionAction.CLOSE): 4,
        }[(order.trade_type, order.position)]
        self.assertEqual(expected_side, request_data["side"])

    def validate_order_cancelation_request(self, order: InFlightOrder, request_call: RequestCall):
        self.assertEqual([order.exchange_order_id], json.loads(request_call.kwargs["data"]))

    def validate_order_status_request(self, order: InFlightOrder, request_call: RequestCall):
        self.assertIsNone(request_call.kwargs.get("data"))  # GET request, the order id travels in the URL

    def validate_trades_request(self, order: InFlightOrder, request_call: RequestCall):
        self.assertIsNone(request_call.kwargs.get("data"))

    # ---- cancelation -----------------------------------------------------------------------------------------
    def _cancelation_response(self, order: InFlightOrder, error_code: int, error_message: str) -> dict:
        return {"success": True, "code": 0, "data": [
            {"orderId": order.exchange_order_id, "errorCode": error_code, "errorMsg": error_message}]}

    def configure_successful_cancelation_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        mock_api.post(self.cancel_url, body=json.dumps(self._cancelation_response(order, 0, "success")),
                      callback=callback)
        return self.cancel_url

    def configure_erroneous_cancelation_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        mock_api.post(self.cancel_url,
                      body=json.dumps(self._cancelation_response(order, 2041, "order state cannot be cancelled")),
                      callback=callback)
        return self.cancel_url

    def configure_order_not_found_error_cancelation_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        mock_api.post(self.cancel_url, body=json.dumps(self._cancelation_response(order, 2040, "order not exist")),
                      callback=callback)
        return self.cancel_url

    def configure_one_successful_one_erroneous_cancel_all_response(
            self, successful_order: InFlightOrder, erroneous_order: InFlightOrder,
            mock_api: aioresponses) -> List[str]:
        self.configure_successful_cancelation_response(successful_order, mock_api)
        self.configure_erroneous_cancelation_response(erroneous_order, mock_api)
        return [self.cancel_url]

    # ---- order status and fills ------------------------------------------------------------------------------
    def _configure_order_status(self, order, mock_api, state, deal_volume=Decimal("0"), callback=None):
        response = {"success": True, "code": 0, "data": self._order_data(order, state, deal_volume)}
        mock_api.get(self.order_status_url, body=json.dumps(response), callback=callback)
        return self.order_status_url

    def configure_completely_filled_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        return self._configure_order_status(order, mock_api, 3, self._to_contracts(order.amount), callback)

    def configure_canceled_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        return self._configure_order_status(order, mock_api, 4, callback=callback)

    def configure_open_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        return self._configure_order_status(order, mock_api, 2, callback=callback)

    def configure_http_error_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        mock_api.get(self.order_status_url, status=401, callback=callback)
        return self.order_status_url

    def configure_partially_filled_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        return self._configure_order_status(
            order, mock_api, 2, self._to_contracts(self.expected_partial_fill_amount), callback)

    def configure_order_not_found_error_order_status_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        response = {"success": False, "code": 2040, "message": "order not exist"}
        mock_api.get(self.order_status_url, body=json.dumps(response), callback=callback)
        return self.order_status_url

    def configure_partial_fill_trade_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        response = {"success": True, "code": 0,
                    "data": [self._deal_data(order, self.expected_partial_fill_amount)]}
        mock_api.get(self.trades_url, body=json.dumps(response), callback=callback)
        return self.trades_url

    def configure_erroneous_http_fill_trade_response(
            self, order: InFlightOrder, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> str:
        mock_api.get(self.trades_url, status=400, callback=callback)
        return self.trades_url

    def configure_full_fill_trade_response(
            self, order: InFlightOrder, mock_api: aioresponses, callback: Optional[Callable] = None) -> str:
        response = {"success": True, "code": 0, "data": [self._deal_data(order, order.amount)]}
        mock_api.get(self.trades_url, body=json.dumps(response), callback=callback)
        return self.trades_url

    # ---- position mode and leverage --------------------------------------------------------------------------
    def configure_successful_set_position_mode(
            self, position_mode: PositionMode, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None):
        mock_api.post(self.set_position_mode_url, body=json.dumps({"success": True, "code": 0}), callback=callback)
        return self.set_position_mode_url

    def configure_failed_set_position_mode(
            self, position_mode: PositionMode, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> Tuple[str, str]:
        body = json.dumps({"success": False, "code": 1000, "message": "Some problem"})
        mock_api.post(self.set_position_mode_url, status=400, body=body, callback=callback)
        return self.set_position_mode_url, f"mexc {body}"  # the message ccxt raises with

    def configure_successful_set_leverage(
            self, leverage: int, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None):
        # MEXC sets the leverage per position side, so the connector sends one request for each side
        for _ in range(2):
            mock_api.post(self.set_leverage_url, body=json.dumps({"success": True, "code": 0}), callback=callback)
        return self.set_leverage_url

    def configure_failed_set_leverage(
            self, leverage: int, mock_api: aioresponses,
            callback: Optional[Callable] = lambda *args, **kwargs: None) -> Tuple[str, str]:
        body = json.dumps({"success": False, "code": 1000, "message": "Some problem"})
        mock_api.post(self.set_leverage_url, status=400, body=body, callback=callback)
        return self.set_leverage_url, f"mexc {body}"  # the message ccxt raises with

    # ---- private stream events (ccxt unified structures) -----------------------------------------------------
    def _ccxt_order(self, order: InFlightOrder, status: str, filled: Decimal) -> dict:
        return {"id": order.exchange_order_id, "clientOrderId": order.client_order_id, "symbol": self.ccxt_symbol,
                "status": status, "filled": float(filled), "amount": float(self._to_contracts(order.amount)),
                "lastUpdateTimestamp": int(self.exchange.current_timestamp * 1e3)}

    def order_event_for_new_order_websocket_update(self, order: InFlightOrder):
        return {"channel": "orders", "data": self._ccxt_order(order, "open", Decimal("0"))}

    def order_event_for_canceled_order_websocket_update(self, order: InFlightOrder):
        return {"channel": "orders", "data": self._ccxt_order(order, "canceled", Decimal("0"))}

    def order_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {"channel": "orders",
                "data": self._ccxt_order(order, "closed", self._to_contracts(order.amount))}

    def trade_event_for_full_fill_websocket_update(self, order: InFlightOrder):
        return {"channel": "trades", "data": {
            "id": self.expected_fill_trade_id, "order": order.exchange_order_id, "symbol": self.ccxt_symbol,
            "timestamp": int(self.exchange.current_timestamp * 1e3), "side": order.trade_type.name.lower(),
            "price": float(order.price), "amount": float(self._to_contracts(order.amount)),
            "fee": {"currency": self.quote_asset, "cost": 0.1}}}

    def position_event_for_full_fill_websocket_update(self, order: InFlightOrder, unrealized_pnl: float):
        # ccxt.pro has no watch_positions for MEXC (the connector polls them), but the generic adapter handles the
        # positions channel of the exchanges that have it, which is what this event exercises
        return {"channel": "positions", "data": {
            "symbol": self.ccxt_symbol, "side": "short" if order.trade_type == TradeType.SELL else "long",
            "contracts": float(self._to_contracts(order.amount)), "contractSize": float(self.CONTRACT_SIZE),
            "entryPrice": float(order.price), "unrealizedPnl": unrealized_pnl, "leverage": order.leverage}}

    # ---- overrides of the native tests that need two MEXC endpoints ------------------------------------------
    @aioresponses()
    @patch("asyncio.Queue.get")
    def test_listen_for_funding_info_update_initializes_funding_info(self, mock_api, mock_queue_get):
        """MEXC funding rate and index/mark prices come from two endpoints, both are mocked here."""
        mock_api.get(self.funding_info_url, body=json.dumps(self.funding_info_mock_response))
        mock_api.get(self.ticker_url, body=json.dumps(self.funding_info_ticker_mock_response))
        mock_queue_get.side_effect = [asyncio.CancelledError]

        try:
            self.async_run_with_timeout(self.exchange._listen_for_funding_info())
        except asyncio.CancelledError:
            pass

        funding_info: FundingInfo = self.exchange.get_funding_info(self.trading_pair)
        self.assertEqual(self.trading_pair, funding_info.trading_pair)
        self.assertEqual(self.target_funding_info_index_price, funding_info.index_price)
        self.assertEqual(self.target_funding_info_mark_price, funding_info.mark_price)
        self.assertEqual(self.target_funding_info_next_funding_utc_timestamp, funding_info.next_funding_utc_timestamp)
        self.assertEqual(self.target_funding_info_rate, funding_info.rate)

    @aioresponses()
    @patch("asyncio.Queue.get")
    def test_listen_for_funding_info_update_updates_funding_info(self, mock_api, mock_queue_get):
        mock_api.get(self.funding_info_url, body=json.dumps(self.funding_info_mock_response))
        mock_api.get(self.ticker_url, body=json.dumps(self.funding_info_ticker_mock_response))
        mock_queue_get.side_effect = [self.funding_info_event_for_websocket_update(), asyncio.CancelledError]

        try:
            self.async_run_with_timeout(self.exchange._listen_for_funding_info())
        except asyncio.CancelledError:
            pass

        self.assertEqual(1, self.exchange._perpetual_trading.funding_info_stream.qsize())

    # ---- MEXC specific ---------------------------------------------------------------------------------------
    def test_get_buy_and_sell_collateral_tokens(self):
        self._simulate_trading_rules_initialized()
        self.assertEqual(self.quote_asset, self.exchange.get_buy_collateral_token(self.trading_pair))
        self.assertEqual(self.quote_asset, self.exchange.get_sell_collateral_token(self.trading_pair))

    def test_supported_position_modes(self):
        self.assertEqual(self.expected_supported_position_modes, self.exchange.supported_position_modes())

    def test_unknown_market_contract_size_is_never_guessed(self):
        self.exchange._ccxt_markets = {}
        with self.assertRaises(ValueError):
            self.exchange._to_contracts(self.ccxt_symbol, Decimal("1"))

    @aioresponses()
    async def test_update_positions_polls_open_positions(self, mock_api):
        response = {"success": True, "code": 0, "data": [{
            "positionId": 1394650, "symbol": self.mexc_symbol, "positionType": 2, "openType": 2, "state": 1,
            "holdVol": 20, "frozenVol": 0, "closeVol": 0, "holdAvgPrice": 100, "openAvgPrice": 100,
            "closeAvgPrice": 0, "liquidatePrice": 110, "oim": 0.1, "im": 0.1, "holdFee": 0, "realised": 0,
            "leverage": 10, "createTime": 1640780000000, "updateTime": 1640780000000}]}
        mock_api.get(url_regex("private/position/open_positions"), body=json.dumps(response))

        await self.exchange._update_positions()

        position = list(self.exchange.account_positions.values())[0]
        self.assertEqual(self.trading_pair, position.trading_pair)
        self.assertEqual(Decimal("-2"), position.amount)  # a short of 20 contracts of 0.1
        self.assertEqual(Decimal("100"), position.entry_price)
        self.assertEqual(Decimal("10"), position.leverage)

    @aioresponses()
    async def test_set_leverage_sends_one_request_per_position_side(self, mock_api):
        requests = []
        self.configure_successful_set_leverage(5, mock_api, callback=lambda url, **kwargs: requests.append(kwargs))

        success, message = await self.exchange._set_trading_pair_leverage(self.trading_pair, 5)

        self.assertTrue(success)
        self.assertEqual({1, 2}, {json.loads(request["data"])["positionType"] for request in requests})
        self.assertEqual({5}, {json.loads(request["data"])["leverage"] for request in requests})

    @aioresponses()
    async def test_ccxt_calls_go_through_the_hummingbot_throttler(self, mock_api):
        mock_api.get(self.network_status_url, body=json.dumps(self.network_status_request_successful_mock_response))
        with patch.object(self.exchange._throttler, "execute_task", wraps=self.exchange._throttler.execute_task) as spy:
            await self.exchange.check_network()
        spy.assert_called_once()
        limit_id = spy.call_args.args[0] if spy.call_args.args else spy.call_args.kwargs["limit_id"]
        self.assertIn(limit_id, {limit.limit_id for limit in self.exchange.rate_limits_rules})

    async def test_the_documented_endpoint_limits_are_the_ones_of_the_throttler(self):
        self.assertEqual(CONSTANTS.RATE_LIMITS, self.exchange.rate_limits_rules)
        self.assertEqual((1, 5), (self.exchange._throttler._id_to_limit_map["detail"].limit,
                                  self.exchange._throttler._id_to_limit_map["detail"].time_interval))

    async def test_stop_network_closes_the_ccxt_client(self):
        self.exchange._ccxt_client.close = AsyncMock()
        await self.exchange.stop_network()
        self.exchange._ccxt_client.close.assert_awaited_once()

    # ---- order parameters ------------------------------------------------------------------------------------
    async def _place_and_capture(self, mock_api, trade_type, order_type, position_action, price=Decimal("100")):
        bodies = []
        mock_api.post(self.order_creation_url, body=json.dumps(self.order_creation_request_successful_mock_response),
                      callback=lambda url, **kwargs: bodies.append(json.loads(kwargs["data"])))
        exchange_order_id, _ = await self.exchange._place_order(
            "HBOT-1", self.trading_pair, Decimal("1"), trade_type, order_type, price, position_action)
        self.assertEqual(self.exchange_order_id, exchange_order_id)
        return bodies[0]

    @aioresponses()
    async def test_place_market_order_sends_a_market_type_without_price(self, mock_api):
        body = await self._place_and_capture(mock_api, TradeType.SELL, OrderType.MARKET, PositionAction.OPEN,
                                             Decimal("NaN"))
        self.assertEqual(6, body["type"])  # MEXC: market order converted to the current price (ccxt default)
        self.assertNotIn("price", body)
        self.assertEqual(3, body["side"])

    @aioresponses()
    async def test_place_limit_maker_order_is_post_only(self, mock_api):
        body = await self._place_and_capture(mock_api, TradeType.BUY, OrderType.LIMIT_MAKER, PositionAction.OPEN)
        self.assertEqual(2, body["type"])

    @aioresponses()
    async def test_closing_orders_use_the_close_sides_and_no_reduce_only_flag(self, mock_api):
        for trade_type, expected_side in [(TradeType.BUY, 2), (TradeType.SELL, 4)]:
            body = await self._place_and_capture(mock_api, trade_type, OrderType.LIMIT, PositionAction.CLOSE)
            self.assertEqual(expected_side, body["side"])
            self.assertNotIn("reduceOnly", body)

    @aioresponses()
    async def test_hedge_mode_orders_send_the_position_mode(self, mock_api):
        self.exchange._perpetual_trading.set_position_mode(PositionMode.HEDGE)
        body = await self._place_and_capture(mock_api, TradeType.SELL, OrderType.LIMIT, PositionAction.CLOSE)
        self.assertEqual(1, body["positionMode"])  # hedge
        self.assertEqual(4, body["side"])  # close long

    def test_order_state_mapping(self):
        cases = [("open", 0, OrderState.OPEN), ("open", 3, OrderState.PARTIALLY_FILLED),
                 ("closed", 10, OrderState.FILLED), ("canceled", 0, OrderState.CANCELED),
                 ("canceled", 3, OrderState.CANCELED), ("rejected", 0, OrderState.FAILED),
                 ("expired", 0, OrderState.CANCELED), ("1", 0, OrderState.OPEN)]  # MEXC "1": not yet informed
        for status, filled, expected in cases:
            with self.subTest(status=status, filled=filled):
                self.assertEqual(expected, self.exchange._order_state_from_ccxt(status, filled))

    def test_error_classification(self):
        self.assertTrue(self.exchange._is_request_exception_related_to_time_synchronizer(ccxt.InvalidNonce("t")))
        self.assertFalse(self.exchange._is_request_exception_related_to_time_synchronizer(ccxt.NetworkError("x")))
        for check in (self.exchange._is_order_not_found_during_cancelation_error,
                      self.exchange._is_order_not_found_during_status_update_error):
            self.assertTrue(check(ccxt.OrderNotFound("gone")))
            self.assertTrue(check(ccxt.InvalidOrder("mexc ... order not exist")))
            self.assertFalse(check(ccxt.NetworkError("down")))

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
        self._mock_position_mode(mock_api, hedged=True)
        await self.exchange._check_account_position_mode()
        self.assertTrue(self.is_logged(
            "ERROR", "The Mexc_perpetual account is in hedge mode but this connector supports only one-way mode. "
                     "Switch the account to one-way mode, orders may be rejected or open the wrong position."))

    @aioresponses()
    async def test_an_account_in_one_way_mode_is_not_reported(self, mock_api):
        self._mock_position_mode(mock_api, hedged=False)
        await self.exchange._check_account_position_mode()
        self.assertFalse(any(record.levelname == "ERROR" for record in self.log_records))

    def _mock_position_mode(self, mock_api, hedged: bool):
        mock_api.get(url_regex("private/position/position_mode"),
                     body=json.dumps({"success": True, "code": 0, "data": 1 if hedged else 2}))

    # ---- hardening found by review ---------------------------------------------------------------------------
    @aioresponses()
    async def test_an_order_is_recovered_by_its_client_order_id(self, mock_api):
        order = self._order_data(self._tracked_order(), 2)
        mock_api.get(url_regex("private/order/external/"), body=json.dumps({"success": True, "code": 0, "data": order}))

        found = await self.exchange._fetch_order_by_client_id(self.ccxt_symbol, "HBOT-1")

        self.assertEqual(self.exchange_order_id, found["id"])
        self.assertEqual("open", found["status"])

    @aioresponses()
    async def test_an_unknown_client_order_id_is_not_found(self, mock_api):
        mock_api.get(url_regex("private/order/external/"), body=json.dumps({"success": True, "code": 0, "data": None}))
        with self.assertRaises(ccxt.OrderNotFound):
            await self.exchange._fetch_order_by_client_id(self.ccxt_symbol, "HBOT-1")

    @aioresponses()
    async def test_a_lost_submission_response_is_recovered_without_sending_the_order_twice(self, mock_api):
        self.exchange._sleep = AsyncMock()
        mock_api.post(self.order_creation_url, exception=asyncio.TimeoutError())
        order = self._order_data(self._tracked_order(), 2)
        mock_api.get(url_regex("private/order/external/"), body=json.dumps({"success": True, "code": 0, "data": order}))

        exchange_order_id, _ = await self.exchange._place_order(
            "HBOT-1", self.trading_pair, Decimal("1"), TradeType.BUY, OrderType.LIMIT, Decimal("100"),
            PositionAction.OPEN)

        self.assertEqual(self.exchange_order_id, exchange_order_id)
        self.assertEqual(1, len(self._all_executed_requests(mock_api, self.order_creation_url)))

    def test_the_stream_balance_uses_the_same_wallet_balance_as_the_rest_one(self):
        """push.personal.asset has no cashBalance: available + frozen + position margin is the same number."""
        stream = {"info": {"currency": self.quote_asset, "availableBalance": 10, "frozenBalance": 2,
                           "positionMargin": 3, "bonus": 0}, self.quote_asset: {"free": 10.0, "used": 2.0, "total": 12.0}}
        rest = self.balance_request_mock_response_for_base_and_quote["data"]
        total, available = self.exchange._parse_balances(stream)
        self.assertEqual(({self.quote_asset: Decimal("15")}, {self.quote_asset: Decimal("10")}), (total, available))
        total, _ = self.exchange._parse_balances({"info": {"data": rest}})
        self.assertEqual(Decimal("15"), total[self.base_asset])  # cashBalance of the same fixture

    def test_unrealized_pnl_is_read_from_the_raw_position_when_ccxt_has_none(self):
        for info, expected in [({"unRealizedPnl": "1.5"}, Decimal("1.5")), ({"unrealizedPnl": 2}, Decimal("2")),
                               ({}, None)]:
            with self.subTest(info=info):
                self.assertEqual(expected, self.exchange._exchange_unrealized_pnl({"unrealizedPnl": None, "info": info}))

    def _tracked_order(self) -> InFlightOrder:
        self.exchange.start_tracking_order(
            order_id="HBOT-1", exchange_order_id=self.exchange_order_id, trading_pair=self.trading_pair,
            trade_type=TradeType.BUY, price=Decimal("10000"), amount=Decimal("1"), order_type=OrderType.LIMIT,
            position_action=PositionAction.OPEN)
        return self.exchange.in_flight_orders["HBOT-1"]

    @aioresponses()
    async def test_a_perpetual_connector_never_touches_the_spot_api(self, mock_api):
        """The balance command updates balances on a connector that has not loaded its markets yet."""
        exchange = self.create_exchange_instance()
        try:
            mock_api.get(self.all_symbols_url, body=json.dumps(self.all_symbols_request_mock_response))
            mock_api.get(self.balance_url, body=json.dumps(self.balance_request_mock_response_for_base_and_quote))

            await exchange._update_balances()

            hosts = {key[1].host for key in mock_api.requests}
            self.assertEqual({"contract.mexc.com"}, hosts)  # no api.mexc.com, no private spot currencies call
            self.assertEqual(Decimal("2000"), exchange.get_balance(self.quote_asset))
        finally:
            await exchange._ccxt_client.close()
