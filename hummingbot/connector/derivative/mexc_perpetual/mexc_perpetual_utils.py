from decimal import Decimal

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.connector.ccxt_adapter.ccxt_utils import (  # noqa: F401
    ccxt_symbol_to_trading_pair,
    is_exchange_information_valid,
    trading_pair_to_ccxt_symbol,
)
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0"),
    taker_percent_fee_decimal=Decimal("0.0002"),
    buy_percent_fee_deducted_from_returns=True
)

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-USDT"


class MexcPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "mexc_perpetual"
    mexc_perpetual_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your MEXC Perpetual API key",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    mexc_perpetual_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your MEXC Perpetual API secret",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    model_config = ConfigDict(title="mexc_perpetual")


KEYS = MexcPerpetualConfigMap.model_construct()
