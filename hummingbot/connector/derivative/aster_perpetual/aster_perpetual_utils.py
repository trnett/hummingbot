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
    maker_percent_fee_decimal=Decimal("0.0001"),
    taker_percent_fee_decimal=Decimal("0.00035"),
    buy_percent_fee_deducted_from_returns=True
)

CENTRALIZED = True

EXAMPLE_PAIR = "BTC-USDT"


class AsterPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "aster_perpetual"
    aster_perpetual_user: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aster main wallet address (the account that owns the API wallet)",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    aster_perpetual_signer: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter your Aster API wallet address (create it on asterdex.com/en/api-wallet, Pro API)",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    aster_perpetual_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": "Enter the private key of your Aster API wallet (not of your main wallet)",
            "is_secure": True, "is_connect_key": True, "prompt_on_new": True}
    )
    model_config = ConfigDict(title="aster_perpetual")


KEYS = AsterPerpetualConfigMap.model_construct()
