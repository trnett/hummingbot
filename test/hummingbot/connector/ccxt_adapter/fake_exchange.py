"""A tiny in-memory exchange speaking the ccxt methods the connectors use, for rehearsing the live checklist."""
import asyncio
import time
from typing import Any, Dict, List, Optional

import ccxt

SYMBOL = "ETH/USDT:USDT"
FEE_RATE = 0.0004


class FakeExchange:
    """One market, a static order book, market orders fill at once, limit orders rest. No private streams."""

    def __init__(self, bid: float = 2000.0, ask: float = 2000.1, wallet: float = 1000.0):
        self.bid, self.ask, self.wallet = bid, ask, wallet
        self.id = "fake"
        self.precisionMode = ccxt.TICK_SIZE
        self.has = {"watchOrders": False, "watchMyTrades": False, "watchBalance": False, "watchPositions": False,
                    "watchOrderBook": False, "fetchOrderTrades": False}
        self.orders: Dict[str, Dict[str, Any]] = {}
        self.trades: List[Dict[str, Any]] = []
        self.contracts = 0.0  # signed net position
        self.entry = 0.0
        self.calls: List[str] = []
        self._next_id = 1000
        self.market = {
            "id": "ETHUSDT", "symbol": SYMBOL, "base": "ETH", "quote": "USDT", "settle": "USDT", "type": "swap",
            "swap": True, "linear": True, "inverse": False, "active": True, "contract": True, "contractSize": 1.0,
            "precision": {"amount": 0.01, "price": 0.05},
            "limits": {"amount": {"min": 0.01, "max": 1000}, "cost": {"min": 5}},
        }

    def milliseconds(self) -> int:  # replaced by the connector
        return int(time.time() * 1000)

    # ---- public ----------------------------------------------------------------------------------------------
    async def load_markets(self, reload: bool = False):
        return {SYMBOL: self.market}

    async def fetch_time(self):
        return int(time.time() * 1000)

    async def fetch_order_book(self, symbol, limit=None, params={}):
        return {"bids": [[self.bid, 50.0]], "asks": [[self.ask, 50.0]], "timestamp": int(time.time() * 1000),
                "nonce": None}

    async def fetch_funding_rate(self, symbol, params={}):
        return {"symbol": SYMBOL, "markPrice": (self.bid + self.ask) / 2, "indexPrice": (self.bid + self.ask) / 2,
                "fundingRate": 0.0001, "fundingTimestamp": None,
                "nextFundingTimestamp": int(time.time() * 1000) + 3_600_000}

    async def fetch_funding_history(self, symbol=None, since=None, limit=None, params={}):
        return []

    async def fetch_ticker(self, symbol, params={}):
        return {"symbol": SYMBOL, "last": (self.bid + self.ask) / 2}

    # ---- private ---------------------------------------------------------------------------------------------
    async def fetch_balance(self, params={}):
        return {"USDT": {"free": self.wallet, "used": 0.0, "total": self.wallet}, "info": {}}

    async def fetch_positions(self, symbols=None, params={}):
        if self.contracts == 0:
            return []
        return [{"symbol": SYMBOL, "side": "long" if self.contracts > 0 else "short", "contracts": abs(self.contracts),
                 "contractSize": 1.0, "entryPrice": self.entry, "unrealizedPnl": 0.0, "leverage": 1.0}]

    async def fetch_position_mode(self, symbol=None, params={}):
        return {"hedged": False}

    async def set_leverage(self, leverage, symbol=None, params={}):
        self.calls.append(f"set_leverage {leverage}")
        return {}

    async def create_order(self, symbol, type, side, amount, price=None, params={}):
        self.calls.append(f"create_order {type} {side} {amount} {price} {params}")
        order_id = str(self._next_id)
        self._next_id += 1
        order = {"id": order_id, "clientOrderId": params.get("clientOrderId"), "symbol": SYMBOL, "type": type,
                 "side": side, "amount": amount, "price": price, "filled": 0.0, "status": "open",
                 "timestamp": int(time.time() * 1000), "lastUpdateTimestamp": int(time.time() * 1000),
                 "reduceOnly": bool(params.get("reduceOnly"))}
        self.orders[order_id] = order
        if type == "market":
            self._fill(order, self.ask if side == "buy" else self.bid)
        return {"id": order_id}

    def _fill(self, order: Dict[str, Any], price: float):
        order.update(filled=order["amount"], status="closed", lastUpdateTimestamp=int(time.time() * 1000))
        signed = order["amount"] if order["side"] == "buy" else -order["amount"]
        if order["reduceOnly"] and abs(signed) > abs(self.contracts):
            signed = -self.contracts
        self.contracts = round(self.contracts + signed, 8)
        self.entry = price if self.contracts != 0 else 0.0
        fee = round(order["amount"] * price * FEE_RATE, 8)
        self.wallet -= fee
        self.trades.append({"id": str(len(self.trades) + 1), "order": order["id"], "timestamp": order["timestamp"],
                            "side": order["side"], "price": price, "amount": order["amount"],
                            "fee": {"currency": "USDT", "cost": fee}})

    async def fetch_order(self, id=None, symbol=None, params={}):
        client_id = params.get("clientOrderId")
        for order in self.orders.values():
            if order["id"] == id or (client_id and order["clientOrderId"] == client_id):
                return dict(order)
        raise ccxt.OrderNotFound("order does not exist")

    async def fetch_open_orders(self, symbol=None, since=None, limit=None, params={}):
        return [dict(o) for o in self.orders.values() if o["status"] == "open"]

    async def cancel_order(self, id, symbol=None, params={}):
        order = self.orders.get(str(id))
        if order is None or order["status"] != "open":
            raise ccxt.OrderNotFound("order does not exist")
        order["status"] = "canceled"
        return dict(order)

    async def fetch_my_trades(self, symbol=None, since=None, limit=None, params={}):
        return [dict(t) for t in self.trades if since is None or t["timestamp"] >= since][:limit]

    async def close(self):
        await asyncio.sleep(0)

    def set_markets(self, markets):
        return markets

    def parse_order(self, data, market=None):
        return data

    def contract_size(self) -> Optional[float]:
        return 1.0
