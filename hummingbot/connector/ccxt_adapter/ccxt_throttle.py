import asyncio
import logging
import time
from contextlib import nullcontext
from typing import Any, Callable, Dict, Optional, Type

import ccxt

from hummingbot.connector.ccxt_adapter import ccxt_utils
from hummingbot.core.api_throttler.async_throttler_base import AsyncThrottlerBase
from hummingbot.logger.logger import HummingbotLogger


class HummingbotThrottled:
    """
    Mixin of a ccxt exchange class that rate limits every HTTP request (``fetch2``) with Hummingbot's throttler, with
    the limits the exchange documents, instead of ccxt's own rate limiter (disabled). One ccxt method may send several
    requests, each of them is paid for.

    A rate limit answer (HTTP 429 or the ban 418) pauses all the requests of the connector for a while, doubling for
    every consecutive answer, instead of being hammered again. Nothing is sent during the pause, so no order is lost
    or duplicated by it: the requests only wait.
    """

    BACKOFF_BASE = 5.0    # seconds paused after the first rate limit answer
    BACKOFF_MAX = 120.0   # longest pause caused by repeated rate limit answers
    BAN_BACKOFF = 120.0   # shortest pause after an IP ban (HTTP 418), the exchanges ban from 2 minutes up

    hb_throttler: Optional[AsyncThrottlerBase] = None
    # (path, api, method, params) of a ccxt request -> id of the RateLimit that request is charged to
    hb_limit_id_for: Optional[Callable[[str, Any, str, Dict[str, Any]], str]] = None

    _logger: Optional[HummingbotLogger] = None
    _hb_pause_until: float = 0.0
    _hb_consecutive_rate_limits: int = 0

    @classmethod
    def hb_logger(cls) -> HummingbotLogger:
        if HummingbotThrottled._logger is None:
            HummingbotThrottled._logger = logging.getLogger(__name__)
        return HummingbotThrottled._logger

    async def fetch2(self, path, api="public", method="GET", params={}, headers=None, body=None, config={}):
        await self._hb_wait_out_pause()
        throttler, resolver = self.hb_throttler, self.hb_limit_id_for
        context = throttler.execute_task(resolver(path, api, method, params)) \
            if throttler is not None and resolver is not None else nullcontext()
        async with context:
            try:
                response = await super().fetch2(path, api, method, params, headers, body, config)
            except (ccxt.RateLimitExceeded, ccxt.DDoSProtection) as error:  # HTTP 429, HTTP 418 (ban), exchange codes
                self._hb_start_pause(error)
                raise
        self._hb_consecutive_rate_limits = 0
        return response

    async def _hb_wait_out_pause(self):
        while True:
            remaining = self._hb_pause_until - time.monotonic()
            if remaining <= 0:
                return
            await asyncio.sleep(remaining)

    def _hb_start_pause(self, error: ccxt.NetworkError):
        self._hb_consecutive_rate_limits += 1
        message = ccxt_utils.sanitize_error_message(str(error))
        pause = min(self.BACKOFF_MAX, self.BACKOFF_BASE * 2 ** (self._hb_consecutive_rate_limits - 1))
        if isinstance(error, ccxt.DDoSProtection) or " 418 " in message:
            pause = max(pause, self.BAN_BACKOFF)
        retry_after = self._hb_retry_after()
        if retry_after is not None:
            pause = retry_after
        self._hb_pause_until = max(self._hb_pause_until, time.monotonic() + pause)
        self.hb_logger().warning(f"The exchange answered that its rate limit was exceeded ({message}). "
                                 f"Pausing every request to it for {pause:.1f} seconds.")

    def _hb_retry_after(self) -> Optional[float]:
        headers = getattr(self, "last_response_headers", None) or {}
        value = next((headers[key] for key in headers if str(key).lower() == "retry-after"), None)
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None


_throttled_classes: Dict[Type, Type] = {}


def throttled(exchange_class: Type) -> Type:
    """The ccxt exchange class with Hummingbot's per request throttling and rate limit pauses (one class per exchange)."""
    if exchange_class not in _throttled_classes:
        _throttled_classes[exchange_class] = type(
            f"Throttled{exchange_class.__name__}", (HummingbotThrottled, exchange_class), {})
    return _throttled_classes[exchange_class]
