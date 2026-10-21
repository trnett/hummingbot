"""
Rate limiting of the ccxt adapter happens per HTTP request (one ccxt method may send several), through Hummingbot's
AsyncThrottler with the limits the exchange documents, and a rate limit answer (HTTP 429 / 418) pauses every request
of the connector for a while instead of being repeated.
"""
import asyncio
import time
from test.isolated_asyncio_wrapper_test_case import IsolatedAsyncioWrapperTestCase
from typing import Any, Dict, List
from unittest.mock import patch

import ccxt

from hummingbot.connector.ccxt_adapter.ccxt_throttle import HummingbotThrottled, throttled
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit


class FakeExchange:
    """Stands for a ccxt exchange: ``fetch2`` is the method that sends one HTTP request."""

    def __init__(self):
        self.requests: List[Dict[str, Any]] = []
        self.failures: List[Exception] = []
        self.last_response_headers: Dict[str, str] = {}

    async def fetch2(self, path, api="public", method="GET", params={}, headers=None, body=None, config={}):
        self.requests.append({"path": path, "api": api, "method": method, "params": params, "at": time.monotonic()})
        if self.failures:
            raise self.failures.pop(0)
        return {"ok": path}

    async def two_requests_method(self):
        return [await self.fetch2("a"), await self.fetch2("b")]


ThrottledFake = throttled(FakeExchange)


def new_client(limits=None, resolver=None):
    client = ThrottledFake()
    client.BACKOFF_BASE = 0.05
    client.BACKOFF_MAX = 0.4
    client.BAN_BACKOFF = 0.3
    limits = limits or [RateLimit("default", 1000, 1)]
    client.hb_throttler = AsyncThrottler(limits, retry_interval=0.01)
    client.hb_limit_id_for = resolver or (lambda path, api, method, params: "default")
    return client


class CcxtThrottleTests(IsolatedAsyncioWrapperTestCase):
    def test_throttled_is_a_cached_subclass_of_the_exchange_with_the_mixin_first(self):
        self.assertTrue(issubclass(ThrottledFake, HummingbotThrottled))
        self.assertTrue(issubclass(ThrottledFake, FakeExchange))
        self.assertIs(ThrottledFake, throttled(FakeExchange))
        self.assertLess(ThrottledFake.__mro__.index(HummingbotThrottled), ThrottledFake.__mro__.index(FakeExchange))

    async def test_without_a_throttler_requests_pass_through(self):
        client = ThrottledFake()
        self.assertEqual({"ok": "x"}, await client.fetch2("x"))

    async def test_every_http_request_acquires_the_throttler_with_its_limit_id(self):
        client = new_client(resolver=lambda path, api, method, params: f"{method} {path}")
        client.hb_throttler.set_rate_limits([RateLimit("GET a", 100, 1), RateLimit("GET b", 100, 1)])
        with patch.object(client.hb_throttler, "execute_task", wraps=client.hb_throttler.execute_task) as spy:
            await client.two_requests_method()
        self.assertEqual(["GET a", "GET b"], [call.args[0] if call.args else call.kwargs["limit_id"]
                                              for call in spy.call_args_list])

    async def test_the_resolver_sees_path_api_method_and_parameters(self):
        seen = []
        client = new_client(resolver=lambda *args: seen.append(args) or "default")
        await client.fetch2("v1/depth", "fapiPublic", "GET", {"limit": 5})
        self.assertEqual([("v1/depth", "fapiPublic", "GET", {"limit": 5})], seen)

    async def test_requests_wait_when_the_limit_is_used_up(self):
        client = new_client(limits=[RateLimit("default", 2, 0.5)])
        started = time.monotonic()
        for _ in range(3):
            await client.fetch2("x")
        self.assertGreaterEqual(time.monotonic() - started, 0.4)

    async def test_linked_limits_with_weights_are_honoured(self):
        limits = [RateLimit("weight", 10, 0.5),
                  RateLimit("heavy", 100, 0.5, linked_limits=[LinkedLimitWeightPair("weight", 5)])]
        client = new_client(limits=limits, resolver=lambda *args: "heavy")
        started = time.monotonic()
        for _ in range(3):  # 3 x weight 5 = 15 > 10 in half a second
            await client.fetch2("x")
        self.assertGreaterEqual(time.monotonic() - started, 0.3)

    # ---- rate limit answers ---------------------------------------------------------------------------------
    async def test_a_rate_limit_answer_pauses_the_next_request(self):
        client = new_client()
        client.failures = [ccxt.RateLimitExceeded("fake GET https://x 429 Too Many Requests")]
        with self.assertRaises(ccxt.RateLimitExceeded):
            await client.fetch2("x")
        failed_at = client.requests[-1]["at"]

        await client.fetch2("y")

        self.assertGreaterEqual(client.requests[-1]["at"] - failed_at, client.BACKOFF_BASE * 0.9)

    async def test_consecutive_rate_limit_answers_double_the_pause_up_to_the_maximum(self):
        client = new_client()
        pauses = []
        for _ in range(5):
            client.failures = [ccxt.RateLimitExceeded("fake GET https://x 429")]
            with self.assertRaises(ccxt.RateLimitExceeded):
                await client.fetch2("x")  # first waits out the pause of the previous answer
            pauses.append(client._hb_pause_until - time.monotonic())
        self.assertTrue(all(later >= earlier - 0.02 for earlier, later in zip(pauses, pauses[1:])), pauses)
        self.assertAlmostEqual(client.BACKOFF_BASE, pauses[0], delta=0.02)
        self.assertAlmostEqual(client.BACKOFF_BASE * 2, pauses[1], delta=0.02)
        self.assertLessEqual(max(pauses), client.BACKOFF_MAX + 0.02)

    async def test_a_successful_request_resets_the_pause(self):
        client = new_client()
        for _ in range(2):
            client.failures = [ccxt.RateLimitExceeded("fake GET https://x 429")]
            with self.assertRaises(ccxt.RateLimitExceeded):
                await client.fetch2("x")
        await client.fetch2("ok")
        client.failures = [ccxt.RateLimitExceeded("fake GET https://x 429")]
        with self.assertRaises(ccxt.RateLimitExceeded):
            await client.fetch2("x")
        self.assertAlmostEqual(client.BACKOFF_BASE, client._hb_pause_until - time.monotonic(), delta=0.02)

    async def test_an_ip_ban_answer_pauses_for_the_ban_duration(self):
        client = new_client()
        client.failures = [ccxt.DDoSProtection("fake GET https://x 418 I'm a Teapot")]
        before = time.monotonic()
        with self.assertRaises(ccxt.DDoSProtection):
            await client.fetch2("x")
        self.assertGreaterEqual(client._hb_pause_until - before, client.BAN_BACKOFF * 0.95)

    async def test_retry_after_header_is_used_when_the_exchange_sends_it(self):
        client = new_client()
        client.last_response_headers = {"Retry-After": "0.25"}
        client.failures = [ccxt.RateLimitExceeded("fake GET https://x 429")]
        before = time.monotonic()
        with self.assertRaises(ccxt.RateLimitExceeded):
            await client.fetch2("x")
        self.assertAlmostEqual(0.25, client._hb_pause_until - before, delta=0.03)

    async def test_the_pause_covers_every_request_of_the_connector_and_nothing_is_sent_during_it(self):
        client = new_client()
        client.failures = [ccxt.RateLimitExceeded("fake GET https://x 429")]
        with self.assertRaises(ccxt.RateLimitExceeded):
            await client.fetch2("x")
        sent_before = len(client.requests)

        task = asyncio.ensure_future(client.fetch2("other", "private", "POST"))
        await asyncio.sleep(client.BACKOFF_BASE / 3)
        self.assertEqual(sent_before, len(client.requests))  # still paused
        await task
        self.assertEqual(sent_before + 1, len(client.requests))

    async def test_other_errors_do_not_pause_requests(self):
        client = new_client()
        for error in (ccxt.NetworkError("down"), ccxt.ExchangeError("bad"), ccxt.InvalidNonce("nonce")):
            client.failures = [error]
            with self.assertRaises(type(error)):
                await client.fetch2("x")
        before = time.monotonic()
        await client.fetch2("y")
        self.assertLess(time.monotonic() - before, 0.03)

    async def test_the_pause_is_logged_as_a_warning(self):
        client = new_client()
        client.failures = [ccxt.RateLimitExceeded("fake GET https://x 429")]
        with self.assertLogs(HummingbotThrottled.hb_logger(), level="WARNING") as logs:
            with self.assertRaises(ccxt.RateLimitExceeded):
                await client.fetch2("x")
        self.assertIn("rate limit", logs.output[0].lower())
        self.assertIn("pausing", logs.output[0].lower())

    async def test_the_signature_of_the_failed_request_is_not_logged(self):
        client = new_client()
        client.failures = [ccxt.RateLimitExceeded("fake GET https://x?a=1&signature=0xdeadbeef 429")]
        with self.assertLogs(HummingbotThrottled.hb_logger(), level="WARNING") as logs:
            with self.assertRaises(ccxt.RateLimitExceeded):
                await client.fetch2("x")
        self.assertNotIn("deadbeef", "".join(logs.output))
