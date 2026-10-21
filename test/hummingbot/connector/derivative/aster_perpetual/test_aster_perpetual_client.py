"""
Aster API V3 request signing of the ccxt client used by the Aster perpetual connector.

V1 (api key + HMAC secret) is gone. V3 signs every private request with the private key of an API wallet (EIP-712)
and identifies the account with the master wallet address (``user``) and the API wallet address (``signer``).
"""
import unittest
from urllib.parse import parse_qsl

from eth_account import Account
from eth_account.messages import encode_typed_data

from hummingbot.connector.derivative.aster_perpetual.aster_perpetual_client import AsterV3Exchange

USER = "0x63DD5aCC6b1aa0f563956C0e534DD30B6dcF7C4e"
SIGNER_ACCOUNT = Account.from_key("0x" + "11" * 32)  # throwaway key, only used by these tests
SIGNER = SIGNER_ACCOUNT.address
TYPED_DATA = {
    "types": {
        "EIP712Domain": [
            {"name": "name", "type": "string"},
            {"name": "version", "type": "string"},
            {"name": "chainId", "type": "uint256"},
            {"name": "verifyingContract", "type": "address"},
        ],
        "Message": [{"name": "msg", "type": "string"}],
    },
    "primaryType": "Message",
    "domain": {
        "name": "AsterSignTransaction", "version": "1", "chainId": 1666,
        "verifyingContract": "0x0000000000000000000000000000000000000000"},
    "message": {"msg": ""},
}


def new_client(**overrides) -> AsterV3Exchange:
    config = {"walletAddress": USER, "privateKey": SIGNER_ACCOUNT.key.hex(), "options": {"signerAddress": SIGNER}}
    config.update(overrides)
    return AsterV3Exchange(config)


def recover_signer(message: str, signature: str) -> str:
    typed_data = {**TYPED_DATA, "message": {"msg": message}}
    return Account.recover_message(encode_typed_data(full_message=typed_data), signature=signature)


class AsterV3ExchangeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = new_client()
        self.client.milliseconds = lambda: 1_700_000_000_123

    def sign(self, path="v1/order", api="fapiPrivate", method="POST", params=None):
        return self.client.sign(path, api, method, params or {})

    def test_every_version_of_a_path_goes_to_v3(self):
        for path, expected in (("v1/order", "v3/order"), ("v2/positionRisk", "v3/positionRisk"),
                               ("v4/account", "v3/account"), ("v1/positionSide/dual", "v3/positionSide/dual"),
                               ("v1/leverageBracket", "v3/leverageBrackets"), ("v3/balance", "v3/balance")):
            request = self.sign(path=path, method="GET")
            self.assertTrue(request["url"].startswith(f"https://fapi.asterdex.com/fapi/{expected}?"), request["url"])

    def test_public_requests_are_not_signed(self):
        request = self.sign(path="v1/depth", api="fapiPublic", method="GET", params={"symbol": "BTCUSDT", "limit": 5})
        self.assertEqual("https://fapi.asterdex.com/fapi/v3/depth?symbol=BTCUSDT&limit=5", request["url"])
        self.assertIsNone(request["body"])
        self.assertNotIn("signature", request["url"])

    def test_get_request_is_signed_in_the_query_string(self):
        request = self.sign(path="v4/account", method="GET")
        query = request["url"].split("?", 1)[1]
        fields = dict(parse_qsl(query))
        self.assertEqual({"nonce", "user", "signer", "signature"}, set(fields))
        self.assertIsNone(request["body"])
        self.assertEqual(USER, fields["user"])
        self.assertEqual(SIGNER, fields["signer"])
        unsigned, signature = query.rsplit("&signature=", 1)
        self.assertEqual(SIGNER, recover_signer(unsigned, signature))

    def test_post_request_is_signed_in_a_form_body(self):
        request = self.sign(params={"symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT", "quantity": 0.5})
        self.assertEqual("https://fapi.asterdex.com/fapi/v3/order", request["url"])
        self.assertEqual("application/x-www-form-urlencoded", request["headers"]["Content-Type"])
        fields = dict(parse_qsl(request["body"]))
        self.assertEqual("0.5", fields["quantity"])

    def test_signature_is_the_eip712_signature_of_the_unsigned_parameters_by_the_api_wallet(self):
        request = self.sign(params={"symbol": "BTCUSDT", "side": "BUY", "type": "LIMIT", "price": "100.5"})
        unsigned, signature = request["body"].rsplit("&signature=", 1)
        self.assertTrue(signature.startswith("0x") and len(signature) == 132, signature)
        self.assertEqual(SIGNER, recover_signer(unsigned, signature))

    def test_signed_parameters_keep_the_order_they_are_sent_in(self):
        request = self.sign(params={"symbol": "BTCUSDT", "side": "SELL"})
        names = [name for name, _ in parse_qsl(request["body"])]
        self.assertEqual(["symbol", "side", "nonce", "user", "signer", "signature"], names)

    def test_booleans_and_special_characters_are_encoded_once_and_signed_as_sent(self):
        request = self.sign(params={"reduceOnly": True, "newClientOrderId": "x-a&b c"})
        unsigned, signature = request["body"].rsplit("&signature=", 1)
        fields = dict(parse_qsl(unsigned))
        self.assertEqual("true", fields["reduceOnly"])
        self.assertEqual("x-a&b c", fields["newClientOrderId"])
        self.assertEqual(SIGNER, recover_signer(unsigned, signature))

    def test_ccxt_time_parameters_of_v1_are_not_sent(self):
        request = self.sign(path="v4/account", method="GET", params={"timestamp": 1, "recvWindow": 10000})
        fields = dict(parse_qsl(request["url"].split("?", 1)[1]))
        self.assertNotIn("timestamp", fields)
        self.assertNotIn("recvWindow", fields)

    def test_nonce_is_in_microseconds_of_the_synchronized_time(self):
        request = self.sign()
        self.assertEqual(1_700_000_000_123_000, int(dict(parse_qsl(request["body"]))["nonce"]))

    def test_nonce_never_repeats_inside_the_same_millisecond(self):
        nonces = [int(dict(parse_qsl(self.sign()["body"]))["nonce"]) for _ in range(5)]
        self.assertEqual(sorted(set(nonces)), nonces)

    def test_nonce_never_goes_back_when_the_clock_does(self):
        first = int(dict(parse_qsl(self.sign()["body"]))["nonce"])
        self.client.milliseconds = lambda: 1_700_000_000_000
        second = int(dict(parse_qsl(self.sign()["body"]))["nonce"])
        self.assertGreater(second, first)

    def test_private_key_without_the_0x_prefix_is_accepted(self):
        client = new_client(privateKey=SIGNER_ACCOUNT.key.hex().removeprefix("0x"))
        client.milliseconds = lambda: 1_700_000_000_123
        request = client.sign("v1/order", "fapiPrivate", "POST", {"symbol": "BTCUSDT"})
        unsigned, signature = request["body"].rsplit("&signature=", 1)
        self.assertEqual(SIGNER, recover_signer(unsigned, signature))

    def test_missing_credentials_are_reported_before_anything_is_sent(self):
        for missing in ("walletAddress", "privateKey"):
            client = new_client(**{missing: None})
            with self.assertRaises(Exception) as context:
                client.sign("v1/order", "fapiPrivate", "POST", {})
            self.assertIn(missing, str(context.exception))
        client = new_client(options={})
        with self.assertRaises(Exception) as context:
            client.sign("v1/order", "fapiPrivate", "POST", {})
        self.assertIn("signerAddress", str(context.exception))

    def test_spot_hosts_are_never_called(self):
        for api in ("sapiPublic", "sapiPrivate"):
            with self.assertRaises(Exception):
                self.client.sign("v1/exchangeInfo", api, "GET", {})

    def test_the_private_key_is_never_part_of_a_request_or_an_error(self):
        request = self.sign(params={"symbol": "BTCUSDT"})
        self.assertNotIn(SIGNER_ACCOUNT.key.hex().removeprefix("0x"), str(request))
