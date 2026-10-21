from typing import Any, Dict, Optional
from urllib.parse import urlencode

import ccxt
import ccxt.pro as ccxt_pro
from eth_account import Account
from eth_account.messages import encode_typed_data

from hummingbot.connector.derivative.aster_perpetual import aster_perpetual_constants as CONSTANTS

# Aster API V3 EIP-712 envelope: the signed message is the url encoded parameter string
_TYPED_DATA = {
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
        "name": "AsterSignTransaction",
        "version": "1",
        "chainId": CONSTANTS.SIGNATURE_CHAIN_ID,
        "verifyingContract": "0x0000000000000000000000000000000000000000",
    },
}

# parameters ccxt adds for the V1 API, V3 has the nonce instead
_V1_ONLY_PARAMETERS = ("timestamp", "recvWindow")


class AsterV3Exchange(ccxt_pro.aster):
    """
    ccxt's Aster client speaking Aster API V3. V1 (api key + HMAC) is discontinued; ccxt 4.5.32 knows V3 for three
    endpoints only, so every ccxt call (written for V1 paths) is redirected to its V3 path here and signed the way the
    V3 documentation describes: EIP-712 signature of the url encoded parameters by the API wallet (``signer``) of the
    master wallet (``user``).

    Credentials: ``walletAddress`` is the master wallet, ``privateKey`` the one of the API wallet and
    ``options["signerAddress"]`` the address of the API wallet.
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self._last_nonce = 0

    def sign(self, path, api="public", method="GET", params={}, headers=None, body=None):
        if api in ("sapiPublic", "sapiPrivate"):
            raise ccxt.NotSupported(f"{self.id}: spot endpoints are not used by a perpetual connector ({path})")
        path = CONSTANTS.v3_path(path)
        url = f"{self.implode_hostname(self.urls['api'][api])}/{path}"
        if api == "fapiPublic":
            query = self._encode(params)
            return {"url": f"{url}?{query}" if query else url, "method": method, "body": None, "headers": headers}
        wallet, signer, private_key = self._credentials()
        parameters = {key: value for key, value in params.items() if key not in _V1_ONLY_PARAMETERS}
        parameters.update({"nonce": self._next_nonce(), "user": wallet, "signer": signer})
        message = self._encode(parameters)
        signed = f"{message}&signature={self._signature(message, private_key)}"
        if method == "GET":
            return {"url": f"{url}?{signed}", "method": method, "body": None, "headers": headers}
        return {"url": url, "method": method, "body": signed,
                "headers": {"Content-Type": "application/x-www-form-urlencoded"}}

    def _credentials(self):
        for name, value in (("walletAddress", self.walletAddress), ("privateKey", self.privateKey),
                            ("signerAddress", (self.options or {}).get("signerAddress"))):
            if not value:
                raise ccxt.AuthenticationError(f"{self.id} requires {name} to sign a request")
        key = self.privateKey if self.privateKey.startswith("0x") else f"0x{self.privateKey}"
        return self.walletAddress, self.options["signerAddress"], key

    def _next_nonce(self) -> int:
        """Microseconds of the (synchronized) time, strictly increasing: Aster rejects a repeated or older nonce."""
        self._last_nonce = max(self._last_nonce + 1, int(self.milliseconds()) * 1000)
        return self._last_nonce

    @staticmethod
    def _encode(parameters: Dict[str, Any]) -> str:
        return urlencode({key: ("true" if value is True else "false" if value is False else value)
                          for key, value in parameters.items() if value is not None})

    @staticmethod
    def _signature(message: str, private_key: str) -> str:
        typed_data = {**_TYPED_DATA, "message": {"msg": message}}
        signed = Account.sign_message(encode_typed_data(full_message=typed_data), private_key=private_key)
        return "0x" + signed.signature.hex().removeprefix("0x")
