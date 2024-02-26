import base64
import getpass
import json
import os

import requests as r
from dapla import AuthClient


class StatbankAuth:
    """Parent class for shared behavior between Statbankens "Transfer-API" and "Uttaksbeskrivelse-API".

    Methods:
        _build_headers() -> dict:
            Creates dict of headers needed in request to talk to Statbank-API
        _build_auth() -> str:
            Gets key from environment and encrypts password with key, combines it with username into expected Authentication header.
        _encrypt_request() -> str:
            Encrypts password with key from local service, url for service should be environment variables. Password is not possible to send into function. Because safety.
        _build_urls() -> dict:
            Urls will differ based environment variables, returns a dict of urls.
        __init__():

            is not implemented, as Transfer and UttrekksBeskrivelse both add their own.
    """

    def __init__(self) -> None:
        """This init will never be used directly, as this class is always inherited from.

        This is for typing with Mypy.
        """
        self.loaduser: str

    def _build_headers(self) -> dict[str, str]:
        return {
            "Authorization": self._build_auth(),
            "Content-Type": "multipart/form-data; boundary=12345",
            "Connection": "keep-alive",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": r"*/*",
            "User-Agent": self._build_user_agent(),
        }

    @staticmethod
    def check_env() -> str:
        """Check if you are on Dapla or in prodsone.

        Simplified terribly by the addition of env vars for this, keeping this method for legacy reasons.

        Returns:
            str: "DAPLA" if on dapla, "PROD" if you are in prodsone.
        """
        return os.environ.get("DAPLA_ENVIRONMENT", "TEST")

    @staticmethod
    def check_database() -> str:
        """Checks if we are in prod environment. And which statbank-database we are sending to."""
        db = "TEST"
        if os.environ.get("DAPLA_ENVIRONMENT", "TEST") == "PROD":
            db = "PROD"
        return db

    def _build_user_agent(self) -> str:
        envir = os.environ.get("DAPLA_ENVIRONMENT", "TEST")
        service = os.environ.get("DAPLA_SERVICE", "JUPYTERLAB")
        region = os.environ.get("DAPLA_REGION", "ON_PREM")

        user_agent = f"{envir}-{region}-{service}-"

        return user_agent + r.utils.default_headers()["User-agent"]

    def _build_auth(self) -> str:
        response = self._encrypt_request()
        try:
            username_encryptedpassword = (
                bytes(self.loaduser, "UTF-8")
                + bytes(":", "UTF-8")
                + bytes(json.loads(response.text)["message"], "UTF-8")
            )
        finally:
            del response
        return "Basic " + base64.b64encode(username_encryptedpassword).decode("utf8")

    def _encrypt_request(self) -> r.Response:
        db = self.check_database()
        if AuthClient.is_ready():
            headers = {
                "Authorization": f"Bearer {AuthClient.fetch_personal_token()}",
                "Content-type": "application/json",
            }
        else:
            headers = {
                "Content-type": "application/json",
            }
        return r.post(
            os.environ.get("STATBANK_ENCRYPT_URL", "Cant find url in environ."),
            headers=headers,
            json={"message": getpass.getpass(f"Lastepassord ({db}):")},
            timeout=5,
        )

    @staticmethod
    def _build_urls() -> dict[str, str]:
        base_url = os.environ.get("STATBANK_BASE_URL", "Cant find url in environ.")
        end_urls = {
            "loader": "statbank/sos/v1/DataLoader?",
            "uttak": "statbank/sos/v1/uttaksbeskrivelse?",
            "gui": "lastelogg/gui/",
            "api": "lastelogg/api/",
        }
        return {k: base_url + v for k, v in end_urls.items()}
