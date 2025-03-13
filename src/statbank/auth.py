import base64
import getpass
import json
import os

import requests as r
from dapla import AuthClient
from dapla.auth import AuthError

from statbank.statbank_logger import logger


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

    def check_database(self) -> str:
        """Checks if we are in prod environment. And which statbank-database we are sending to."""
        db = "TEST"
        if os.environ.get("DAPLA_ENVIRONMENT", "TEST") == "PROD":
            db = "PROD"
        if self.use_test_db:  # type: ignore[attr-defined]
            db = "TEST"
        return db

    def _build_user_agent(self) -> str:
        envir = os.environ.get("DAPLA_ENVIRONMENT", "TEST")
        service = os.environ.get("DAPLA_SERVICE", "JUPYTERLAB")
        region = os.environ.get("DAPLA_REGION", "ON_PREM")

        user_agent = f"{envir}-{region}-{service}-"

        return user_agent + r.utils.default_headers()["User-agent"]

    def _build_auth(self) -> str:
        username_encryptedpassword = (
            bytes(
                self._get_user(),
                "UTF-8",
            )
            + bytes(":", "UTF-8")
            + bytes(json.loads(self._encrypt_request().text)["message"], "UTF-8")
        )
        return "Basic " + base64.b64encode(username_encryptedpassword).decode("utf8")

    @staticmethod
    def _get_user() -> str:
        return getpass.getpass("Lastebruker:")

    def _use_test_url(self, test_env: str, prod_env: str) -> str:
        use_test = (
            self.use_test_db and os.environ.get("DAPLA_ENVIRONMENT", "TEST") == "PROD"  # type: ignore[attr-defined]
        )
        env = test_env if use_test else prod_env
        return os.environ.get(env, f"Cant find {env} in environ.")

    def _encrypt_request(self) -> r.Response:
        db = self.check_database()
        try:
            headers = {
                "Content-type": "application/json",
            }
            if os.environ.get("DAPLA_REGION", "TEST") != "ON_PREM":
                headers["Authorization"] = f"Bearer {AuthClient.fetch_personal_token()}"
        except AuthError as err:
            logger.warning(str(err))
            headers = {
                "Content-type": "application/json",
            }

        return r.post(
            self._use_test_url("STATBANK_TEST_ENCRYPT_URL", "STATBANK_ENCRYPT_URL"),
            headers=headers,
            json={"message": getpass.getpass(f"Lastepassord ({db}):")},
            timeout=5,
        )

    def _build_urls(self) -> dict[str, str]:
        base_url = self._use_test_url("STATBANK_TEST_BASE_URL", "STATBANK_BASE_URL")
        end_urls = {
            "loader": "statbank/sos/v1/DataLoader?",
            "uttak": "statbank/sos/v1/uttaksbeskrivelse?",
            "gui": "lastelogg/gui/",
            "api": "lastelogg/api/",
        }
        return {k: base_url + v for k, v in end_urls.items()}
