#!/usr/bin/env python3

import base64
import getpass
import json
import os
from pathlib import Path

import requests as r
from dapla import AuthClient

import statbank.logger


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

    def _build_headers(self) -> dict:
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

        Returns:
        -------
        str
            "DAPLA" if on dapla, "PROD" if you are in prodsone.

        Raises:
        ------
        OSError
            If no indications match, dapla/prod may have changed (please report)
            Or you are using the function outside of dapla/prod on purpose?
        """
        platform = ""
        jupyter_image_spec = os.environ.get("JUPYTER_IMAGE_SPEC", "")
        if jupyter_image_spec and "jupyterlab-dapla" in jupyter_image_spec:
            platform = "DAPLA"
        elif Path("/ssb/bruker").is_dir():
            platform = "PROD"
        if platform:
            return platform
        error_msg = "Ikke i prodsonen, eller på Dapla? Må funksjonen skrives om?"
        raise OSError(error_msg)

    @staticmethod
    def check_database() -> str:
        """Checks if we are in staging/testing environment. And which statbank-database we are sending to."""
        target_database = ""
        if "test" in os.environ.get("STATBANK_BASE_URL", ""):
            statbank.logger.info(
                "Warning: Descriptions and data in the TEST-database may be outdated!",
            )
            target_database = "TEST"
        elif "i.ssb" in os.environ.get("STATBANK_BASE_URL", ""):
            target_database = "PROD"
        if target_database:
            return target_database
        error_msg = (
            "Can't determine if Im sending to the test-database or the prod-database"
        )
        raise SystemError(error_msg)

    def _build_user_agent(self) -> str:
        if self.check_env() == "DAPLA":
            user_agent = "Dapla"
        elif self.check_env() == "PROD":
            user_agent = "Bakke"
        else:
            error_msg = "Can't determine if Im in dapla or in prodsone"
            raise SystemError(error_msg)

        if self.check_database() == "TEST":
            user_agent += "Test-"
        elif self.check_database() == "PROD":
            user_agent += "Prod-"
        else:
            error_msg = "Can't determine if Im sending to the test-database or the prod-database"
            raise SystemError(error_msg)

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
    def _build_urls() -> dict:
        base_url = os.environ.get("STATBANK_BASE_URL", "Cant find url in environ.")
        end_urls = {
            "loader": "statbank/sos/v1/DataLoader?",
            "uttak": "statbank/sos/v1/uttaksbeskrivelse?",
            "gui": "lastelogg/gui/",
            "api": "lastelogg/api/",
        }
        return {k: base_url + v for k, v in end_urls.items()}
