#!/usr/bin/env python3

import base64
import getpass
import json
import os

import requests as r
from dapla import AuthClient


class StatbankAuth:
    """
    Parent class for shared behavior between Statbankens "Transfer-API" and "Uttaksbeskrivelse-API"
    ...

    Methods
    -------
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

    def _build_user_agent(self):
        url = os.environ["STATBANK_ENCRYPT_URL"].split("://")[1]
        if url.startswith("dapla"):
            user_agent = "Dapla"
        elif "ssb" in os.listdir("/"):
            if "stamme01" in os.listdir("/ssb/"):
                user_agent = "Bakke"
            else:
                raise SystemError("Can't determine if Im in dapla or in prodsone")
        else:
            raise SystemError("Can't determine if Im in dapla or in prodsone")

        if "test" in os.environ["STATBANK_BASE_URL"]:
            user_agent += "Test-"
        elif "i.ssb" in os.environ["STATBANK_BASE_URL"]:
            user_agent += "Prod-"
        else:
            raise SystemError(
                "Can't determine if Im sending to the test-database or the prod-database"
            )

        return user_agent + r.utils.default_headers()["User-agent"]

    def _build_auth(self):
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

    @staticmethod
    def _encrypt_request():
        if "test" in os.environ["STATBANK_BASE_URL"].lower():
            db = "TEST"
        else:
            db = "PROD"
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
            os.environ["STATBANK_ENCRYPT_URL"],
            headers=headers,
            json={"message": getpass.getpass(f"Lastepassord ({db}):")},
        )

    @staticmethod
    def _build_urls() -> dict:
        base_url = os.environ["STATBANK_BASE_URL"]
        end_urls = {
            "loader": "statbank/sos/v1/DataLoader?",
            "uttak": "statbank/sos/v1/uttaksbeskrivelse?",
            "gui": "lastelogg/gui/",
            "api": "lastelogg/api/",
        }
        return {k: base_url + v for k, v in end_urls.items()}
