#!/usr/bin/env python3

import os
import getpass
import base64
import requests as r

from dapla import AuthClient

class StatbankAuth:
    """
    Parent class for shared behavior between Statbankens "Transfer-API" and "Uttaksbeskrivelse-API"
    ...

    Methods
    -------
    _decide_dapla_environ() -> str:
        If in Dapla-staging, should return "TEST", otherwise "PROD". 
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
    @staticmethod
    def _decide_dapla_environ() -> str:
        if "staging" in os.environ["CLUSTER_ID"].lower():
            return "TEST"
        else:
            return "PROD"
        
    def _build_headers(self) -> dict:
        return {
            'Authorization': self._build_auth(),
            'Content-Type': 'multipart/form-data; boundary=12345',
            'Connection': 'keep-alive',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept' : r'*/*',
            }


    def _build_auth(self):
        response = self._encrypt_request()
        try:
            username_encryptedpassword = bytes(self.lastebruker, 'UTF-8') + bytes(':', 'UTF-8') + bytes(json.loads(response.text)['message'], 'UTF-8')
        finally:
            del response
        return bytes('Basic ', 'UTF-8') + base64.b64encode(username_encryptedpassword)

    @staticmethod
    def _encrypt_request():
        if "test" in os.environ['STATBANK_BASE_URL'].lower():
            db = "TEST"
        else:
            db = "PROD"
        return r.post(os.environ['STATBANK_ENCRYPT_URL'],
                      headers={
                              'Authorization': f'Bearer {AuthClient.fetch_personal_token()}',
                              'Content-type': 'application/json'}, 
                      json={"message": getpass.getpass(f"Lastepassord ({db}):")}
                     )

    @staticmethod
    def _build_urls() -> dict:
        base_url = os.environ['STATBANK_BASE_URL']
        END_URLS = {
            'loader': 'statbank/sos/v1/DataLoader?',
            'uttak': 'statbank/sos/v1/uttaksbeskrivelse?',
            'gui': 'lastelogg/gui/',
            'api': 'lastelogg/api/',
        }
        return {k: base_url+v for k, v in END_URLS.items()}
