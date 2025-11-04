from pathlib import Path
from typing import cast
from unittest import mock

import pytest
import requests.auth
from furl import furl
from requests import Response

from statbank.auth import StatbankAuth
from statbank.auth import StatbankConfig
from statbank.auth import TokenAuth
from statbank.auth import UseDb
from statbank.globals import DaplaEnvironment
from statbank.globals import DaplaRegion
from statbank.writable_netrc import Netrc


@pytest.fixture
def patch_getpass(monkeypatch: pytest.MonkeyPatch):
    def getpass_return(prompt: str) -> str:
        if "bruker" in prompt:
            return "kari"
        if "passord" in prompt:
            return "qwerty"
        return ""

    monkeypatch.setattr("getpass.getpass", getpass_return)


@pytest.fixture
def fake_encrypt_response(monkeypatch: pytest.MonkeyPatch, fake_auth: str) -> mock.Mock:
    mock_response = mock.Mock(spec_set=Response)
    mock_response.json.return_value = {"message": fake_auth}

    mock_post = mock.Mock(return_value=mock_response)
    monkeypatch.setattr("requests.post", mock_post)

    return mock_post


@pytest.fixture
def patch_dapla_auth(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "dapla_auth_client.AuthClient.fetch_personal_token",
        lambda: "token",
    )


@pytest.mark.usefixtures("mock_environ_on_prem_prod")
def test_check_databases_from_prod(
    auth_fixture: requests.auth.AuthBase,
) -> None:
    statbank_auth = StatbankAuth(use_db=UseDb.PROD, auth=auth_fixture)
    assert statbank_auth.check_database() == "PROD"
    statbank_auth.use_db = UseDb.TEST
    assert statbank_auth.check_database() == "TEST"


@pytest.mark.usefixtures("patch_getpass", "patch_dapla_auth")
def test_auth_without_authfile(
    empty_netrc_file: Path,
    fake_auth: str,
    fake_encrypt_response: mock.Mock,
):
    config = StatbankConfig(
        environment=DaplaEnvironment.PROD,
        region=DaplaRegion.ON_PREM,
        endpoint_base=furl("https://fakeurl.com"),
        encrypt_url=furl("https://fakeurl.com/encrypt"),
        useragent="statbank-test",
        netrc_path=empty_netrc_file,
    )

    statbankauth = StatbankAuth(config=config)

    fake_encrypt_response.assert_called_once_with(
        config.encrypt_url.url,
        json={"message": "qwerty"},
        auth=None,
        timeout=mock.ANY,
    )

    auth = statbankauth._auth  # noqa: SLF001

    assert isinstance(auth, requests.auth.HTTPBasicAuth)
    assert auth.username == "kari"
    assert auth.password == fake_auth


@pytest.mark.usefixtures("patch_getpass", "patch_dapla_auth")
def test_auth_without_authfile_dapla_lab(
    empty_netrc_file: Path,
    fake_auth: str,
    fake_encrypt_response: mock.Mock,
):
    config = StatbankConfig(
        environment=DaplaEnvironment.PROD,
        region=DaplaRegion.DAPLA_LAB,
        endpoint_base=furl("https://fakeurl.com"),
        encrypt_url=furl("https://fakeurl.com/encrypt"),
        useragent="statbank-test",
        netrc_path=empty_netrc_file,
    )

    statbankauth = StatbankAuth(config=config)

    fake_encrypt_response.assert_called_once_with(
        config.encrypt_url.url,
        json={"message": "qwerty"},
        auth=TokenAuth("token"),
        timeout=mock.ANY,
    )

    auth = statbankauth._auth  # noqa: SLF001

    assert isinstance(auth, requests.auth.HTTPBasicAuth)
    assert auth.username == "kari"
    assert auth.password == fake_auth


@pytest.mark.usefixtures("fake_encrypt_response", "patch_dapla_auth")
def test_auth_persisted(empty_netrc_file: Path, fake_auth: str):
    config = StatbankConfig(
        environment=DaplaEnvironment.PROD,
        region=DaplaRegion.ON_PREM,
        endpoint_base=furl("https://fakeurl.com"),
        encrypt_url=furl("https://fakeurl.com/encrypt"),
        useragent="statbank-test",
        netrc_path=empty_netrc_file,
    )

    StatbankAuth(config=config)

    host = cast("str", config.endpoint_base.host)
    authfile = Netrc(empty_netrc_file)
    assert authfile[host].login == "kari"
    assert authfile[host].password == fake_auth


@pytest.mark.usefixtures("patch_getpass")
def test_read_auth_from_authfile(
    existing_netrc_file: Path,
    fake_auth: str,
    fake_encrypt_response: mock.Mock,
):
    config = StatbankConfig(
        environment=DaplaEnvironment.PROD,
        region=DaplaRegion.ON_PREM,
        endpoint_base=furl("https://fakeurl.com"),
        encrypt_url=furl("https://fakeurl.com/encrypt"),
        useragent="statbank-test",
        netrc_path=existing_netrc_file,
    )

    statbankauth = StatbankAuth(use_db=UseDb.PROD, config=config)

    fake_encrypt_response.assert_not_called()

    auth = statbankauth._auth  # noqa: SLF001

    assert isinstance(auth, requests.auth.HTTPBasicAuth)
    assert auth.username == "ola"
    assert auth.password == fake_auth
