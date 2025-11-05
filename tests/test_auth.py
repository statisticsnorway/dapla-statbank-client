from pathlib import Path
from typing import Any
from typing import cast
from unittest import mock

import pytest
import requests.auth
from furl import furl
from requests import Response

from statbank.auth import StatbankAuth
from statbank.auth import StatbankConfig
from statbank.auth import UseDb
from statbank.globals import DaplaEnvironment
from statbank.globals import DaplaRegion
from statbank.writable_netrc import Netrc


# Mock for os.environ.get
@pytest.fixture
def mock_environ_test():
    with mock.patch.dict(
        "os.environ",
        {
            "DAPLA_ENVIRONMENT": "TEST",
            "DAPLA_SERVICE": "JUPYTERLAB",
            "DAPLA_REGION": "ON_PREM",
            "STATBANK_ENCRYPT_URL": "https://fakeurl.com/encrypt",
            "STATBANK_BASE_URL": "https://fakeurl.com/",
        },
    ):
        yield


@pytest.fixture
def mock_environ_prod_dapla():
    with mock.patch.dict(
        "os.environ",
        {
            "DAPLA_ENVIRONMENT": "PROD",
            "DAPLA_SERVICE": "JUPYTERLAB",
            "DAPLA_REGION": "ON_PREM",
            "STATBANK_ENCRYPT_URL": "https://fakeurl.com/encrypt",
            "STATBANK_TEST_ENCRYPT_URL": "https://test.fakeurl.com/encrypt",
            "STATBANK_BASE_URL": "https://fakeurl.com/",
            "STATBANK_TEST_BASE_URL": "https://test.fakeurl.com/",
        },
    ):
        yield


# Mock for getpass.getpass
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
def fake_encrypt_response(monkeypatch: pytest.MonkeyPatch, fake_auth: str) -> None:
    def fake_request(*_: Any, **__: Any):
        mock_response = mock.Mock(spec_set=Response)
        mock_response.json.return_value = {"message": fake_auth}
        return mock_response

    monkeypatch.setattr("requests.post", fake_request)


@pytest.mark.usefixtures("mock_environ_test")
def test_build_urls(
    auth_fixture: requests.auth.AuthBase,
) -> None:
    # Instantiate the class
    statbank_auth = StatbankAuth(use_db=UseDb.PROD, auth=auth_fixture)

    # Call the _build_urls method
    urls = statbank_auth._build_urls()  # noqa: SLF001

    # Verify the expected URLs
    expected_urls = {
        "loader": furl("https://fakeurl.com/statbank/sos/v1/DataLoader"),
        "uttak": furl("https://fakeurl.com/statbank/sos/v1/uttaksbeskrivelse"),
        "gui": furl("https://fakeurl.com/lastelogg/gui"),
        "api": furl("https://fakeurl.com/lastelogg/api"),
    }
    assert urls == expected_urls


@pytest.mark.usefixtures("mock_environ_prod_dapla")
def test_build_urls_testdb_from_prod(
    auth_fixture: requests.auth.AuthBase,
) -> None:
    # Instantiate the class
    statbank_auth = StatbankAuth(use_db=UseDb.TEST, auth=auth_fixture)

    # Call the _build_urls method
    urls = statbank_auth._build_urls()  # noqa: SLF001

    # Verify the expected URLs
    expected_urls = {
        "loader": furl("https://test.fakeurl.com/statbank/sos/v1/DataLoader"),
        "uttak": furl("https://test.fakeurl.com/statbank/sos/v1/uttaksbeskrivelse"),
        "gui": furl("https://test.fakeurl.com/lastelogg/gui"),
        "api": furl("https://test.fakeurl.com/lastelogg/api"),
    }
    assert urls == expected_urls


@pytest.mark.usefixtures("mock_environ_prod_dapla")
def test_check_databases_from_prod(
    auth_fixture: requests.auth.AuthBase,
) -> None:
    statbank_auth = StatbankAuth(use_db=UseDb.PROD, auth=auth_fixture)
    assert statbank_auth.check_database() == "PROD"
    statbank_auth.use_db = UseDb.TEST
    assert statbank_auth.check_database() == "TEST"


@pytest.mark.usefixtures("fake_encrypt_response", "patch_getpass")
def test_auth_without_authfile(empty_netrc_file: Path, fake_auth: str):
    config = StatbankConfig(
        environment=DaplaEnvironment.PROD,
        region=DaplaRegion.ON_PREM,
        endpoint_base=furl("https://fakeurl.com"),
        encrypt_url=furl("https://fakeurl.com/encrypt"),
        useragent="statbank-test",
        netrc_path=empty_netrc_file,
    )

    statbankauth = StatbankAuth(config=config)
    auth = statbankauth._auth  # noqa: SLF001

    assert isinstance(auth, requests.auth.HTTPBasicAuth)
    assert auth.username == "kari"
    assert auth.password == fake_auth


@pytest.mark.usefixtures("fake_encrypt_response", "patch_getpass")
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
def test_read_auth_from_authfile(existing_netrc_file: Path, fake_auth: str):
    config = StatbankConfig(
        environment=DaplaEnvironment.PROD,
        region=DaplaRegion.ON_PREM,
        endpoint_base=furl("https://fakeurl.com"),
        encrypt_url=furl("https://fakeurl.com/encrypt"),
        useragent="statbank-test",
        netrc_path=existing_netrc_file,
    )

    statbankauth = StatbankAuth(use_db=UseDb.PROD, config=config)
    auth = statbankauth._auth  # noqa: SLF001

    assert isinstance(auth, requests.auth.HTTPBasicAuth)
    assert auth.username == "ola"
    assert auth.password == fake_auth
