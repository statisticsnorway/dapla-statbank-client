from pathlib import Path
from typing import cast
from unittest import mock

import pytest
import requests.auth
from furl import furl
from requests import Response

from statbank.api_exceptions import StatbankAuthError
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


@pytest.mark.usefixtures("fake_encrypt_response", "patch_getpass", "patch_dapla_auth")
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


def test_reset_auth_calls_cleanup_and_refreshes_auth(
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
    monkeypatch: pytest.MonkeyPatch,
):
    sa = StatbankAuth(config=config_fixture, auth=auth_fixture)

    new_auth = requests.auth.HTTPBasicAuth(
        username="new",
        password="secret",  # noqa: S106 - fake password dude
    )
    cleanup_mock = mock.Mock()
    get_auth_mock = mock.Mock(return_value=new_auth)

    monkeypatch.setattr(sa, "_cleanup_netrc", cleanup_mock)
    monkeypatch.setattr(sa, "_get_auth", get_auth_mock)

    sa.reset_auth()

    cleanup_mock.assert_called_once()
    get_auth_mock.assert_called_once()
    assert sa._auth is new_auth  # noqa: SLF001 - We are internal in the package dude


def test_react_to_httperror_oracle_28000_raises(
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
    monkeypatch: pytest.MonkeyPatch,
):
    sa = StatbankAuth(config=config_fixture, auth=auth_fixture)
    cleanup_mock = mock.Mock()
    monkeypatch.setattr(sa, "_cleanup_netrc", cleanup_mock)

    err = StatbankAuthError("wrapped")
    err.response_content = {"ExceptionMessage": "ORA-28000: account locked"}

    with pytest.raises(StatbankAuthError) as exc:
        sa._react_to_httperror_should_retry(err)  # noqa: SLF001

    cleanup_mock.assert_called_once()
    assert "locked" in str(exc.value).lower()


def test_react_to_httperror_oracle_01017_returns_true_and_refreshes(
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
    monkeypatch: pytest.MonkeyPatch,
):
    sa = StatbankAuth(config=config_fixture, auth=auth_fixture)
    cleanup_mock = mock.Mock()
    monkeypatch.setattr(sa, "_cleanup_netrc", cleanup_mock)

    new_auth = requests.auth.HTTPBasicAuth(
        username="newuser",
        password="pw",  # noqa: S106 - fake password dude
    )
    get_auth_mock = mock.Mock(return_value=new_auth)
    monkeypatch.setattr(sa, "_get_auth", get_auth_mock)

    err = StatbankAuthError("wrapped")
    err.response_content = {"ExceptionMessage": "ORA-01017: invalid username/password"}

    should_retry = sa._react_to_httperror_should_retry(err)  # noqa: SLF001

    cleanup_mock.assert_called_once()
    get_auth_mock.assert_called_once()
    assert should_retry is True
    assert sa._auth is new_auth  # noqa: SLF001 - We are internal in the package dude


def test_react_to_httperror_other_raises_original(
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
    monkeypatch: pytest.MonkeyPatch,
):
    sa = StatbankAuth(config=config_fixture, auth=auth_fixture)
    cleanup_mock = mock.Mock()
    monkeypatch.setattr(sa, "_cleanup_netrc", cleanup_mock)

    err = StatbankAuthError("some other error")
    err.response_content = {"ExceptionMessage": "SOME_OTHER_CODE"}

    with pytest.raises(StatbankAuthError) as exc:
        sa._react_to_httperror_should_retry(err)  # noqa: SLF001

    cleanup_mock.assert_called_once()
    # The method re-raises the same exception object when unhandled
    assert exc.value is err


def test_config_from_environ_missing_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    # Ensure valid enums so we reach the KeyError -> ValueError branch
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "TEST")
    monkeypatch.setenv("DAPLA_REGION", "ON_PREM")
    # Remove required envs to trigger KeyError -> ValueError
    monkeypatch.delenv("STATBANK_BASE_URL", raising=False)
    monkeypatch.delenv("STATBANK_ENCRYPT_URL", raising=False)
    with pytest.raises(ValueError, match=r"Kunne ikke finne"):
        StatbankConfig.from_environ(use_db=None)


def test_config_from_environ_invalid_url(monkeypatch: pytest.MonkeyPatch) -> None:
    # Force furl() to raise ValueError to hit the except ValueError branch
    monkeypatch.setenv("STATBANK_BASE_URL", "https://example.invalid")
    monkeypatch.setenv("STATBANK_ENCRYPT_URL", "https://example.invalid/encrypt")
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "TEST")
    monkeypatch.setenv("DAPLA_REGION", "ON_PREM")

    def _boom(_: str):
        msg = "bad url"
        raise ValueError(msg)

    monkeypatch.setattr("statbank.auth.furl", _boom)
    with pytest.raises(ValueError, match=r"ikke er gyldig"):
        StatbankConfig.from_environ(use_db=None)


def test_cleanup_netrc_removes_entry(
    existing_netrc_file: Path,
    auth_fixture: requests.auth.AuthBase,
):
    # Ensure cleanup only touches the provided temp netrc file
    cfg = StatbankConfig(
        endpoint_base=furl("https://fakeurl.com"),
        encrypt_url=furl("https://fakeurl.com/encrypt"),
        useragent="statbank-test",
        environment=DaplaEnvironment.PROD,
        region=DaplaRegion.ON_PREM,
        netrc_path=existing_netrc_file,
    )
    sa = StatbankAuth(config=cfg, auth=auth_fixture)
    # Verify entry exists initially
    host = cast("str", cfg.endpoint_base.host)
    with Netrc(existing_netrc_file) as authfile:
        assert authfile[host]
    # Perform cleanup and verify removal
    sa._cleanup_netrc()  # noqa: SLF001
    with Netrc(existing_netrc_file) as authfile:
        assert not authfile[host]
