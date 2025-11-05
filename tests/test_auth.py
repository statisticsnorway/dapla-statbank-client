from collections.abc import Callable
from unittest.mock import Mock
from unittest.mock import patch

import pytest
import requests.auth
from furl import furl
from requests import Response

from statbank.auth import StatbankAuth
from statbank.auth import UseDb


# Mock for os.environ.get
@pytest.fixture
def mock_environ_test():
    with patch.dict(
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
    with patch.dict(
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
def mock_getpass():
    with patch("getpass.getpass", return_value="fakepassword"):
        yield


# Mock for AuthClient.fetch_personal_token
@pytest.fixture
def mock_fetch_token():
    with patch(
        "dapla_auth_client.AuthClient.fetch_personal_token",
        return_value="fake_token",
    ):
        yield


# Mock for requests.post
@pytest.fixture
def mock_requests_post():
    with patch("requests.post") as mock_post:
        mock_response = Mock(spec=Response)
        mock_response.text = '{"message": "encrypted_password"}'
        mock_response.status_code = 200
        mock_post.return_value = mock_response
        yield mock_post


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


def test_check_databases_from_prod(
    mock_environ_prod_dapla: Callable[[], None],  # noqa: ARG001
    auth_fixture: requests.auth.AuthBase,
) -> None:
    statbank_auth = StatbankAuth(use_db=UseDb.PROD, auth=auth_fixture)
    assert statbank_auth.check_database() == "PROD"
    statbank_auth.use_db = UseDb.TEST
    assert statbank_auth.check_database() == "TEST"
