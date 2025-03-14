import base64
from collections.abc import Callable
from unittest.mock import Mock
from unittest.mock import patch

import pytest
from dapla.auth import AuthError
from requests import Response

from statbank.auth import StatbankAuth
from statbank.auth import UseDb

SUCCESS_STATUS = 200


# Mock for os.environ.get
@pytest.fixture
def mock_environ_test():
    with patch.dict(
        "os.environ",
        {
            "DAPLA_ENVIRONMENT": "TEST",
            "DAPLA_SERVICE": "JUPYTERLAB",
            "DAPLA_REGION": "ON_PREM",
            "STATBANK_ENCRYPT_URL": "http://fakeurl.com/encrypt",
            "STATBANK_BASE_URL": "http://fakeurl.com/",
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
            "DAPLA_REGION": "DAPLA",
            "STATBANK_ENCRYPT_URL": "http://fakeurl.com/encrypt",
            "STATBANK_TEST_ENCRYPT_URL": "http://test.fakeurl.com/encrypt",
            "STATBANK_BASE_URL": "http://fakeurl.com/",
            "STATBANK_TEST_BASE_URL": "http://test.fakeurl.com/",
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
    with patch("dapla.AuthClient.fetch_personal_token", return_value="fake_token"):
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


def test_build_auth(
    mock_environ_test: Callable[[], None],  # noqa: ARG001
    mock_getpass: Callable[[], None],  # noqa: ARG001
    mock_fetch_token: Callable[[], None],  # noqa: ARG001
    mock_requests_post: Callable[[], Mock],  # noqa: ARG001
) -> None:
    # Instantiate the class
    statbank_auth = StatbankAuth(use_db=UseDb.PROD)

    # Call the _build_auth method
    auth_header = statbank_auth._build_auth()  # noqa: SLF001

    # Verify the expected output
    expected_auth = "Basic " + base64.b64encode(
        b"fakepassword:encrypted_password",
    ).decode("utf8")
    assert auth_header == expected_auth


def test_encrypt_request_success(
    mock_environ_test: Callable[[], None],  # noqa: ARG001
    mock_getpass: Callable[[], None],  # noqa: ARG001
    mock_fetch_token: Callable[[], None],  # noqa: ARG001
    mock_requests_post: Callable[[], Mock],  # noqa: ARG001
) -> None:
    # Instantiate the class
    statbank_auth = StatbankAuth(use_db=UseDb.PROD)

    # Call the _encrypt_request method
    response = statbank_auth._encrypt_request()  # noqa: SLF001

    # Verify that the response is correct
    assert response.status_code == SUCCESS_STATUS
    assert response.text == '{"message": "encrypted_password"}'


def test_encrypt_request_no_token(
    mock_environ_test: Callable[[], None],  # noqa: ARG001
    mock_getpass: Callable[[], None],  # noqa: ARG001
    mock_requests_post: Callable[[], Mock],  # noqa: ARG001
) -> None:
    # Mock the AuthError exception
    with patch("dapla.AuthClient.fetch_personal_token", side_effect=AuthError):
        # Instantiate the class
        statbank_auth = StatbankAuth(use_db=UseDb.PROD)

        # Call the _encrypt_request method
        response = statbank_auth._encrypt_request()  # noqa: SLF001

        # Verify that the response is correct
        assert response.status_code == SUCCESS_STATUS
        assert response.text == '{"message": "encrypted_password"}'


def test_build_headers(
    mock_environ_test: Callable[[], None],  # noqa: ARG001
    mock_getpass: Callable[[], None],  # noqa: ARG001
    mock_fetch_token: Callable[[], None],  # noqa: ARG001
    mock_requests_post: Callable[[], Mock],  # noqa: ARG001
) -> None:
    # Instantiate the class
    statbank_auth = StatbankAuth(use_db=UseDb.PROD)

    # Call the _build_headers method
    headers = statbank_auth._build_headers()  # noqa: SLF001

    # Verify the expected headers
    expected_headers = {
        "Authorization": statbank_auth._build_auth(),  # noqa: SLF001
        "Content-Type": "multipart/form-data; boundary=12345",
        "Connection": "keep-alive",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "*/*",
        "User-Agent": statbank_auth._build_user_agent(),  # noqa: SLF001
    }
    assert headers == expected_headers


def test_build_user_agent(
    mock_environ_test: Callable[[], None],  # noqa: ARG001
) -> None:
    # Instantiate the class
    statbank_auth = StatbankAuth(use_db=UseDb.PROD)

    # Call the _build_user_agent method
    user_agent = statbank_auth._build_user_agent()  # noqa: SLF001

    # Verify the expected user agent
    expected_user_agent_prefix = "TEST-ON_PREM-JUPYTERLAB-"
    assert user_agent.startswith(expected_user_agent_prefix)


def test_build_urls(
    mock_environ_test: Callable[[], None],  # noqa: ARG001
) -> None:
    # Instantiate the class
    statbank_auth = StatbankAuth(use_db=UseDb.PROD)

    # Call the _build_urls method
    urls = statbank_auth._build_urls()  # noqa: SLF001

    # Verify the expected URLs
    expected_urls = {
        "loader": "http://fakeurl.com/statbank/sos/v1/DataLoader?",
        "uttak": "http://fakeurl.com/statbank/sos/v1/uttaksbeskrivelse?",
        "gui": "http://fakeurl.com/lastelogg/gui/",
        "api": "http://fakeurl.com/lastelogg/api/",
    }
    assert urls == expected_urls


def test_build_urls_testdb_from_prod(
    mock_environ_prod_dapla: Callable[[], None],  # noqa: ARG001
) -> None:
    # Instantiate the class
    statbank_auth = StatbankAuth(use_db=UseDb.TEST)

    # Call the _build_urls method
    urls = statbank_auth._build_urls()  # noqa: SLF001

    # Verify the expected URLs
    expected_urls = {
        "loader": "http://test.fakeurl.com/statbank/sos/v1/DataLoader?",
        "uttak": "http://test.fakeurl.com/statbank/sos/v1/uttaksbeskrivelse?",
        "gui": "http://test.fakeurl.com/lastelogg/gui/",
        "api": "http://test.fakeurl.com/lastelogg/api/",
    }
    assert urls == expected_urls


def test_check_databases_from_prod(
    mock_environ_prod_dapla: Callable[[], None],  # noqa: ARG001
) -> None:
    statbank_auth = StatbankAuth(use_db=UseDb.PROD)
    assert statbank_auth.check_database() == "PROD"
    statbank_auth.use_db = UseDb.TEST
    assert statbank_auth.check_database() == "TEST"
