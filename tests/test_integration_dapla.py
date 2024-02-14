import os

from dotenv import load_dotenv

load_dotenv()
import pytest
from _pytest.monkeypatch import monkeypatch

from statbank import StatbankClient


@pytest.mark.integration_dapla()
@pytest.fixture(scope="module", autouse=True)
def client() -> StatbankClient:
    mp = monkeypatch()
    mp.setattr("builtins.input", lambda _: os.environ.get("STATBANK_TEST_PASSWORD"))
    return StatbankClient(os.environ.get("STATBANK_TEST_USER"))


@pytest.mark.integration_dapla()
def test_client_date(client: StatbankClient):
    assert isinstance(client.approve, int)


@pytest.mark.integration_dapla()
def test_client_get_description(client: StatbankClient):
    filbesk = client.get_description("05300")
    assert len(filbesk.codelists)
