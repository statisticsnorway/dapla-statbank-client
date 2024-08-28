import os
from typing import Callable

from dotenv import load_dotenv

load_dotenv()
import pytest

from statbank import StatbankClient


@pytest.fixture(scope="module")
def monkeymodule():
    from _pytest.monkeypatch import MonkeyPatch

    mpatch = MonkeyPatch()
    yield mpatch
    mpatch.undo()


@pytest.fixture(scope="module")
def client_input(monkeymodule: Callable) -> StatbankClient:
    monkeymodule.setattr(
        "getpass.getpass",
        lambda _: os.environ["STATBANK_TEST_PASSWORD"],
    )
    return StatbankClient(
        os.environ["STATBANK_TEST_USER"],
        check_username_password=False,
    )


@pytest.mark.integration_dapla
def test_client_date(client_input: StatbankClient):
    assert isinstance(client_input.approve, int)


@pytest.mark.integration_dapla
def test_client_get_description(client_input: StatbankClient):
    filbesk = client_input.get_description("05300")
    assert len(filbesk.codelists)
