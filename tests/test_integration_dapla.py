import os
from unittest import mock
import getpass

import pandas as pd
import pytest
import requests

from statbank import StatbankClient
from statbank.apidata import apidata, apidata_all, apidata_rotate, apidata_query_all


@pytest.mark.integration_dapla
@pytest.fixture(scope="module", autouse=True)
def client():
    user = getpass.getpass("Lastebruker: ")
    return StatbankClient(user)

@pytest.mark.integration_dapla
def test_client_date(client):
    assert isinstance(client.approve, int)

@pytest.mark.integration_dapla
def test_client_get_descripiton(client):
    filbesk = client.get_description("05300")
    assert len(filbesk.codelists)