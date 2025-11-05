import os
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest import mock

import pandas as pd
import pytest
import requests.auth
from _pytest.fixtures import FixtureRequest
from _pytest.monkeypatch import MonkeyPatch
from furl import furl

from statbank.auth import StatbankConfig
from statbank.client import StatbankClient
from statbank.globals import DaplaEnvironment
from statbank.globals import DaplaRegion
from statbank.transfer import StatbankTransfer


def pytest_runtest_setup(item: pytest.Item):
    markers = (mark.name for mark in item.iter_markers())
    if (
        markers
        and "integration_dapla" in markers
        and not (
            os.environ.get("DAPLA_ENVIRONMENT") == "TEST"
            and os.environ.get("DAPLA_REGION") == "DAPLA_LAB"
        )
    ):
        pytest.skip(reason="Can only run on Dapla lab in test environment")


# The package uses mainly requests.get and .post, we dont want it to make ACTUAL requests during testing.
# This fixture blocks them in "the first layer", and therefore forces tests that have not patched these out correctly to fail.
# If running the jobs for "integration_dapla", this might need to be made conditional


@pytest.fixture(autouse=True)
def block_requests(request: FixtureRequest, monkeypatch: MonkeyPatch) -> None:
    if request.node.get_closest_marker("integration_dapla"):
        # Test is marked with @pytest.mark.integration_dapla, allow real HTTP when integration testing
        return

    def fake_request(*_: Any, **__: Any):
        err = "Attempted to make external HTTP request during testing!"
        raise RuntimeError(err)

    monkeypatch.setattr("requests.get", fake_request)
    monkeypatch.setattr("requests.post", fake_request)


@pytest.fixture(autouse=True)
def modify_env(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("JUPYTERHUB_USER", "ssb@ssb.no")
    monkeypatch.setenv("DAPLA_USER", "usr@ssb.no")
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "DEV")

    monkeypatch.setenv(
        "STATBANK_BASE_URL",
        os.environ.get(
            "STATBANK_BASE_URL",
            "https://test_fake_url/",
        ),
    )
    monkeypatch.setenv(
        "STATBANK_ENCRYPT_URL",
        os.environ.get(
            "STATBANK_ENCRYPT_URL",
            "https://fake_url2/",
        ),
    )


@pytest.fixture
def mock_environ_dapla_lab_prod(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "PROD")
    monkeypatch.setenv("DAPLA_SERVICE", "VS_CODE")
    monkeypatch.setenv("DAPLA_REGION", "DAPLA_LAB")
    monkeypatch.setenv("STATBANK_ENCRYPT_URL", "https://fakeurl.com/encrypt")
    monkeypatch.setenv("STATBANK_TEST_ENCRYPT_URL", "https://test.fakeurl.com/encrypt")
    monkeypatch.setenv("STATBANK_BASE_URL", "https://fakeurl.com")
    monkeypatch.setenv("STATBANK_TEST_BASE_URL", "https://test.fakeurl.com")


@pytest.fixture
def mock_environ_dapla_lab_test(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "TEST")
    monkeypatch.setenv("DAPLA_SERVICE", "VS_CODE")
    monkeypatch.setenv("DAPLA_REGION", "DAPLA_LAB")
    monkeypatch.setenv("STATBANK_ENCRYPT_URL", "https://test.fakeurl.com/encrypt")
    monkeypatch.setenv("STATBANK_BASE_URL", "https://test.fakeurl.com")


@pytest.fixture
def mock_environ_on_prem_prod(monkeypatch: MonkeyPatch):
    monkeypatch.setenv("DAPLA_ENVIRONMENT", "PROD")
    monkeypatch.setenv("DAPLA_SERVICE", "JUPYTERHUB")
    monkeypatch.setenv("DAPLA_REGION", "ON_PREM")
    monkeypatch.setenv("STATBANK_ENCRYPT_URL", "https://fakeurl.com/encrypt")
    monkeypatch.setenv("STATBANK_TEST_ENCRYPT_URL", "https://test.fakeurl.com/encrypt")
    monkeypatch.setenv("STATBANK_BASE_URL", "https://fakeurl.com")
    monkeypatch.setenv("STATBANK_TEST_BASE_URL", "https://test.fakeurl.com")


@pytest.fixture
def config_fixture() -> StatbankConfig:
    return StatbankConfig(
        endpoint_base=furl("https://test_fake_url/"),
        encrypt_url=furl("https://fake_url2/"),
        useragent="statbank-test",
        environment=DaplaEnvironment.TEST,
        region=DaplaRegion.DAPLA_LAB,
    )


@pytest.fixture
def fake_auth() -> str:
    return "SoCipherVerySecure"


@pytest.fixture
def auth_fixture(fake_auth: str) -> requests.auth.AuthBase:
    username = "TEST"
    return requests.auth.HTTPBasicAuth(username=username, password=fake_auth)


@pytest.fixture
def client_fixture(
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
) -> StatbankClient:
    return StatbankClient(
        check_username_password=False,
        config=config_fixture,
        auth=auth_fixture,
    )


@pytest.fixture
def transfer_fixture(
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
    transfer_data_fixture: dict[str, pd.DataFrame],
    fake_post_response_transfer_successful: requests.Response,
) -> Generator[StatbankTransfer, None, None]:
    with mock.patch.object(
        StatbankTransfer,
        "_make_transfer_request",
    ) as mock_transfer_make_request:
        mock_transfer_make_request.return_value = fake_post_response_transfer_successful

        yield StatbankTransfer(
            transfer_data_fixture,
            tableid="10000",
            config=config_fixture,
            auth=auth_fixture,
        )


@pytest.fixture
def transfer_data_fixture() -> dict[str, pd.DataFrame]:
    return {
        "delfil1.dat": pd.DataFrame(
            {
                "1": ["999", "01", "02"],
                "2": ["2022", "2022", "2000"],
                "3": [1.5, 2.5, 3.5],
                "4": [1.15, 2.25, 3.35],
            },
        ),
    }


# Fake responses from APIs
@pytest.fixture
def fake_get_response_uttrekksbeskrivelse_successful(fake_auth: str):
    response = requests.Response()
    response.status_code = 200
    response._content = bytes(  # noqa: SLF001
        '{"Uttaksbeskrivelse_lagd":"29.09.2022 klokka 18:51" , "base": "DB1T","TabellId":"10000","Huvudtabell":"HovedTabellNavn","DeltabellTitler":[{ "Filnavn": "delfil1.dat" , "Filtext": "10000: Fake table" }] ,"deltabller":[{"deltabell":"delfil1.dat","variabler":[{"kolonnenummer":"1","Klassifikasjonsvariabel":"Kodeliste1","Variabeltext":"kodeliste1","Kodeliste_id":"Kodeliste1","Kodeliste_text":"Kodeliste 1"},{"kolonnenummer":"2","Klassifikasjonsvariabel":"Tid","Variabeltext":"tid","Kodeliste_id":"-","Kodeliste_text":"Tidsperioden for tabelldataene, enhet = år, format = åååå"}],"statistikkvariabler":[{ "kolonnenummer":"3","Text":"Antall","Enhet":"personer","Antall_lagrede_desimaler":"0","Antall_viste_desimaler":"0"}, { "kolonnenummer":"4","Text":"Antall","Enhet":"personer","Antall_lagrede_desimaler":"1","Antall_viste_desimaler":"1"}],"eksempel_linje":"01;2022;100"}],"kodelister":[{"kodeliste":"Kodeliste1","SumIALtTotalKode":"999","koder":[{"kode":"999","text":"i alt"},{"kode":"01","text":"Kode1"},{"kode":"02","text":"Kode2"}]}]}',
        "utf8",
    )
    response.request = requests.PreparedRequest()
    response.request.headers = {
        "Authorization": fake_auth,
        "Content-Type": "multipart/form-data; boundary=12345",
    }
    return response


@pytest.fixture
def fake_post_response_transfer_successful(fake_auth: str) -> requests.Response:
    response = requests.Response()
    response.status_code = 200
    response._content = bytes(  # noqa: SLF001
        '{"TotalResult":{"GeneratedId":null,"Status":"Success","Message":"ExecutePublish with AutoGodkjennData \'2\', AutoOverskrivData \'1\', Fagansvarlig1 \'tbf\', Fagansvarlig2 \'tbf\', Hovedtabell \'HovedTabellNavn\', Publiseringsdato \'07.01.2023 00:00:00\', Publiseringstid \'08:00\':  Status 0, OK, lasting er registrert med lasteoppdragsnummer:197885 => INFORMASJON. Publiseringen er satt til kl 08:00:00","Exception":null,"ValidationInfoItems":null},"ItemResults":[{"GeneratedId":null,"Status":"Success","Message":"DataLoader with file name \'delfil1.dat\', intials \'tbf\' and time \'29.09.2022 19:01:14\': Loading completed into temp table","Exception":null,"ValidationInfoItems":null},{"GeneratedId":null,"Status":"Success","Message":"ExecutePublish with AutoGodkjennData \'2\', AutoOverskrivData \'1\', Fagansvarlig1 \'tbf\', Fagansvarlig2 \'tbf\', Hovedtabell \'HovedTabellNavn\', Publiseringsdato \'07.01.2023 00:00:00\', Publiseringstid \'08:00\':  Status 0, OK, lasting er registrert med lasteoppdragsnummer:197885 => INFORMASJON. Publiseringen er satt til kl 08:00:00","Exception":null,"ValidationInfoItems":null}]}',
        "utf8",
    )
    response.request = requests.PreparedRequest()
    response.request.headers = {
        "Authorization": fake_auth,
        "Content-Type": "multipart/form-data; boundary=12345",
    }
    return response


@pytest.fixture
def empty_netrc_file(tmp_path: Path) -> Generator[Path]:
    netrc_path = tmp_path / "empty.netrc"
    yield netrc_path
    netrc_path.unlink(missing_ok=True)


@pytest.fixture
def existing_netrc_file(tmp_path: Path, fake_auth: str) -> Generator[Path]:
    netrc_path = tmp_path / "existing.netrc"
    netrc_path.write_text(
        f'machine fakeurl.com login ola password {fake_auth}\nmacdef init\n\techo "foobar"\n\n',
    )
    yield netrc_path
    netrc_path.unlink()
