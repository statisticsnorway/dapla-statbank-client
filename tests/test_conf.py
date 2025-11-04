# Mock for os.environ.get
import pytest
from furl import furl

from statbank.auth import StatbankConfig
from statbank.globals import DaplaRegion
from statbank.globals import UseDb

test_cases_prod = [
    (None, furl("https://fakeurl.com"), furl("https://fakeurl.com/encrypt")),
    (UseDb.PROD, furl("https://fakeurl.com"), furl("https://fakeurl.com/encrypt")),
    (
        UseDb.TEST,
        furl("https://test.fakeurl.com"),
        furl("https://test.fakeurl.com/encrypt"),
    ),
]

test_cases_test = [
    (None, furl("https://test.fakeurl.com"), furl("https://test.fakeurl.com/encrypt")),
    (
        UseDb.TEST,
        furl("https://test.fakeurl.com"),
        furl("https://test.fakeurl.com/encrypt"),
    ),
]


@pytest.mark.usefixtures("mock_environ_dapla_lab_prod")
@pytest.mark.parametrize(
    ("usedb", "expected_endpoint_base", "expected_encrypt_url"),
    test_cases_prod,
)
def test_config_on_dapla_lab_prod(
    usedb: UseDb,
    expected_endpoint_base: furl,
    expected_encrypt_url: furl,
):
    config = StatbankConfig.from_environ(usedb)

    assert config.endpoint_base == expected_endpoint_base
    assert config.encrypt_url == expected_encrypt_url
    assert config.region == DaplaRegion.DAPLA_LAB


@pytest.mark.usefixtures("mock_environ_dapla_lab_test")
@pytest.mark.parametrize(
    ("usedb", "expected_endpoint_base", "expected_encrypt_url"),
    test_cases_test,
)
def test_config_on_dapla_lab_test(
    usedb: UseDb,
    expected_endpoint_base: furl,
    expected_encrypt_url: furl,
):
    config = StatbankConfig.from_environ(use_db=usedb)

    assert config.endpoint_base == expected_endpoint_base
    assert config.encrypt_url == expected_encrypt_url
    assert config.region == DaplaRegion.DAPLA_LAB


@pytest.mark.usefixtures("mock_environ_dapla_lab_test")
def test_config_on_dapla_lab_test_raises():
    with pytest.raises(
        RuntimeError,
        match=r"Statbankens produksjonsmiljø ikke tilgjengelig fra Daplas testmiljø",
    ):
        StatbankConfig.from_environ(use_db=UseDb.PROD)


@pytest.mark.usefixtures("mock_environ_on_prem_prod")
@pytest.mark.parametrize(
    ("usedb", "expected_endpoint_base", "expected_encrypt_url"),
    test_cases_prod,
)
def test_config_on_on_prem_prod(
    usedb: UseDb,
    expected_endpoint_base: furl,
    expected_encrypt_url: furl,
):
    config = StatbankConfig.from_environ(use_db=usedb)

    assert config.endpoint_base == expected_endpoint_base
    assert config.encrypt_url == expected_encrypt_url
    assert config.region == DaplaRegion.ON_PREM
