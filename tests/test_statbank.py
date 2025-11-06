from __future__ import annotations

import datetime
import getpass
import json
import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock

import ipywidgets as widgets
import pandas as pd
import pytest
import requests
import requests.auth
from furl import furl
from typeguard import suppress_type_checks

from statbank import StatbankClient
from statbank.globals import OSLO_TIMEZONE
from statbank.transfer import StatbankTransfer
from statbank.uttrekk import StatbankUttrekksBeskrivelse
from statbank.uttrekk import UttrekksBeskrivelseData
from statbank.uttrekk_validations import StatbankValidateError

if TYPE_CHECKING:
    from collections.abc import Callable

    from statbank.auth import StatbankConfig


def test_round_up_zero():
    assert StatbankUttrekksBeskrivelse._round(0.0, 0) == "0"  # noqa: SLF001


def fake_user():
    return "tbf"


# Successful fixtures
@pytest.fixture
def uttrekksbeskrivelse_success(
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
):
    bdata = UttrekksBeskrivelseData(
        tableid=10000,
        tablename="HovedTabellNavn",
        time_retrieved=datetime.datetime(  # noqa: DTZ001
            day=29,
            month=9,
            year=2022,
            hour=18,
            minute=51,
        ),
        subtables={"delfil1.dat": "10000: Fake table"},
        variables=[
            {
                "deltabell": "delfil1.dat",
                "variabler": [
                    {
                        "kolonnenummer": "1",
                        "Klassifikasjonsvariabel": "Kodeliste1",
                        "Variabeltext": "kodeliste1",
                        "Kodeliste_id": "Kodeliste1",
                        "Kodeliste_text": "Kodeliste 1",
                    },
                    {
                        "kolonnenummer": "2",
                        "Klassifikasjonsvariabel": "Tid",
                        "Variabeltext": "tid",
                        "Kodeliste_id": "-",
                        "Kodeliste_text": "Tidsperioden for tabelldataene, enhet = år, format = åååå",
                    },
                ],
                "statistikkvariabler": [
                    {
                        "kolonnenummer": "3",
                        "Text": "Antall",
                        "Enhet": "personer",
                        "Antall_lagrede_desimaler": "0",
                        "Antall_viste_desimaler": "0",
                    },
                    {
                        "kolonnenummer": "4",
                        "Text": "Antall",
                        "Enhet": "personer",
                        "Antall_lagrede_desimaler": "1",
                        "Antall_viste_desimaler": "1",
                    },
                ],
                "eksempel_linje": "01;2022;100",
            },
        ],
        codelists={
            "Kodeliste1": {
                "SumIALtTotalKode": "999",
                "koder": {"999": "i alt", "01": "Kode1", "02": "Kode2"},
            },
        },
        suppression=None,
    )

    return StatbankUttrekksBeskrivelse(
        data=bdata,
        raise_errors=True,
        config=config_fixture,
        auth=auth_fixture,
    )


def test_uttrekksbeskrivelse_str_contains_core_info(
    uttrekksbeskrivelse_success: StatbankUttrekksBeskrivelse,
):
    s = str(uttrekksbeskrivelse_success)
    assert "Uttrekksbeskrivelse for statbanktabell 10000" in s
    assert "Deltabell (DataFrame) nummer 1" in s
    # From fixture data: includes codelist text and example line
    assert "Kodeliste 1" in s
    assert "Antall" in s
    assert "Eksempellinje: 01;2022;100" in s


def test_uttrekksbeskrivelse_repr_format(
    uttrekksbeskrivelse_success: StatbankUttrekksBeskrivelse,
):
    r = repr(uttrekksbeskrivelse_success)
    assert r == 'StatbankUttrekksBeskrivelse(tableid="10000",)'


def test_transferdata_template_without_dfs(
    uttrekksbeskrivelse_success: StatbankUttrekksBeskrivelse,
):
    template = uttrekksbeskrivelse_success.transferdata_template()
    assert template == {"delfil1.dat": "df0"}


def test_transferdata_template_with_dfs(
    uttrekksbeskrivelse_success: StatbankUttrekksBeskrivelse,
):
    df_test_template = pd.DataFrame({"A": [1, 2]})
    template = uttrekksbeskrivelse_success.transferdata_template([df_test_template])
    assert list(template.keys()) == ["delfil1.dat"]
    assert template["delfil1.dat"] is df_test_template


def test_transferdata_template_raises_for_wrong_len(
    uttrekksbeskrivelse_success: StatbankUttrekksBeskrivelse,
):
    with pytest.raises(KeyError):
        uttrekksbeskrivelse_success.transferdata_template([])


def test_transferdata_template_raises_for_non_df(
    uttrekksbeskrivelse_success: StatbankUttrekksBeskrivelse,
):
    with pytest.raises(TypeError):
        uttrekksbeskrivelse_success.transferdata_template(["not a df"])  # type: ignore[list-item]


@mock.patch.object(StatbankTransfer, "_make_transfer_request")
def test_transfer_date_is_string(
    mock_transfer_make_request: mock.Mock,
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
    transfer_data_fixture: dict[str, pd.DataFrame],
    fake_post_response_transfer_successful: requests.Response,
):
    mock_transfer_make_request.return_value = fake_post_response_transfer_successful
    trans = StatbankTransfer(
        transfer_data_fixture,
        "10000",
        date="2050-01-01",
        config=config_fixture,
        auth=auth_fixture,
    )
    assert trans.oppdragsnummer.isdigit()


@mock.patch.object(StatbankTransfer, "_make_transfer_request")
def test_transfer_date_is_invalid_string_raises(
    mock_transfer_make_request: mock.Mock,
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
    transfer_data_fixture: dict[str, pd.DataFrame],
    fake_post_response_transfer_successful: requests.Response,
):
    mock_transfer_make_request.return_value = fake_post_response_transfer_successful
    with pytest.raises(
        TypeError,
        match="Skriv inn datoformen for publisering som 1900-01-01",
    ) as _:
        StatbankTransfer(
            transfer_data_fixture,
            "10000",
            date="205000-01-01",
            config=config_fixture,
            auth=auth_fixture,
        )


@mock.patch.object(StatbankTransfer, "_make_transfer_request")
def test_str_transfer_on_delay_and_after(
    mock_transfer_make_request: mock.Mock,
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
    transfer_data_fixture: dict[str, pd.DataFrame],
    fake_post_response_transfer_successful: requests.Response,
):
    mock_transfer_make_request.return_value = fake_post_response_transfer_successful
    trans = StatbankTransfer(
        transfer_data_fixture,
        "10000",
        date="2050-01-01",
        delay=True,
        config=config_fixture,
        auth=auth_fixture,
    )
    assert "Ikke overført enda" in trans.__str__()
    trans.transfer()
    assert len(trans.__str__())
    assert "Ikke overført enda" not in trans.__str__()


@suppress_type_checks
@mock.patch.object(StatbankTransfer, "_make_transfer_request")
def test_transfer_overwrite_wrong_format(
    mock_transfer_make_request: mock.Mock,
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
    transfer_data_fixture: dict[str, pd.DataFrame],
    fake_post_response_transfer_successful: requests.Response,
):
    mock_transfer_make_request.return_value = fake_post_response_transfer_successful
    with pytest.raises(TypeError, match="Sett overwrite") as _:
        StatbankTransfer(
            transfer_data_fixture,
            "10000",
            overwrite=1,
            config=config_fixture,
            auth=auth_fixture,
        )


@suppress_type_checks
@mock.patch.object(StatbankTransfer, "_make_transfer_request")
def test_transfer_approve_wrong_format(
    mock_transfer_make_request: Callable,
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
    transfer_data_fixture: dict[str, pd.DataFrame],
    fake_post_response_transfer_successful: requests.Response,
):
    mock_transfer_make_request.return_value = fake_post_response_transfer_successful
    with pytest.raises(TypeError, match="approve") as _:
        StatbankTransfer(
            transfer_data_fixture,
            "10000",
            approve={"1"},
            config=config_fixture,
            auth=auth_fixture,
        )


def test_transfer_request_raises_error(
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
    transfer_data_fixture: dict[str, pd.DataFrame],
):
    # Mock the r.post method
    with mock.patch("statbank.transfer.r.post") as mock_post:
        # Create a mock response object
        mock_response = mock.Mock()
        # Set up the mock to raise an HTTPError when raise_for_status is called
        mock_response.raise_for_status.side_effect = requests.HTTPError()
        mock_post.return_value = mock_response

        transfer = StatbankTransfer(
            transfer_data_fixture,
            tableid="10000",
            config=config_fixture,
            auth=auth_fixture,
            delay=True,
        )

        transfer.body = transfer._body_from_data()  # noqa: SLF001

        # Now, assert that the _make_transfer_request method raises an HTTPError
        with pytest.raises(requests.HTTPError):
            transfer._make_transfer_request(  # noqa: SLF001
                furl("mock_url_params"),
                {},
            )


@suppress_type_checks
@mock.patch.object(StatbankTransfer, "_make_transfer_request")
def test_transfer_approve_int_intstr_str(
    mock_transfer_make_request: mock.Mock,
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
    transfer_data_fixture: dict[str, pd.DataFrame],
    fake_post_response_transfer_successful: requests.Response,
):
    mock_transfer_make_request.return_value = fake_post_response_transfer_successful

    assert StatbankTransfer(
        transfer_data_fixture,
        "10000",
        approve=1,
        config=config_fixture,
        auth=auth_fixture,
    ).oppdragsnummer.isdigit()

    assert StatbankTransfer(
        transfer_data_fixture,
        "10000",
        approve="1",
        config=config_fixture,
        auth=auth_fixture,
    ).oppdragsnummer.isdigit()

    assert StatbankTransfer(
        transfer_data_fixture,
        "10000",
        approve="MANUAL",
        config=config_fixture,
        auth=auth_fixture,
    ).oppdragsnummer.isdigit()

    params_dict: dict[str, str] = StatbankTransfer(  # noqa: SLF001
        transfer_data_fixture,
        "10000",
        approve="MANUAL",
        config=config_fixture,
        auth=auth_fixture,
    )._build_params()
    for v in params_dict.values():
        assert isinstance(v, str)
        assert "Approve." not in v


def test_repr_transfer(transfer_fixture: StatbankTransfer):
    assert "StatbankTransfer" in transfer_fixture.__repr__()


def test_transfer_to_json_return_jsonstring(transfer_fixture: StatbankTransfer):
    # Will throw error and fail test if json string cant be loaded as json
    json.loads(transfer_fixture.to_json())


def test_transfer_cant_transfer_twice_raises(transfer_fixture: StatbankTransfer):
    with pytest.raises(ValueError, match="Already transferred") as _:
        transfer_fixture.transfer()


@mock.patch.object(StatbankTransfer, "_make_transfer_request")
def test_transfer_shortuser_wrong_raises(
    mock_transfer_make_request: mock.Mock,
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
    transfer_data_fixture: dict[str, pd.DataFrame],
    fake_post_response_transfer_successful: requests.Response,
):
    mock_transfer_make_request.return_value = fake_post_response_transfer_successful
    with pytest.raises(ValueError, match="trebokstavsforkortelse") as _:
        StatbankTransfer(
            transfer_data_fixture,
            "10000",
            date="2050-01-01",
            shortuser="aa",
            config=config_fixture,
            auth=auth_fixture,
        )


@mock.patch.object(StatbankTransfer, "_make_transfer_request")
def test_transfer_loaduser_still(
    test_transfer_make_request: mock.Mock,
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
    transfer_data_fixture: dict[str, pd.DataFrame],
    fake_post_response_transfer_successful: requests.Response,
):
    test_transfer_make_request.return_value = fake_post_response_transfer_successful
    with pytest.raises(ValueError, match="Loaduser") as _:
        StatbankTransfer(
            transfer_data_fixture,
            fake_user(),
            "10000",
            auth=auth_fixture,
            config=config_fixture,
        )


@mock.patch.object(StatbankClient, "_get_user_initials")
def test_client_set_approve_overwrite(
    mock_get_user_initials: mock.Mock,
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
):
    mock_get_user_initials.return_value = fake_user()
    client = StatbankClient(
        check_username_password=False,
        overwrite=False,
        approve=1,
        auth=auth_fixture,
        config=config_fixture,
    )
    assert "overwrite" in client.__repr__()
    assert "approve" in client.__repr__()


@suppress_type_checks
@mock.patch.object(StatbankClient, "_get_user_initials")
def test_client_approve_wrong_datatype(
    mock_get_user_initials: mock.Mock,
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
):
    mock_get_user_initials.return_value = fake_user()
    with pytest.raises(TypeError, match="handle approve") as _:
        StatbankClient(
            approve=[1],
            check_username_password=False,
            auth=auth_fixture,
            config=config_fixture,
        )


@suppress_type_checks
@mock.patch.object(StatbankClient, "_get_user_initials")
def test_client_overwrite_wrong_datatype(
    mock_get_user_initials: mock.Mock,
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
):
    mock_get_user_initials.return_value = fake_user()
    with pytest.raises(TypeError, match="overwrite") as _:
        StatbankClient(
            overwrite="1",
            check_username_password=False,
            auth=auth_fixture,
            config=config_fixture,
        )


@suppress_type_checks
@mock.patch.object(StatbankClient, "_get_user_initials")
def test_client_date_wrong_datatype(
    mock_get_user_initials: mock.Mock,
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
):
    mock_get_user_initials.return_value = fake_user()
    with pytest.raises(TypeError, match="Date must be a datetime") as _:
        StatbankClient(
            check_username_password=False,
            date=1,
            auth=auth_fixture,
            config=config_fixture,
        )


@mock.patch.object(StatbankClient, "_get_user_initials")
def test_client_print(client_fixture: StatbankClient):
    assert len(client_fixture.__str__())
    assert isinstance(client_fixture.__str__(), str)


@mock.patch.object(StatbankClient, "_get_user_initials")
def test_client_repr(client_fixture: StatbankClient):
    assert len(client_fixture.__repr__())
    assert isinstance(client_fixture.__repr__(), str)


@mock.patch.object(StatbankClient, "_get_user_initials")
def test_client_with_str_date(
    mock_get_user_initials: mock.Mock,
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
):
    mock_get_user_initials.return_value = fake_user()
    client = StatbankClient(
        "2050-01-01",
        check_username_password=False,
        auth=auth_fixture,
        config=config_fixture,
    )
    assert isinstance(client.date, datetime.date)


@mock.patch.object(StatbankClient, "_get_user_initials")
def test_client_loaduser_still(
    mock_get_user_initials: mock.Mock,
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
):
    mock_get_user_initials.return_value = fake_user()
    with pytest.raises(ValueError, match="Loaduser"):
        StatbankClient(
            fake_user(),
            "2050-01-01",
            check_username_password=False,
            auth=auth_fixture,
            config=config_fixture,
        )


def test_client_date_picker_is_widget(client_fixture: StatbankClient):
    widg = client_fixture.date_picker()
    assert isinstance(widg, widgets.widget_date.DatePicker)


def test_client_set_date_str(client_fixture: StatbankClient):
    client_fixture.set_publish_date("2050-11-11")
    assert "Date set to " in client_fixture.log[-1]


@suppress_type_checks
def test_client_set_date_int_raises(client_fixture: StatbankClient):
    with pytest.raises(
        TypeError,
        match=r"must be a string, datetime, or ipywidgets.DatePicker",
    ) as _:
        client_fixture.set_publish_date(1)


def test_client_set_date_datetime(client_fixture: StatbankClient):
    client_fixture.set_publish_date(
        datetime.datetime.now().astimezone(OSLO_TIMEZONE),
    )
    assert "Date set to " in client_fixture.log[-1]


def test_client_set_date_widget(client_fixture: StatbankClient):
    widg = client_fixture.date_picker()
    client_fixture.set_publish_date(widg)
    assert "Date set to " in client_fixture.log[-1]


@mock.patch.object(StatbankUttrekksBeskrivelse, "_make_request")
def test_client_get_uttrekk(
    mock_make_request: mock.Mock,
    client_fixture: StatbankClient,
    fake_get_response_uttrekksbeskrivelse_successful: requests.Response,
):
    mock_make_request.return_value = fake_get_response_uttrekksbeskrivelse_successful
    desc = client_fixture.get_description("10000")
    assert desc.tableid == "10000"


@mock.patch.object(StatbankUttrekksBeskrivelse, "_make_request")
def test_client_get_uttrekk_test_db(
    test_make_request: Callable,
    client_fixture: StatbankClient,
    caplog: pytest.LogCaptureFixture,
    fake_get_response_uttrekksbeskrivelse_successful: requests.Response,
):
    test_make_request.return_value = fake_get_response_uttrekksbeskrivelse_successful
    with caplog.at_level(logging.WARNING):
        desc = client_fixture.get_description("10000")
    assert desc.tableid == "10000"
    assert any(
        ("metadata" in message.lower() and "utdatert" in message.lower())
        for message in caplog.messages
    )


@mock.patch.object(StatbankUttrekksBeskrivelse, "_make_request")
def test_client_validate_no_errors(
    mock_uttrekksbeskrivelse_make_request: mock.Mock,
    uttrekksbeskrivelse_success: StatbankUttrekksBeskrivelse,
    config_fixture: StatbankConfig,
    auth_fixture: requests.auth.AuthBase,
    transfer_data_fixture: dict[str, pd.DataFrame],
    fake_get_response_uttrekksbeskrivelse_successful: requests.Response,
):
    mock_uttrekksbeskrivelse_make_request.return_value = (
        fake_get_response_uttrekksbeskrivelse_successful
    )
    data = uttrekksbeskrivelse_success.round_data(transfer_data_fixture)
    errors = StatbankClient(
        config=config_fixture,
        auth=auth_fixture,
        check_username_password=False,
    ).validate(data, "10000")
    assert not len(errors)


@suppress_type_checks
def test_client_get_uttrekk_tableid_non_string(client_fixture: StatbankClient):
    with pytest.raises(TypeError, match="not a string") as _:
        client_fixture.get_description(10000)


@suppress_type_checks
def test_client_get_uttrekk_tableid_wrong_length(client_fixture: StatbankClient):
    with pytest.raises(ValueError, match="is numeric, but not") as _:
        client_fixture.get_description("1")


@mock.patch.object(StatbankUttrekksBeskrivelse, "_make_request")
def test_uttrekk_works_no_codelists(
    mock_uttrekksbeskrivelse_make_request: mock.Mock,
    client_fixture: StatbankClient,
    fake_get_response_uttrekksbeskrivelse_successful: requests.Response,
):
    mock_uttrekksbeskrivelse_make_request.return_value = (
        fake_get_response_uttrekksbeskrivelse_successful
    )
    uttrekk = fake_get_response_uttrekksbeskrivelse_successful
    uttrekk._content = bytes(  # noqa: SLF001
        uttrekk._content.decode().replace(  # noqa: SLF001
            ',"kodelister":[{"kodeliste":"Kodeliste1","SumIALtTotalKode":"999","koder":[{"kode":"999","text":"i alt"},{"kode":"01","text":"Kode1"},{"kode":"02","text":"Kode2"}]}]',
            "",
        ),
        "utf8",
    )
    desc = client_fixture.get_description("10000")
    assert desc.tableid == "10000"


def test_uttrekksbeskrivelse_has_kodelister(
    uttrekksbeskrivelse_success: StatbankUttrekksBeskrivelse,
):
    # last thing to get filled during __init__ is .kodelister, check that dict has length
    assert len(uttrekksbeskrivelse_success.codelists)


def test_uttrekksbeskrivelse_can_make_totals(
    uttrekksbeskrivelse_success: StatbankUttrekksBeskrivelse,
):
    result = uttrekksbeskrivelse_success.get_totalcodes_dict()
    assert isinstance(result, dict)
    assert len(result)


def test_uttrekk_json_write_read(
    uttrekksbeskrivelse_success: Callable,
    client_fixture: StatbankClient,
):
    json_file_path = "test_uttrekk.json"
    uttrekksbeskrivelse_success.to_json(json_file_path)
    test_uttrekk = client_fixture.read_description_json(json_file_path)
    Path(json_file_path).unlink()
    assert len(test_uttrekk.codelists)


def test_uttrekk_json_write_read_str(
    uttrekksbeskrivelse_success: Callable,
    client_fixture: StatbankClient,
):
    json_file_path = "test_uttrekk.json"
    uttrekksbeskrivelse_success.to_json(json_file_path)
    with Path(json_file_path).open() as f:
        content = f.read()
    test_uttrekk = client_fixture.read_description_json(content)
    Path(json_file_path).unlink()
    assert len(test_uttrekk.codelists)


def test_transfer_json_write_read(
    transfer_fixture: StatbankTransfer,
    client_fixture: StatbankClient,
):
    json_file_path = "test_transfer.json"
    transfer_fixture.to_json(json_file_path)
    test_transfer = client_fixture.read_transfer_json(json_file_path)
    Path(json_file_path).unlink()
    assert test_transfer.oppdragsnummer.isdigit()


def test_transfer_json_write_read_str(
    transfer_fixture: StatbankTransfer,
    client_fixture: StatbankClient,
):
    json_file_path = "test_transfer.json"
    transfer_fixture.to_json(json_file_path)
    with Path(json_file_path).open() as f:
        content = f.read()
    test_transfer = client_fixture.read_transfer_json(content)
    Path(json_file_path).unlink()
    assert test_transfer.oppdragsnummer.isdigit()


def test_round_data_0decimals(
    uttrekksbeskrivelse_success: StatbankUttrekksBeskrivelse,
    transfer_data_fixture: dict[str, pd.DataFrame],
):
    subtable_name = next(iter(transfer_data_fixture))
    dict_rounded = transfer_data_fixture.copy()
    df_test_rounded = dict_rounded[subtable_name]
    df_test_rounded["3"] = pd.Series(["2,2", "3,3", "4,4"])
    df_actual_rounded = uttrekksbeskrivelse_success.round_data(dict_rounded)[
        subtable_name
    ]
    assert df_test_rounded["3"].equals(df_actual_rounded["3"])


def test_round_data_1decimals(
    uttrekksbeskrivelse_success: StatbankUttrekksBeskrivelse,
    transfer_data_fixture: dict[str, pd.DataFrame],
):
    subtable_name = next(iter(transfer_data_fixture))
    dict_rounded = transfer_data_fixture.copy()
    df_test_rounded = dict_rounded[subtable_name]
    df_test_rounded["4"] = pd.Series(["1,2", "2,3", "3,4"])
    df_actual_rounded = uttrekksbeskrivelse_success.round_data(dict_rounded)[
        subtable_name
    ]
    assert df_test_rounded["4"].equals(df_actual_rounded["4"])


def test_check_round_data_manages_punctum(
    uttrekksbeskrivelse_success: StatbankUttrekksBeskrivelse,
    transfer_data_fixture: dict[str, pd.DataFrame],
):
    subtable_name = next(iter(transfer_data_fixture))
    datadict = transfer_data_fixture.copy()
    datadict = uttrekksbeskrivelse_success.round_data(datadict)
    datadict[subtable_name]["4"] = pd.Series(["1.2", "2.3", "3.4"])
    uttrekksbeskrivelse_success.validate(datadict)


def test_check_round_data_manages_punctum_raises_error(
    uttrekksbeskrivelse_success: StatbankUttrekksBeskrivelse,
    transfer_data_fixture: dict[str, pd.DataFrame],
):
    subtable_name = next(iter(transfer_data_fixture))
    datadict = transfer_data_fixture.copy()
    datadict = uttrekksbeskrivelse_success.round_data(datadict)
    datadict[subtable_name]["4"] = pd.Series(["1.15", "2.25", "3.35"])
    with pytest.raises(StatbankValidateError):
        uttrekksbeskrivelse_success.validate(datadict)


def test_transfer_correct_entry(transfer_fixture: StatbankTransfer):
    # "Lastenummer" is one of the last things set by __init__ and signifies a correctly loaded data-transfer.
    # Is also used to build urls to webpages showing the ingestion status
    assert transfer_fixture.oppdragsnummer.isdigit()


@mock.patch.object(StatbankTransfer, "_make_transfer_request")
def test_client_transfer(
    test_transfer_make_request: Callable,
    client_fixture: StatbankClient,
    transfer_data_fixture: dict[str, pd.DataFrame],
    fake_post_response_transfer_successful: requests.Response,
):
    test_transfer_make_request.return_value = fake_post_response_transfer_successful
    client_fixture.transfer(transfer_data_fixture, "10000")


@mock.patch("builtins.input")
@mock.patch.object(getpass, "getuser")
@mock.patch.object(subprocess, "check_output")
@mock.patch.object(os.environ, "get")
def test_get_user_initials(
    mock_environ_get: Callable,
    mock_check_output: Callable,
    mock_getuser: Callable,
    mock_input: Callable,
):
    # Test when os.environ.get("DAPLA_USER") provides a valid value
    mock_environ_get.side_effect = lambda key, default="": (
        "usr@ssb.no" if key == "DAPLA_USER" else default
    )
    assert StatbankClient._get_user_initials() == "usr"  # noqa: SLF001

    mock_environ_get.side_effect = lambda key, default="": (
        "jup@ssb.no" if key == "JUPYTERHUB_USER" else default
    )
    assert StatbankClient._get_user_initials() == "jup"  # noqa: SLF001
    mock_environ_get.side_effect = (
        lambda _, default="": default
    )  # Reset  env var to not existing

    # Test when os.environ.get("DAPLA_USER") and JUPYTERHUB_USER are empty, fallback to git config
    git_path = shutil.which("git")
    if isinstance(
        git_path,
        str,
    ):  # Only test with git, if git is installed on the system? (windows on github doesnt?)
        mock_check_output.return_value = b"tba@ssb.no"
        assert StatbankClient._get_user_initials() == "tba"  # noqa: SLF001
        # Test when os.environ.get and git config fail, fallback to getpass.getuser
        mock_check_output.side_effect = subprocess.CalledProcessError(
            1,
            f"{git_path} config user.email",
        )
        mock_check_output.return_value = b""  # Reset after git
    mock_getuser.return_value = "tbb@ssb.no"
    # Reset mock_check_output to avoid the error persisting into this test
    mock_check_output.side_effect = None
    assert StatbankClient._get_user_initials() == "tbb"  # noqa: SLF001

    # Test when all other methods fail, fallback to user input
    mock_getuser.return_value = ""
    mock_input.return_value = "tbc@ssb.no"
    assert StatbankClient._get_user_initials() == "tbc"  # noqa: SLF001

    # Test when no valid initials can be found, ensure ValueError is raised
    mock_input.return_value = ""
    with pytest.raises(
        ValueError,
        match=r"Can't find the users email or initials in the system.",
    ):
        StatbankClient._get_user_initials()  # noqa: SLF001
