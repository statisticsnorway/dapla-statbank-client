#!/usr/bin/env python3

import os
from unittest import mock
from datetime import datetime

import pandas as pd
import pytest
import requests
import ipywidgets as widgets

from statbank.transfer import StatbankTransfer
from statbank.uttrekk import StatbankUttrekksBeskrivelse
from statbank import StatbankClient


def fake_mail():
    return "ssb@ssb.no"


os.environ["JUPYTERHUB_USER"] = fake_mail()


@pytest.fixture(autouse=True)
def mock_settings_env_vars():
    with mock.patch.dict(
        os.environ,
        {
            "STATBANK_BASE_URL": "https://fake_url/",
            "STATBANK_ENCRYPT_URL": "https://fake_url2/",
            "JUPYTERHUB_USER": fake_mail(),
        },
    ):
        yield


# Fake Auth
def fake_user():
    return "SSB-person-456"


def fake_pass():
    return "coConU7s6"


def fake_auth():
    return "SoCipherVerySecure"


# Fake data
def fake_data():
    return {
        "delfil1.dat": pd.DataFrame(
            {
                "1": ["999", "01", "02"],
                "2": ["2022", "2022", "2000"],
                "3": [1.5, 2.5, 3.5],
                "4": [1.15, 2.25, 3.35],
            }
        ),
    }


# Fake responses from APIs
def fake_get_response_uttrekksbeskrivelse_successful():
    response = requests.Response()
    response.status_code = 200
    response._content = bytes(
        '{"Uttaksbeskrivelse_lagd":"29.09.2022 klokka 18:51" , "base": "DB1T","TabellId":"10000","Huvudtabell":"HovedTabellNavn","DeltabellTitler":[{ "Filnavn": "delfil1.dat" , "Filtext": "10000: Fake table" }] ,"deltabller":[{"deltabell":"delfil1.dat","variabler":[{"kolonnenummer":"1","Klassifikasjonsvariabel":"Kodeliste1","Variabeltext":"kodeliste1","Kodeliste_id":"Kodeliste1","Kodeliste_text":"Kodeliste 1"},{"kolonnenummer":"2","Klassifikasjonsvariabel":"Tid","Variabeltext":"tid","Kodeliste_id":"-","Kodeliste_text":"Tidsperioden for tabelldataene, enhet = år, format = åååå"}],"statistikkvariabler":[{ "kolonnenummer":"3","Text":"Antall","Enhet":"personer","Antall_lagrede_desimaler":"0","Antall_viste_desimaler":"0"}, { "kolonnenummer":"4","Text":"Antall","Enhet":"personer","Antall_lagrede_desimaler":"1","Antall_viste_desimaler":"1"}],"eksempel_linje":"01;2022;100"}],"kodelister":[{"kodeliste":"Kodeliste1","SumIALtTotalKode":"999","koder":[{"kode":"999","text":"i alt"},{"kode":"01","text":"Kode1"},{"kode":"02","text":"Kode2"}]}]}',
        "utf8",
    )
    response.request = requests.PreparedRequest()
    response.request.headers = {
        "Authorization": fake_auth(),
        "Content-Type": "multipart/form-data; boundary=12345",
    }
    return response


def fake_post_response_key_service():
    response = requests.Response()
    response.status_code = 200
    response._content = bytes('{"message":"' + fake_auth() + '"}', "utf8")
    return response


def fake_post_response_transfer_successful():
    response = requests.Response()
    response.status_code = 200
    response._content = bytes(
        '{"TotalResult":{"GeneratedId":null,"Status":"Success","Message":"ExecutePublish with AutoGodkjennData \'2\', AutoOverskrivData \'1\', Fagansvarlig1 \'tbf\', Fagansvarlig2 \'tbf\', Hovedtabell \'HovedTabellNavn\', Publiseringsdato \'07.01.2023 00:00:00\', Publiseringstid \'08:00\':  Status 0, OK, lasting er registrert med lasteoppdragsnummer:197885 => INFORMASJON. Publiseringen er satt til kl 08:00:00","Exception":null,"ValidationInfoItems":null},"ItemResults":[{"GeneratedId":null,"Status":"Success","Message":"DataLoader with file name \'delfil1.dat\', intials \'tbf\' and time \'29.09.2022 19:01:14\': Loading completed into temp table","Exception":null,"ValidationInfoItems":null},{"GeneratedId":null,"Status":"Success","Message":"ExecutePublish with AutoGodkjennData \'2\', AutoOverskrivData \'1\', Fagansvarlig1 \'tbf\', Fagansvarlig2 \'tbf\', Hovedtabell \'HovedTabellNavn\', Publiseringsdato \'07.01.2023 00:00:00\', Publiseringstid \'08:00\':  Status 0, OK, lasting er registrert med lasteoppdragsnummer:197885 => INFORMASJON. Publiseringen er satt til kl 08:00:00","Exception":null,"ValidationInfoItems":null}]}',
        "utf8",
    )
    response.request = requests.PreparedRequest()
    response.request.headers = {
        "Authorization": fake_auth(),
        "Content-Type": "multipart/form-data; boundary=12345",
    }
    return response


# Successful fixtures

# Our only get-request is for the "uttrekksbeskrivelse"
@pytest.fixture
@mock.patch.object(StatbankUttrekksBeskrivelse, "_make_request")
@mock.patch.object(StatbankUttrekksBeskrivelse, "_encrypt_request")
def uttrekksbeskrivelse_success(test_encrypt, test_make_request):
    test_make_request.return_value = fake_get_response_uttrekksbeskrivelse_successful()
    test_encrypt.return_value = fake_post_response_key_service()
    return StatbankUttrekksBeskrivelse("10000", fake_user())


@pytest.fixture
@mock.patch.object(StatbankTransfer, "_make_transfer_request")
@mock.patch.object(StatbankTransfer, "_encrypt_request")
def transfer_success(
    test_transfer_encrypt,
    test_transfer_make_request,
):
    test_transfer_make_request.return_value = fake_post_response_transfer_successful()
    test_transfer_encrypt.return_value = fake_post_response_key_service()
    return StatbankTransfer(fake_data(), "10000", fake_user())


@pytest.fixture
@mock.patch.object(StatbankClient, "_encrypt_request")
def client_fake(encrypt_fake):
    encrypt_fake.return_value = fake_post_response_key_service()
    return StatbankClient(fake_user())

@mock.patch.object(StatbankClient, "_encrypt_request")
def test_client_no_loaduser_set(encrypt_fake):
    encrypt_fake.return_value = fake_post_response_key_service()
    with pytest.raises(Exception) as e_info:
        StatbankClient(1)

@mock.patch.object(StatbankClient, "_encrypt_request")
def test_client_approve_wrong_datatype(encrypt_fake):
    encrypt_fake.return_value = fake_post_response_key_service()
    with pytest.raises(Exception) as e_info:
        StatbankClient(fake_user(), approve="1")
        
@mock.patch.object(StatbankClient, "_encrypt_request")
def test_client_overwrite_wrong_datatype(encrypt_fake):
    encrypt_fake.return_value = fake_post_response_key_service()
    with pytest.raises(Exception) as e_info:
        StatbankClient(fake_user(), overwrite="1")

def test_client_print(client_fake):
    assert len(client_fake.__str__())
    assert isinstance(client_fake.__str__(), str)
    
def test_client_repr(client_fake):
    assert len(client_fake.__repr__())
    assert isinstance(client_fake.__repr__(), str)

@mock.patch.object(StatbankClient, "_encrypt_request")
def test_client_with_str_date(encrypt_fake):
    encrypt_fake.return_value = fake_post_response_key_service()
    client = StatbankClient(fake_user(), "2050-01-01")
    assert isinstance(client.date, datetime)

def test_client_date_picker_is_widget(client_fake):
    widg = client_fake.date_picker()
    assert isinstance(widg, widgets.widget_date.DatePicker)

def test_client_set_date_str(client_fake):
    client_fake.set_publish_date("2050-11-11")
    assert "Date set to " in client_fake.log[-1]
    
def test_client_set_date_int_raises(client_fake):
    with pytest.raises(Exception) as e_info:
        client_fake.set_publish_date(1)
    
    
def test_client_set_date_datetime(client_fake):
    client_fake.set_publish_date(datetime.now())
    assert "Date set to " in client_fake.log[-1]
    
def test_client_set_date_widget(client_fake):
    widg = client_fake.date_picker()
    client_fake.set_publish_date(widg)
    assert "Date set to " in client_fake.log[-1]

@mock.patch.object(StatbankUttrekksBeskrivelse, "_make_request")
@mock.patch.object(StatbankUttrekksBeskrivelse, "_encrypt_request")
def test_client_get_uttrekk(test_encrypt, test_make_request, client_fake):
    test_make_request.return_value = fake_get_response_uttrekksbeskrivelse_successful()
    test_encrypt.return_value = fake_post_response_key_service()
    desc = client_fake.get_description("10000")
    assert desc.tableid == "10000"

@mock.patch.object(StatbankUttrekksBeskrivelse, "_make_request")
@mock.patch.object(StatbankUttrekksBeskrivelse, "_encrypt_request")
def test_client_validate_no_errors(test_encrypt, test_make_request, client_fake, uttrekksbeskrivelse_success):
    test_make_request.return_value = fake_get_response_uttrekksbeskrivelse_successful()
    test_encrypt.return_value = fake_post_response_key_service(),
    data = uttrekksbeskrivelse_success.round_data(fake_data())
    errors = client_fake.validate(data, "10000")
    assert not len(errors)

def test_client_get_uttrekk_tableid_non_string(client_fake):
    with pytest.raises(Exception) as e_info:
        client_fake.get_description(10000)
        
def test_client_get_uttrekk_tableid_wrong_length(client_fake):
    with pytest.raises(Exception) as e_info:
        client_fake.get_description("1")

def test_uttrekksbeskrivelse_has_kodelister(uttrekksbeskrivelse_success):
    # last thing to get filled during __init__ is .kodelister, check that dict has length
    assert len(uttrekksbeskrivelse_success.codelists)

def test_uttrekk_json_write_read(uttrekksbeskrivelse_success, client_fake):
    json_file_path = "test_uttrekk.json"
    uttrekksbeskrivelse_success.to_json(json_file_path)
    test_uttrekk = client_fake.read_description_json(json_file_path)
    os.remove(json_file_path)
    assert len(test_uttrekk.codelists)
    
def test_transfer_json_write_read(transfer_success, client_fake):
    json_file_path = "test_transfer.json"
    transfer_success.to_json(json_file_path)
    test_transfer = client_fake.read_transfer_json(json_file_path)
    os.remove(json_file_path)
    assert test_transfer.oppdragsnummer.isdigit()

def test_round_data_0decimals(uttrekksbeskrivelse_success):
    subtable_name = list(fake_data().keys())[0]
    dict_rounded = fake_data().copy()
    df_test_rounded = dict_rounded[subtable_name]
    df_test_rounded["3"] = pd.Series(["2,2", "3,3", "4,4"])
    df_actual_rounded = uttrekksbeskrivelse_success.round_data(dict_rounded)[
        subtable_name
    ]
    print(df_test_rounded.compare(df_actual_rounded))
    assert df_test_rounded["3"].equals(df_actual_rounded["3"])


def test_round_data_1decimals(uttrekksbeskrivelse_success):
    subtable_name = list(fake_data().keys())[0]
    dict_rounded = fake_data().copy()
    df_test_rounded = dict_rounded[subtable_name]
    df_test_rounded["4"] = pd.Series(["1,2", "2,3", "3,4"])
    df_actual_rounded = uttrekksbeskrivelse_success.round_data(dict_rounded)[
        subtable_name
    ]
    print(df_test_rounded.compare(df_actual_rounded))
    assert df_test_rounded["4"].equals(df_actual_rounded["4"])

    
def test_check_round_data_manages_punctum(uttrekksbeskrivelse_success):
    subtable_name = list(fake_data().keys())[0]
    datadict = fake_data().copy()
    datadict[subtable_name]["4"] = pd.Series(["1.2", "2.3", "3.4"])
    # Fails on validate raising error
    uttrekksbeskrivelse_success.validate(datadict)
    
def test_check_round_data_manages_punctum_raises_error(uttrekksbeskrivelse_success):
    subtable_name = list(fake_data().keys())[0]
    datadict = fake_data().copy()
    datadict[subtable_name]["4"] = pd.Series(["1.15", "2.25", "3.35"])
    # Fails on validate raising error
    uttrekksbeskrivelse_success.validate(datadict)
    
def test_transfer_correct_entry(transfer_success):
    # "Lastenummer" is one of the last things set by __init__ and signifies a correctly loaded data-transfer.
    # Is also used to build urls to webpages showing the ingestion status
    assert transfer_success.oppdragsnummer.isdigit()


def test_transfer_no_auth_residuals(transfer_success):
    # Do a search for the key, password, and ciphered auth in the returned object.
    # Important to remove any traces of these before object is handed to user

    # Username should be in object (checks integrity of object, and validity of search)
    assert len(search__dict__(transfer_success, fake_user(), keep={}))

    # Make sure none of these are in the object for security
    assert 0 == len(search__dict__(transfer_success, fake_pass(), keep={}))
    assert 0 == len(search__dict__(transfer_success, fake_auth(), keep={}))


def search__dict__(obj, searchterm: str, path="root", keep={}):  # noqa: B006
    """Recursive search through all nested objects having a __dict__-attribute"""
    if hasattr(obj, "__dict__"):
        for key, elem in obj.__dict__.items():
            if hasattr(elem, "__dict__"):
                path = path + "/" + key
                keep = search__dict__(elem, searchterm, path=path, keep=keep)
            if (
                searchterm.lower() in str(elem).lower()
                or searchterm.lower() in str(key).lower()
            ):
                keep[path + "/" + key] = elem
    return keep

@mock.patch.object(StatbankTransfer, "_make_transfer_request")
@mock.patch.object(StatbankTransfer, "_encrypt_request")
def test_client_transfer(
    test_transfer_encrypt,
    test_transfer_make_request, 
    client_fake
):
    test_transfer_make_request.return_value = fake_post_response_transfer_successful()
    test_transfer_encrypt.return_value = fake_post_response_key_service()
    transfer = client_fake.transfer(fake_data(), "10000")