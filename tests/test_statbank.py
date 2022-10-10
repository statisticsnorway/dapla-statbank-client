#!/usr/bin/env python3

from statbank import *
import pandas as pd
import pytest
from unittest import mock
import requests
import os

def fake_mail():
    return "ssb@ssb.no"

os.environ["JUPYTERHUB_USER"] = fake_mail()

@pytest.fixture(autouse=True)
def mock_settings_env_vars():
    with mock.patch.dict(os.environ, {"STATBANK_BASE_URL": "https://fake_url/",
                                     "STATBANK_ENCRYPT_URL": "https://fake_url2/",
                                     "JUPYTERHUB_USER": fake_mail() 
                                     }):
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
    return pd.DataFrame({"1":["999","01","02"],
                        "2":["2022", "2022", "2000"],
                        "3":["100", "2000", "30000"]})
#@pytest.fixture
#def fake_body():
#    return "--12345\r\nContent-Disposition:form-data; filename=delfil1.dat\r\nContent-type:text/plain\r\n\r\n01;2022;100\r\n02;2022;2000\r\n04;2022;30000\r\n--12345--\r\n"


# Fake responses from APIs

def fake_get_response_uttrekksbeskrivelse_successful():
    response = requests.Response()
    response.status_code = 200
    response._content = bytes('{"Uttaksbeskrivelse_lagd":"29.09.2022 klokka 18:51" , "base": "DB1T","TabellId":"10000","Huvudtabell":"HovedTabellNavn","DeltabellTitler":[{ "Filnavn": "delfil1.dat" , "Filtext": "10000: Fake table" }] ,"deltabller":[{"deltabell":"delfil1.dat","variabler":[{"kolonnenummer":"1","Klassifikasjonsvariabel":"Kodeliste1","Variabeltext":"kodeliste1","Kodeliste_id":"Kodeliste1","Kodeliste_text":"Kodeliste 1"},{"kolonnenummer":"2","Klassifikasjonsvariabel":"Tid","Variabeltext":"tid","Kodeliste_id":"-","Kodeliste_text":"Tidsperioden for tabelldataene, enhet = år, format = åååå"}],"statistikkvariabler":[{ "kolonnenummer":"3","Text":"Antall","Enhet":"personer","Antall_lagrede_desimaler":"0","Antall_viste_desimaler":"0"}],"eksempel_linje":"01;2022;100"}],"kodelister":[{"kodeliste":"Kodeliste1","SumIALtTotalKode":"999","koder":[{"kode":"999","text":"i alt"},{"kode":"01","text":"Kode1"},{"kode":"02","text":"Kode2"}]}]}', "utf8")
    response.request = requests.PreparedRequest()
    response.request.headers = {'Authorization': fake_auth(), 'Content-Type': 'multipart/form-data; boundary=12345'}
    return response

def fake_post_response_key_service():
    response = requests.Response()
    response.status_code = 200
    response._content = bytes('{"message":"' + fake_auth() + '"}', "utf8")
    return response

def fake_post_response_transfer_successful():
    response = requests.Response()
    response.status_code = 200
    response._content = bytes('{"TotalResult":{"GeneratedId":null,"Status":"Success","Message":"ExecutePublish with AutoGodkjennData \'2\', AutoOverskrivData \'1\', Fagansvarlig1 \'tbf\', Fagansvarlig2 \'tbf\', Hovedtabell \'HovedTabellNavn\', Publiseringsdato \'07.01.2023 00:00:00\', Publiseringstid \'08:00\':  Status 0, OK, lasting er registrert med lasteoppdragsnummer:197885 => INFORMASJON. Publiseringen er satt til kl 08:00:00","Exception":null,"ValidationInfoItems":null},"ItemResults":[{"GeneratedId":null,"Status":"Success","Message":"DataLoader with file name \'delfil1.dat\', intials \'tbf\' and time \'29.09.2022 19:01:14\': Loading completed into temp table","Exception":null,"ValidationInfoItems":null},{"GeneratedId":null,"Status":"Success","Message":"ExecutePublish with AutoGodkjennData \'2\', AutoOverskrivData \'1\', Fagansvarlig1 \'tbf\', Fagansvarlig2 \'tbf\', Hovedtabell \'HovedTabellNavn\', Publiseringsdato \'07.01.2023 00:00:00\', Publiseringstid \'08:00\':  Status 0, OK, lasting er registrert med lasteoppdragsnummer:197885 => INFORMASJON. Publiseringen er satt til kl 08:00:00","Exception":null,"ValidationInfoItems":null}]}', "utf8")
    response.request = requests.PreparedRequest()
    response.request.headers = {'Authorization': fake_auth(), 'Content-Type': 'multipart/form-data; boundary=12345'}
    return response


# Successful fixtures

# Our only get-request is for the "uttrekksbeskrivelse"
@pytest.fixture
@mock.patch.object(dapla.statbank.StatbankUttrekksBeskrivelse, "_make_request")
@mock.patch.object(dapla.statbank.StatbankUttrekksBeskrivelse, "_encrypt_request")            
def uttrekksbeskrivelse_success(test_encrypt, test_make_request):
    test_make_request.return_value = fake_get_response_uttrekksbeskrivelse_successful()
    test_encrypt.return_value = fake_post_response_key_service()
    return dapla.statbank.StatbankUttrekksBeskrivelse("10000", fake_user())

@pytest.fixture
@mock.patch.object(dapla.statbank.StatbankUttrekksBeskrivelse, "_make_request")
@mock.patch.object(dapla.statbank.StatbankUttrekksBeskrivelse, "_encrypt_request")  
@mock.patch.object(dapla.statbank.StatbankTransfer, "_make_transfer_request")
@mock.patch.object(dapla.statbank.StatbankTransfer, "_encrypt_request")  
def transfer_success(test_transfer_encrypt, test_transfer_make_request, test_besk_encrypt, test_besk_make_request):
    test_besk_make_request.return_value = fake_get_response_uttrekksbeskrivelse_successful()
    test_besk_encrypt.return_value = fake_post_response_key_service()
    test_transfer_make_request.return_value = fake_post_response_transfer_successful()
    test_transfer_encrypt.return_value = fake_post_response_key_service()
    return dapla.statbank.StatbankTransfer(fake_data(), "10000", fake_user())


def test_uttrekksbeskrivelse_has_kodelister(uttrekksbeskrivelse_success):
    # last thing to get filled during __init__ is .kodelister, check that dict has length
    assert len(uttrekksbeskrivelse_success.kodelister)

# def test_uttrekksbeskrivelse_validate_data_wrong_deltabell_count():
#    ...

# def test_uttrekksbeskrivelse_validate_data_wrong_col_count():
#    ...

# def test_uttrekksbeskrivelse_validate_data_codes_outside_beskrivelse():
#    ...

def test_transfer_correct_entry(transfer_success):
    # "Lastenummer" is one of the last things set by __init__ and signifies a correctly loaded data-transfer.
    # Is also used to build urls to webpages showing the ingestion status
    assert transfer_success.oppdragsnummer.isdigit()


def test_transfer_no_auth_residuals(transfer_success):
    # Do a search for the key, password, and ciphered auth in the returned object.
    # Important to remove any traces of these before object is handed to user
    
    # Username should be in object (checks integrity of object, and validity of search) 
    assert len(search__dict__(transfer_success, fake_user(), keep = {}))
    
    # Make sure none of these are in the object for security
    assert 0 == len(search__dict__(transfer_success, fake_pass(), keep = {}))
    assert 0 == len(search__dict__(transfer_success, fake_auth(), keep = {}))

def search__dict__(obj, searchterm: str, path = "root", keep = {}):
    """ Recursive search through all nested objects having a __dict__-attribute"""
    if hasattr(obj, "__dict__"):
        for key, elem in obj.__dict__.items():
            if hasattr(elem, "__dict__"):
                path = path + "/" + key
                keep = search__dict__(elem, searchterm, path=path, keep=keep)
            if searchterm.lower() in str(elem).lower() or searchterm.lower() in str(key).lower():
                keep[path + "/" + key] = elem
    return keep


#def test_transfer_validation_error():
    # Alter in-data to introduce error which causes the validation-process to error-out
#    ...

#def test_transfer_auth_error():
    # Sending in the wrong password, make sure its handled elegantly
#    with pytest.raises(Exception):
#        ...

