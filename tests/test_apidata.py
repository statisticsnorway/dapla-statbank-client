from collections.abc import Callable
from typing import TYPE_CHECKING
from typing import Any
from unittest import mock

import pandas as pd
import pytest
import requests
from dotenv import load_dotenv
from requests.exceptions import HTTPError

from statbank import StatbankClient
from statbank.api_exceptions import StatbankParameterError
from statbank.api_exceptions import StatbankVariableSelectionError
from statbank.api_exceptions import TooBigRequestError
from statbank.apidata import _check_selection
from statbank.apidata import apicodelist
from statbank.apidata import apidata
from statbank.apidata import apidata_all
from statbank.apidata import apidata_query_all
from statbank.apidata import apidata_rotate
from statbank.apidata import apimetadata

if TYPE_CHECKING:
    from statbank.api_types import QueryWholeType

load_dotenv()

DIGITS_IN_YEAR = 4


def fake_user() -> str:
    return "SSB-person-456"


def fake_auth() -> str:
    return "SoCipherVerySecure"


def fake_build_user_agent() -> str:
    return "TestEnvPytestDB" + requests.utils.default_headers()["User-agent"]


def fake_post_response_key_service() -> requests.Response:
    response = requests.Response()
    response.status_code = 200
    response._content = bytes(  # noqa: SLF001
        '{"message":"' + fake_auth() + '"}',
        "utf8",
    )
    return response


VAR_NUM = 4


def fake_get_table_meta() -> requests.Response:
    response = requests.Response()
    response.status_code = 200
    response._content = bytes(  # noqa: SLF001
        '{"title":"05300: Avstand til nærmeste lokale/sted (prosent), etter avstand, kulturtilbud, statistikkvariabel og år","variables": [{"code":"Avstand1","text":"avstand","values":["01","02","03","04","05","06"], "valueTexts":["Under 1 km","1-4,9 km","5-9,9 km","10-24 km","25-49 km","50 km eller over"]}, {"code":"Kulturtilbud","text":"kulturtilbud","values":["01","02","03","04","05","06","07","08"], "valueTexts":["Kino eller lokale med jevnlig spillefilmframvisning", "Teater eller lokale med jevnlige teater- eller operaforestillinger", "Konsertsal eller lokale med jevnlige musikkarrangement", "Galleri eller lokale med jevnlige kunstutstillinger","Museum", "Idrettsplass eller idrettshall","Folkebibliotek","Bokhandel"]}, {"code":"ContentsCode","text":"statistikkvariabel","values":["Avstand"], "valueTexts":["Avstand til nærmeste lokale/sted"]}, {"code":"Tid","text":"år","values": ["1991","1994","1997","2000","2004","2008","2012","2016","2021"],"valueTexts": ["1991","1994","1997","2000","2004","2008","2012","2016","2021"],"time":true}]}',
        "utf8",
    )
    response.request = requests.PreparedRequest()
    return response


def fake_post_apidata() -> requests.Response:
    response = requests.Response()
    response.status_code = 200
    response._content = bytes(  # noqa: SLF001
        '{"class":"dataset","label":"05300: Avstand til nærmeste lokale/sted (prosent), etter avstand, kulturtilbud, statistikkvariabel og år","source":"Statistisk sentralbyrå","updated":"2022-05-24T06:00:00Z","id":["Avstand1","Kulturtilbud","ContentsCode","Tid"],"size":[6,8,1,9],"dimension":{"Avstand1":{"label":"avstand","category":{"index":{"01":0,"02":1,"03":2,"04":3,"05":4,"06":5},"label":{"01":"Under 1 km","02":"1-4,9 km","03":"5-9,9 km","04":"10-24 km","05":"25-49 km","06":"50 km eller over"}}},"Kulturtilbud":{"label":"kulturtilbud","category":{"index":{"01":0,"02":1,"03":2,"04":3,"05":4,"06":5,"07":6,"08":7},"label":{"01":"Kino eller lokale med jevnlig spillefilmframvisning","02":"Teater eller lokale med jevnlige teater- eller operaforestillinger","03":"Konsertsal eller lokale med jevnlige musikkarrangement","04":"Galleri eller lokale med jevnlige kunstutstillinger","05":"Museum","06":"Idrettsplass eller idrettshall","07":"Folkebibliotek","08":"Bokhandel"}}},"ContentsCode":{"label":"statistikkvariabel","category":{"index":{"Avstand":0},"label":{"Avstand":"Avstand til nærmeste lokale/sted"},"unit":{"Avstand":{"base":"prosent","decimals":0}}}},"Tid":{"label":"år","category":{"index":{"1991":0,"1994":1,"1997":2,"2000":3,"2004":4,"2008":5,"2012":6,"2016":7,"2021":8},"label":{"1991":"1991","1994":"1994","1997":"1997","2000":"2000","2004":"2004","2008":"2008","2012":"2012","2016":"2016","2021":"2021"}}}},"value":[16,14,12,12,11,12,10,12,12,7,7,6,6,6,7,7,8,7,8,8,8,8,7,10,9,10,8,11,11,9,10,10,12,10,11,8,10,9,9,9,8,10,8,9,8,38,36,33,32,33,38,34,34,27,23,23,20,18,19,20,18,17,15,20,22,20,19,18,21,18,18,null,39,39,38,35,36,36,39,42,null,23,23,23,23,26,28,30,32,null,26,28,24,25,30,31,34,35,null,33,34,30,29,33,33,36,37,null,32,35,33,32,32,33,34,34,null,44,47,49,48,48,44,48,50,null,50,50,50,51,49,48,50,51,null,42,44,43,42,45,44,46,48,null,21,20,20,19,21,21,20,21,null,16,15,15,15,17,19,18,22,null,16,17,16,16,18,20,19,23,null,18,18,17,18,20,19,19,23,null,22,22,20,19,22,20,22,24,null,11,9,11,11,11,9,11,10,null,16,15,17,17,18,17,18,19,null,18,14,16,16,15,14,16,16,null,18,19,21,22,22,21,21,19,16,16,16,21,22,19,22,22,21,16,17,15,21,21,19,22,19,20,15,16,16,20,20,19,20,20,19,16,21,21,24,23,21,23,20,21,18,6,5,6,6,6,6,6,5,5,9,10,10,10,11,12,11,11,10,12,11,14,14,13,14,13,12,null,7,6,7,9,7,7,7,5,6,38,12,13,14,13,11,11,8,10,34,11,12,13,12,9,10,7,10,22,9,10,10,7,8,8,6,10,15,8,9,10,9,8,10,7,11,1,2,1,1,1,1,1,1,2,2,2,2,3,2,2,2,1,4,8,6,5,7,5,4,4,3,null,null,2,2,3,2,3,3,2,3,null,26,23,19,18,13,12,9,15,null,21,19,16,13,9,8,5,10,null,13,14,11,8,7,7,5,11,null,4,6,6,5,6,6,4,10,null,0,0,0,1,1,0,0,1,null,0,1,0,1,1,1,0,2,null,3,2,2,3,3,3,2,null],"status":{"71":".","80":".","89":".","98":".","107":".","116":".","125":".","134":".","143":".","152":".","161":".","170":".","179":".","188":".","197":".","206":".","215":".","287":".","359":".","360":"..","369":"..","378":"..","387":"..","396":"..","405":"..","414":"..","423":"..","431":"."},"role":{"time":["Tid"],"metric":["ContentsCode"]},"version":"2.0","extension":{"px":{"infofile":"None","tableid":"05300","decimals":0}}}',
        "utf8",
    )
    response.request = requests.PreparedRequest()
    return response


def fake_post_too_many_values_selected() -> requests.Response:
    response = requests.Response()
    response.status_code = 400
    response._content = bytes(  # noqa: SLF001
        '{"error": "Too many values selected"}',
        encoding="utf8",
    )
    response.request = requests.PreparedRequest()
    return response


def fake_post_parameter_error() -> requests.Response:
    response = requests.Response()
    response.status_code = 400
    response._content = bytes(  # noqa: SLF001
        '{"error": "Parameter error"}',
        encoding="utf8",
    )
    response.request = requests.PreparedRequest()
    return response


def fake_post_variable_error() -> requests.Response:
    response = requests.Response()
    response.status_code = 400
    response._content = bytes(  # noqa: SLF001
        '{"error": "The request for variable \'Avstand1\' has an error. Please check your query."}',
        encoding="utf8",
    )
    response.request = requests.PreparedRequest()
    return response


def fake_metadata() -> dict[str, Any]:
    return {
        "title": "05300: Avstand til nærmeste lokale/sted (prosent), etter avstand, kulturtilbud, statistikkvariabel og år",
        "variables": [
            {
                "code": "Avstand1",
                "text": "avstand",
                "values": ["01", "02", "03", "04", "05", "06"],
            },
        ],
    }


@pytest.fixture
@mock.patch.object(StatbankClient, "_encrypt_request")
@mock.patch.object(StatbankClient, "_get_user")
@mock.patch.object(StatbankClient, "_get_user_initials")
@mock.patch.object(StatbankClient, "_build_user_agent")
def client_fake(
    test_build_user_agent: Callable,
    test_get_user_initials: Callable,
    test_get_user: Callable,
    encrypt_fake: Callable,
) -> StatbankClient:
    encrypt_fake.return_value = fake_post_response_key_service()
    test_get_user.return_value = fake_user()
    test_get_user_initials.return_value = fake_user()
    test_build_user_agent.return_value = fake_build_user_agent()
    return StatbankClient(check_username_password=False)


@mock.patch.object(requests, "get")
def test_apimetadata(fake_get: Callable) -> None:
    fake_get.return_value = fake_get_table_meta()
    assert len(apimetadata("05300").get("title"))


@mock.patch.object(requests, "get")
def test_apicodelist_all(fake_get: Callable) -> None:
    fake_get.return_value = fake_get_table_meta()
    assert len(apicodelist("05300")) == VAR_NUM


@mock.patch.object(requests, "get")
def test_apicodelist_specific(fake_get: Callable) -> None:
    fake_get.return_value = fake_get_table_meta()
    result = apicodelist("05300", "Avstand1")
    assert len(result)
    assert isinstance(result, dict)
    assert all(isinstance(x, str) for x in result.values())


@mock.patch.object(requests, "get")
def test_apicodelist_specific_text(fake_get: Callable) -> None:
    fake_get.return_value = fake_get_table_meta()
    result = apicodelist("05300", "avstand")
    assert len(result)
    assert isinstance(result, dict)
    assert all(isinstance(x, str) for x in result.values())


@mock.patch.object(requests, "get")
def test_apicodelist_specific_missing_raises(fake_get: Callable) -> None:
    fake_get.return_value = fake_get_table_meta()
    with pytest.raises(ValueError, match="Cant find") as _:
        apicodelist("05300", "missing")


@pytest.fixture
@mock.patch.object(requests, "get")
def query_all_05300(fake_get: Callable) -> pd.DataFrame:
    fake_get.return_value = fake_get_table_meta()
    return apidata_query_all("05300")


@pytest.fixture
@mock.patch.object(requests, "post")
def apidata_05300(fake_post: Callable, query_all_05300: pd.DataFrame) -> pd.DataFrame:
    fake_post.return_value = fake_post_apidata()
    return apidata("05300", query_all_05300, include_id=True)


@mock.patch.object(requests, "get")
def test_query_all_raises_500(fake_get: Callable) -> None:
    fake_get.return_value = fake_get_table_meta()
    fake_get.return_value.status_code = 500
    with pytest.raises(expected_exception=HTTPError) as _:
        apidata_query_all("https://data.ssb.no/api/v0/no/table/05300")


@mock.patch("statbank.apidata")
def test_apidata_all_05300(fake_apidata: Callable, apidata_05300: pd.DataFrame) -> None:
    fake_apidata.return_value = apidata_05300
    df_all = apidata_all("05300", include_id=True)
    assert isinstance(df_all, pd.DataFrame)
    assert len(df_all)


@mock.patch("statbank.apidata")
def test_apidata_rotate_05300(
    fake_apidata: Callable,
    apidata_05300: pd.DataFrame,
) -> None:
    fake_apidata.return_value = apidata_05300
    df_all = apidata_all("05300", include_id=True)
    df_rotate = apidata_rotate(df_all, ind="år", val="value")
    # After rotating index should be 4-digit years
    for ind in df_rotate.index:
        assert len(ind) == DIGITS_IN_YEAR
        assert ind.isdigit()


def test_client_apimetadata(client_fake: Callable) -> None:
    metadata = client_fake.apimetadata("05300")
    assert len(metadata.get("title"))


def test_client_apicodelist(client_fake: Callable) -> None:
    metadata = client_fake.apicodelist("05300", "Avstand1")
    assert len(metadata)
    assert isinstance(metadata, dict)
    assert all(isinstance(x, str) for x in metadata.values())


def test_client_apidata(client_fake: Callable, query_all_05300: pd.DataFrame) -> None:
    tabledata = client_fake.apidata("05300", query_all_05300)
    assert isinstance(tabledata, pd.DataFrame)
    assert len(tabledata)


def test_client_apidata_no_query(client_fake: Callable) -> None:
    tabledata = client_fake.apidata("05300")
    assert isinstance(tabledata, pd.DataFrame)
    assert len(tabledata)


@mock.patch("statbank.apidata")
def test_client_apidata_all(
    fake_apidata: Callable,
    client_fake: Callable,
    apidata_05300: pd.DataFrame,
) -> None:
    fake_apidata.return_value = apidata_05300
    tabledata = client_fake.apidata_all("https://data.ssb.no/api/v0/no/table/05300")
    assert isinstance(tabledata, pd.DataFrame)
    assert len(tabledata)


@mock.patch("statbank.apidata")
def test_client_apidata_rotate_05300(
    fake_apidata: Callable,
    client_fake: Callable,
    apidata_05300: pd.DataFrame,
) -> None:
    fake_apidata.return_value = apidata_05300
    df_all = client_fake.apidata_all(
        "https://data.ssb.no/api/v0/no/table/05300/",
        include_id=True,
    )
    df_rotate = client_fake.apidata_rotate(df_all, ind="år", val="value")
    # After rotating index should be 4-digit years
    for ind in df_rotate.index:
        assert len(ind) == DIGITS_IN_YEAR
        assert ind.isdigit()


@mock.patch.object(requests, "post")
def test_apidata_raises_parameter_error(
    fake_post: Callable,
    query_all_05300: pd.DataFrame,
) -> None:
    fake_post.return_value = fake_post_parameter_error()
    fake_post.return_value.status_code = 400
    with pytest.raises(expected_exception=StatbankParameterError) as _:
        apidata("05300", query_all_05300, include_id=True)


@mock.patch.object(requests, "post")
def test_apidata_raises_variable_error(
    fake_post: Callable,
    query_all_05300: pd.DataFrame,
) -> None:
    fake_post.return_value = fake_post_variable_error()
    fake_post.return_value.status_code = 400
    with pytest.raises(expected_exception=StatbankVariableSelectionError) as _:
        apidata("05300", query_all_05300, include_id=True)


@mock.patch.object(requests, "post")
def test_apidata_raises_too_big_error(
    fake_post: Callable,
    query_all_05300: pd.DataFrame,
) -> None:
    fake_post.return_value = fake_post_too_many_values_selected()
    fake_post.return_value.status_code = 403
    with pytest.raises(expected_exception=TooBigRequestError) as _:
        apidata("05300", query_all_05300, include_id=True)


@mock.patch.object(requests, "post")
def test_apidata_raises_500(
    fake_post: Callable,
    query_all_05300: pd.DataFrame,
) -> None:
    fake_post.return_value = fake_post_apidata()
    fake_post.return_value.status_code = 500
    with pytest.raises(HTTPError) as _:
        apidata("05300", query_all_05300, include_id=True)


def test_apidata_raises_wrong_id(
    query_all_05300: pd.DataFrame,
) -> None:
    with pytest.raises(ValueError, match="statbank ID") as _:
        apidata("0", query_all_05300, include_id=True)


@mock.patch("statbank.apidata")
def test_apidata_all_raises_wrong_id(
    fake_apidata: Callable,
    apidata_05300: pd.DataFrame,
) -> None:
    fake_apidata.return_value = apidata_05300
    with pytest.raises(ValueError, match="statbank ID") as _:
        apidata_all("0", include_id=True)


def test_check_duplicates_in_selection():
    variable = "Avstand1"
    request: QueryWholeType = {
        "query": [
            {
                "code": "Avstand1",
                "selection": {
                    "filter": "item",
                    "values": ["01", "01"],
                },
            },
        ],
        "response": {"format": "json-stat2"},
    }

    message = _check_selection(variable, "05300", request)
    expected = "The value(s) 01 is duplicated for variable Avstand1"
    assert message == expected


@mock.patch("statbank.apimetadata")
def test_check_invalid_in_selection(fake_metadata: Callable):
    fake_metadata.return_value = fake_metadata()
    variable = "Avstand1"
    request: QueryWholeType = {
        "query": [
            {
                "code": "Avstand1",
                "selection": {
                    "filter": "item",
                    "values": ["01", "07", "08"],
                },
            },
        ],
        "response": {"format": "json-stat2"},
    }

    message = _check_selection(variable, "05300", request)
    expected = (
        "Invalid value(s) 07 and 08 have been specified for the variable Avstand1"
    )
    assert message == expected


@mock.patch("statbank.apimetadata")
def test_check_with_wildcard(fake_metadata: Callable):
    fake_metadata.return_value = fake_metadata()
    variable = "Avstand1"
    request: QueryWholeType = {
        "query": [
            {
                "code": "Avstand1",
                "selection": {
                    "filter": "item",
                    "values": ["*"],
                },
            },
        ],
        "response": {"format": "json-stat2"},
    }

    message = _check_selection(variable, "05300", request)
    expected = (
        "One of the values for the variable Avstand1 contains a wildcard character (*)."
    )
    assert message is not None
    assert message.startswith(expected)
