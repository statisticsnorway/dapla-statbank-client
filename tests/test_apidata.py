from __future__ import annotations

import json
from http import HTTPMethod
from http import HTTPStatus
from importlib.resources import files
from typing import TYPE_CHECKING
from typing import Any
from typing import Self
from typing import cast

import httpx
import pandas as pd
import pxwebapi.expression
import pxwebapi.query_types
import pytest
from furl import furl

from statbank.api_exceptions import StatbankParameterError
from statbank.api_exceptions import StatbankVariableSelectionError
from statbank.api_exceptions import TooBigRequestError
from statbank.get_apidata import apidata
from statbank.get_apidata import apidata_rotate
from statbank.get_apidata import convert_to_api2_selection
from statbank.get_apidata_internal import apicodelist_internal
from statbank.get_apidata_internal import apidata_all_internal
from statbank.get_apidata_internal import apidata_internal
from statbank.get_apidata_internal import apidata_query_all
from statbank.get_apidata_internal import apimetadata_internal

from . import resources

if TYPE_CHECKING:
    from collections.abc import Callable

    from statbank.api_types import QueryWholeType


DIGITS_IN_YEAR = 4
VAR_NUM = 4
INTERNAL_05300_URL = "https://i.ssb.no/pxwebi/api/v0/no/prod_24v_intern/START/kf/kf01/kulturbar/div_kulturbar_mappe/Kulturbaromet58"
EXTERNAL_V2_05300_URL = "https://data.ssb.no/api/pxwebapi/v2/tables/05300"

type RequestHandler = Callable[[httpx.Request], httpx.Response]


class _FakeHandler:
    def __init__(self: Self) -> None:
        self.history: list[httpx.Request] = []

    def assert_url_was_called(self: Self, url: furl, n: int | None = None) -> None:
        """Asserts that a URL was called at least once, or n number of times."""
        n_called = sum(
            (request.url.netloc.decode() == url.netloc and request.url.path == url.path)
            for request in self.history
        )
        if n and n != n_called:
            msg = f"The url {url} was called {n_called} times, and not {n} times"
            raise AssertionError(msg)
        if n == 0:
            msg = f"The url {url} was never called"
            raise AssertionError(msg)

    def __call__(self: Self, request: httpx.Request) -> httpx.Response:  # noqa: PLR0911
        self.history.append(request)
        if request.url == INTERNAL_05300_URL:
            if request.method == HTTPMethod.POST:
                content = (files(resources) / "dataset_05300.json").read_bytes()

                return httpx.Response(
                    HTTPStatus.OK,
                    content=content,
                    headers={"Content-Type": "application/json"},
                )

            content = (files(resources) / "table_v0_05300.json").read_bytes()

            return httpx.Response(
                HTTPStatus.OK,
                content=content,
                headers={"Content-Type": "application/json"},
            )

        if request.url.path == "/api/pxwebapi/v2/tables/05300":
            if request.method != HTTPMethod.GET:
                return httpx.Response(
                    HTTPStatus.METHOD_NOT_ALLOWED,
                    headers={"Content-Type": "application/json"},
                )

            content = (files(resources) / "table_v2_05300.json").read_bytes()

            return httpx.Response(
                HTTPStatus.OK,
                content=content,
                headers={"Content-Type": "application/json"},
            )

        if request.url.path == "/api/pxwebapi/v2/tables/05300/metadata":
            if request.method != HTTPMethod.GET:
                return httpx.Response(
                    HTTPStatus.METHOD_NOT_ALLOWED,
                    headers={"Content-Type": "application/json"},
                )

            content = (files(resources) / "metadata_05300.json").read_bytes()

            return httpx.Response(
                HTTPStatus.OK,
                content=content,
                headers={"Content-Type": "application/json"},
            )

        if request.url.path == "/api/pxwebapi/v2/tables/05300/data":
            content = (files(resources) / "05300.parquet").read_bytes()

            return httpx.Response(
                HTTPStatus.OK,
                content=content,
                headers={"Content-Type": "application/json"},
            )

        if request.url.path == "/api/pxwebapi/v2/config":
            if request.method != HTTPMethod.GET:
                return httpx.Response(
                    HTTPStatus.METHOD_NOT_ALLOWED,
                    headers={"Content-Type": "application/json"},
                )

            content = (files(resources) / "config.json").read_bytes()

            return httpx.Response(
                HTTPStatus.OK,
                content=content,
                headers={"Content-Type": "application/json"},
            )

        return httpx.Response(
            HTTPStatus.NOT_FOUND,
        )


def fake_error(request: httpx.Request) -> httpx.Response:  # noqa: ARG001
    return httpx.Response(
        HTTPStatus.INTERNAL_SERVER_ERROR,
        headers={"Content-Type": "application/json"},
    )


def fake_post_error(request: httpx.Request) -> httpx.Response:
    if request.method == HTTPMethod.POST:
        return fake_error(request)
    return _FakeHandler()(request)


def fake_post_apidata(request: httpx.Request) -> httpx.Response:
    if request.method == HTTPMethod.POST:
        content = (files(resources) / "dataset_05300.json").read_bytes()

        return httpx.Response(
            HTTPStatus.OK,
            content=content,
            headers={"Content-Type": "application/json"},
        )

    return _FakeHandler()(request)


def fake_post_too_many_values_selected(request: httpx.Request) -> httpx.Response:
    if request.method == HTTPMethod.POST:
        return httpx.Response(
            HTTPStatus.FORBIDDEN,
            content=b"""{"error": "Too many values selected"}""",
            headers={"Content-Type": "application/json"},
        )

    return _FakeHandler()(request)


def fake_post_parameter_error(request: httpx.Request) -> httpx.Response:
    if request.method == HTTPMethod.POST:
        return httpx.Response(
            HTTPStatus.BAD_REQUEST,
            content=b"""{"error": "Parameter error"}""",
            headers={"Content-Type": "application/json"},
        )

    return _FakeHandler()(request)


def fake_post_variable_error(request: httpx.Request) -> httpx.Response:
    if request.method == HTTPMethod.POST:
        return httpx.Response(
            HTTPStatus.BAD_REQUEST,
            content=b"""{"error": "The request for variable \'Avstand1\' has an error. Please check your query."}""",
            headers={"Content-Type": "application/json"},
        )

    return _FakeHandler()(request)


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
def query_all_05300() -> QueryWholeType:
    return {
        "query": [
            {
                "code": "Avstand1",
                "selection": {
                    "filter": "all",
                    "values": ["*"],
                },
            },
            {
                "code": "Kulturtilbud",
                "selection": {
                    "filter": "all",
                    "values": ["*"],
                },
            },
            {
                "code": "ContentsCode",
                "selection": {
                    "filter": "all",
                    "values": ["*"],
                },
            },
            {
                "code": "Tid",
                "selection": {
                    "filter": "all",
                    "values": ["*"],
                },
            },
        ],
        "response": {},
    }


@pytest.fixture
def query_some_05300() -> QueryWholeType:
    return {
        "query": [
            {
                "code": "Avstand1",
                "selection": {
                    "filter": "range",
                    "values": ["02", "03"],
                },
            },
            {
                "code": "Kulturtilbud",
                "selection": {
                    "filter": "all",
                    "values": ["*"],
                },
            },
            {
                "code": "ContentsCode",
                "selection": {
                    "filter": "item",
                    "values": ["Avstand"],
                },
            },
            {
                "code": "Tid",
                "selection": {
                    "filter": "top",
                    "values": ["3"],
                },
            },
        ],
        "response": {},
    }


@pytest.fixture
def df_53000():
    with (files(resources) / "dataframe_05300.json").open(encoding="utf-8") as buffer:
        data = cast("dict[str, Any]", json.load(buffer))
    return pd.DataFrame(data)


def mock_httpx_client(respons_func: RequestHandler) -> httpx.Client:

    transport = httpx.MockTransport(respons_func)
    return httpx.Client(transport=transport)


def test_apimetadata_internal() -> None:
    handler = _FakeHandler()
    client = mock_httpx_client(handler)
    result = apimetadata_internal(
        INTERNAL_05300_URL,
        client=client,
    )
    assert len(result.get("title"))


def test_apicodelist_all_internal() -> None:
    handler = _FakeHandler()
    client = mock_httpx_client(handler)
    result = apicodelist_internal(
        INTERNAL_05300_URL,
        client=client,
    )
    assert len(result) == VAR_NUM


def test_apicodelist_specific_internal() -> None:
    handler = _FakeHandler()
    client = mock_httpx_client(handler)
    result = apicodelist_internal(
        INTERNAL_05300_URL,
        "Avstand1",
        client=client,
    )
    assert len(result)
    assert isinstance(result, dict)
    assert all(isinstance(x, str) for x in result.values())


def test_apicodelist_specific_text_internal() -> None:
    handler = _FakeHandler()
    client = mock_httpx_client(handler)
    result = apicodelist_internal(
        INTERNAL_05300_URL,
        "avstand",
        client=client,
    )
    assert len(result)
    assert isinstance(result, dict)
    assert all(isinstance(x, str) for x in result.values())


def test_apicodelist_specific_missing_raises_internal() -> None:
    handler = _FakeHandler()
    client = mock_httpx_client(handler)
    with pytest.raises(ValueError, match="Cant find") as _:
        apicodelist_internal(
            INTERNAL_05300_URL,
            "missing",
            client=client,
        )


def test_query_all_raises_500_internal() -> None:
    client = mock_httpx_client(fake_error)
    with pytest.raises(expected_exception=httpx.HTTPStatusError) as _:
        apidata_query_all(
            INTERNAL_05300_URL,
            client=client,
        )


def test_apidata_all_05300_internal() -> None:
    client = mock_httpx_client(fake_post_apidata)
    df_all = apidata_all_internal(
        INTERNAL_05300_URL,
        include_id=True,
        client=client,
    )
    assert isinstance(df_all, pd.DataFrame)
    assert len(df_all)


def test_apidata_rotate_05300_internal(
    df_53000: pd.DataFrame,
) -> None:

    df_rotate = apidata_rotate(df_53000, ind="år", val="value")
    # After rotating index should be 4-digit years
    for ind in df_rotate.index:
        assert len(ind) == DIGITS_IN_YEAR
        assert ind.isdigit()


def test_apidata_raises_parameter_error_internal(
    query_all_05300: QueryWholeType,
) -> None:
    client = mock_httpx_client(fake_post_parameter_error)
    with pytest.raises(expected_exception=StatbankParameterError) as _:
        apidata_internal(
            INTERNAL_05300_URL,
            query_all_05300,
            include_id=True,
            client=client,
        )


def test_apidata_raises_variable_error_internal(
    query_all_05300: QueryWholeType,
) -> None:
    client = mock_httpx_client(fake_post_variable_error)
    with pytest.raises(expected_exception=StatbankVariableSelectionError) as _:
        apidata_internal(
            INTERNAL_05300_URL,
            query_all_05300,
            include_id=True,
            client=client,
        )


def test_apidata_raises_too_big_error_internal(
    query_all_05300: QueryWholeType,
) -> None:
    client = mock_httpx_client(fake_post_too_many_values_selected)
    with pytest.raises(expected_exception=TooBigRequestError) as _:
        apidata_internal(
            INTERNAL_05300_URL,
            query_all_05300,
            include_id=True,
            client=client,
        )


def test_apidata_raises_500_internal(
    query_all_05300: QueryWholeType,
) -> None:
    client = mock_httpx_client(fake_post_error)
    with pytest.raises(httpx.HTTPStatusError) as _:
        apidata_internal(
            INTERNAL_05300_URL,
            query_all_05300,
            include_id=True,
            client=client,
        )


def test_check_duplicates_in_selection_internal():
    client = mock_httpx_client(fake_post_variable_error)
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

    expected_message = r"The value\(s\) 01 is duplicated for variable Avstand1"
    with pytest.raises(
        StatbankVariableSelectionError,
        match=expected_message,
    ):
        apidata_internal(
            INTERNAL_05300_URL,
            request,
            client=client,
        )


def test_check_invalid_in_selection_internal():
    client = mock_httpx_client(fake_post_variable_error)
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

    expected_message = (
        r"Invalid value\(s\) 07 and 08 have been specified for the variable Avstand1"
    )
    with pytest.raises(
        StatbankVariableSelectionError,
        match=expected_message,
    ):
        apidata_internal(
            INTERNAL_05300_URL,
            request,
            client=client,
        )


def test_check_with_wildcard_internal():
    client = mock_httpx_client(fake_post_variable_error)
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

    expected_message = r"One of the values for the variable Avstand1 contains a wildcard character \(\*\)\."
    with pytest.raises(
        StatbankVariableSelectionError,
        match=expected_message,
    ):
        apidata_internal(
            INTERNAL_05300_URL,
            request,
            client=client,
        )


def test_convert_all_query(query_all_05300: QueryWholeType):
    expected = [
        pxwebapi.query_types.Selection(
            "Avstand1",
            value_codes=[pxwebapi.expression.CodeExpression("*")],
        ),
        pxwebapi.query_types.Selection(
            "Kulturtilbud",
            value_codes=[pxwebapi.expression.CodeExpression("*")],
        ),
        pxwebapi.query_types.Selection(
            "ContentsCode",
            value_codes=[pxwebapi.expression.CodeExpression("*")],
        ),
        pxwebapi.query_types.Selection(
            "Tid",
            value_codes=[pxwebapi.expression.CodeExpression("*")],
        ),
    ]
    result = convert_to_api2_selection(query_all_05300["query"])
    assert result == expected


def test_convert_some_query(query_some_05300: QueryWholeType):
    expected = [
        pxwebapi.query_types.Selection(
            "Avstand1",
            value_codes=[pxwebapi.expression.RangeExpression("02", "03")],
        ),
        pxwebapi.query_types.Selection(
            "Kulturtilbud",
            value_codes=[pxwebapi.expression.CodeExpression("*")],
        ),
        pxwebapi.query_types.Selection(
            "ContentsCode",
            value_codes=[pxwebapi.expression.CodeExpression("Avstand")],
        ),
        pxwebapi.query_types.Selection(
            "Tid",
            value_codes=[pxwebapi.expression.TopExpression(3)],
        ),
    ]
    result = convert_to_api2_selection(query_some_05300["query"])
    assert result == expected


def test_apidata_new_with_number(
    query_all_05300: QueryWholeType,
    df_53000: pd.DataFrame,
):
    handler = _FakeHandler()
    client = mock_httpx_client(handler)
    result = apidata("05300", query_all_05300, include_id=False, client=client)

    handler.assert_url_was_called(
        furl("https://data.ssb.no/api/pxwebapi/v2/tables/05300/data"),
        1,
    )

    assert result.shape == df_53000.shape
    pd.testing.assert_index_equal(result.columns, df_53000.columns)


def test_apidata_new_with_oldurl(
    query_all_05300: QueryWholeType,
    df_53000: pd.DataFrame,
):
    handler = _FakeHandler()
    client = mock_httpx_client(handler)
    result = apidata(
        "https://data.ssb.no/api/v0/no/table/05300",
        query_all_05300,
        include_id=False,
        client=client,
    )

    handler.assert_url_was_called(
        furl("https://data.ssb.no/api/pxwebapi/v2/tables/05300/data"),
        1,
    )

    assert result.shape == df_53000.shape
    pd.testing.assert_index_equal(result.columns, df_53000.columns)
