from __future__ import annotations

import re
from collections import Counter
from http import HTTPStatus
from typing import TYPE_CHECKING
from typing import Any

import httpx
import pxwebapi.rate_limiter
from furl import furl

from .api_exceptions import StatbankParameterError
from .api_exceptions import StatbankVariableSelectionError
from .api_exceptions import TooBigRequestError
from .response_to_pandas import response_to_pandas
from .statbank_logger import logger

if TYPE_CHECKING:
    from collections.abc import Iterable
    from collections.abc import Sequence

    import pandas as pd

    from .api_types import QueryPartType
    from .api_types import QueryWholeType


def apidata_internal(
    url: str | furl,
    payload: QueryWholeType | None = None,
    include_id: bool = False,
    client: httpx.Client | None = None,
) -> pd.DataFrame:
    """Get the contents of a published statbank-table as a pandas Dataframe, specifying a query to limit the return.

    Args:
        url: A url to a internal Statbank table, or other Pxweb v0 compatible API.
        payload: A dict in the shape of a QueryWhole, to include with the request, can be copied from the statbank-webpage.
        include_id: If you want to include "codes" in the dataframe, set this to True.
        client: A optional `httpx.Client` to use, to fetch the data.

    Returns:
        pd.DataFrame: The table-content

    Raises:
        ValueError: If the first parameter is not recognized as a statbank ID or a direct url.
    """
    if not isinstance(url, furl):
        url = furl(url)

    if payload is None:
        payload = {
            "query": [],
            "response": {"format": "json-stat2"},
        }

    if client is None:
        client = httpx.Client(
            transport=pxwebapi.rate_limiter.PxUnlimtedTransport(),
            timeout=20,
        )

    logger.info(url)
    # Spør APIet om å få resultatet med httpx
    resultat = client.post(url.url, json=payload)

    if not resultat.is_success:
        _read_error(url, payload, resultat)

    # Få pd.DataFrame fra resultatet
    table_data = response_to_pandas(resultat, include_id=include_id)

    return table_data.convert_dtypes()


def apidata_query_all(
    url: str | furl,
    client: httpx.Client | None = None,
) -> QueryWholeType:
    """Builds a query for ALL THE DATA in a table based on a request for metadata on the table.

    Args:
        url: A url to a internal Statbank table, or other Pxweb v0 compatible API.
        client: A optional `httpx.Client` to use, to fetch the data.

    Returns:
        QueryWholeType: The prepared query based on all the codes in the table.
    """
    meta = apimetadata_internal(url, client)["variables"]
    code_list: list[QueryPartType] = []
    for code in meta:
        tmp: QueryPartType = {"code": "", "selection": {"filter": "", "values": []}}
        for k, v in code.items():
            if k == "code":
                tmp["code"] = v
            if k == "values":
                tmp["selection"] = {"filter": "item", "values": v}
        code_list += [tmp]
    return {"query": code_list, "response": {"format": "json-stat2"}}


def apidata_all_internal(
    url: str | furl,
    include_id: bool = False,
    client: httpx.Client | None = None,
) -> pd.DataFrame:
    """Get ALL the contents of a published statbank-table as a pandas Dataframe.

    Args:
        url: A url to a internal Statbank table, or other Pxweb v0 compatible API.
        include_id (bool): If you want to include "codes" in the dataframe, set this to True
        client: A optional `httpx.Client` to use, to fetch the data.

    Returns:
        pd.DataFrame: Table-content
    """
    return apidata_internal(
        url,
        apidata_query_all(url),
        include_id=include_id,
        client=client,
    )


def apimetadata_internal(
    url: str | furl,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Get the metadata of a published statbank-table as a dict.

    Args:
        url: A url to a internal Statbank table, or other Pxweb v0 compatible API.
        client: A optional `httpx.Client` to use, to fetch the data.

    Returns:
        dict[str, Any]: The metadata of the table as the json returned from the API-get-request.

    Raises:
        ValueError: If the first parameter is not recognized as a statbank ID or a direct url.
    """
    if not isinstance(url, furl):
        url = furl(url)

    if client is None:
        client = httpx.Client(transport=pxwebapi.rate_limiter.PxUnlimtedTransport())

    res = client.get(url.url)
    res.raise_for_status()
    meta: dict[str, Any] = res.json()
    return meta


def apicodelist_internal(
    url: furl | str,
    codelist_name: str | None = None,
    client: httpx.Client | None = None,
) -> dict[str, str] | dict[str, dict[str, str]]:
    """Get one specific or all the codelists of a published statbank-table as a dict or nested dicts.

    Args:
        url: A url to a internal Statbank table, or other Pxweb v0 compatible API.
        codelist_name (str): The name of the specific codelist to get.
        client: A optional `httpx.Client` to use, to fetch the data.

    Returns:
        dict[str, str] | dict[str, dict[str, str]]: The codelist of the table as a dict or a nested dict.

    Raises:
        ValueError: If the specified codelist_name is not in the returned metadata.
    """
    if not isinstance(url, furl):
        url = furl(url)

    metadata = apimetadata_internal(url, client=client)

    if not codelist_name:
        return {
            variable["code"]: dict(
                zip(variable["values"], variable["valueTexts"], strict=False),
            )
            for variable in metadata["variables"]
        }

    for variable in metadata["variables"]:
        if codelist_name == variable["code"] or codelist_name == variable["text"]:
            return dict(zip(variable["values"], variable["valueTexts"], strict=False))

    col_names = ", ".join([col["code"] for col in metadata["variables"]])
    error_msg = f"Cant find {codelist_name} among the available names: {col_names}"
    raise ValueError(error_msg)


# Error handeling and HTTP-return code interpretations follow
def _list_up(sequence: Sequence[str], conjunction: str = "and") -> str:
    if len(sequence) == 1:
        return sequence[0]

    return f"{', '.join(sequence[:-1])} {conjunction} {sequence[-1]}"


def _find_duplicates[T](items: Iterable[T]) -> list[T]:
    return [item for item, n in Counter(items).items() if n > 1]


def _read_error(url: furl, query: QueryWholeType, response: httpx.Response) -> None:
    """Raises an appropriate error."""
    error_message: str | None

    if response.status_code == HTTPStatus.FORBIDDEN:
        error_message = "Your query is too big. The API is limited to 800,000 cells (incl. empty cells)"
        raise TooBigRequestError(error_message)

    if response.status_code == HTTPStatus.BAD_REQUEST:
        api_error_message = response.json().get("error", "")

        match = re.match(
            r"The request for variable '(?P<variable>.+)' has an error\. Please check your query\.",
            api_error_message,
        )

        if match:
            variable = match["variable"]
            error_message = _check_selection(variable, url, query)
            if not error_message:
                error_message = (
                    f'Your query failed with the error message: "{api_error_message}"'
                )
            raise StatbankVariableSelectionError(error_message)

        error_message = (
            f'Your query failed with the error message: "{api_error_message}"'
        )
        raise StatbankParameterError(error_message)

    response.raise_for_status()


def _check_selection(
    variable: str,
    url: furl,
    query: QueryWholeType,
) -> str | None:
    """Checks for common errors in your query selection, and returns a error message.

    When the query fails with the message "The request for variable ... has an error,"
    check that the selection don't contains duplicate and invalid values against the metadata and
    check that the filter is set to "all" when selecting with a wildcard (*).
    Metadata for aggregations are not available, so we can't inspect selection with agg and agg_single filters.
    Errors with "top" and "all" filter always fail with "parameter error", so we don't validate them here.
    """
    query_part = next(filter(lambda part: part["code"] == variable, query["query"]))
    query_selection = query_part["selection"]

    match = re.fullmatch(
        r"(?P<filtertype>(item)|(vs|agg|agg_single))(?(3):(?P<aggregering>.+))",
        query_selection["filter"],
    )

    if match is None:
        return f'The filter don\'t match one of the types "item", "all", "top", "vs:", "agg:", "agg_single:", for variable {variable}.'

    filter_type = match["filtertype"]

    duplicates = _find_duplicates(query_selection["values"])
    if len(duplicates) > 0:
        return (
            f"The value(s) {_list_up(duplicates)} is duplicated for variable {variable}"
        )

    if any("*" in values for values in query_selection["values"]):
        return (
            f"One of the values for the variable {variable} contains a wildcard character (*)."
            'If you wish to select several or all values with a wildcard, "filter" must be set to "all"'
        )

    if filter_type in ("agg", "agg_single"):
        return (
            "A value is probably invalid for variable {variable}, but an aggregation is used,"
            "and metadata for aggregations is not available, so it is not possible to determine witch."
        )

    meta = apimetadata_internal(url)

    variable_meta = next(
        filter(lambda part: part["code"] == variable, meta["variables"]),
    )

    invalid_values = [
        value
        for value in query_selection["values"]
        if value not in variable_meta["values"]
    ]

    if len(invalid_values) > 0:
        return f"Invalid value(s) {_list_up(invalid_values)} have been specified for the variable {variable}"

    return None
