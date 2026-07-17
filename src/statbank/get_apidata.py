from __future__ import annotations

import re
import urllib
from collections import Counter
from http import HTTPStatus
from itertools import chain
from typing import TYPE_CHECKING
from typing import Any
from typing import TypeVar

import pandas as pd
import pxwebapi
import pxwebapi.query_types
import pxwebapi.selection_parser
import requests as r
from furl import furl
from requests.adapters import HTTPAdapter
from requests.adapters import Retry

if TYPE_CHECKING:
    from collections.abc import Iterable
    from collections.abc import Sequence

    import httpx

    from .api_types import QueryPartType
    from .api_types import QueryWholeType

from .api_exceptions import StatbankParameterError
from .api_exceptions import StatbankVariableSelectionError
from .api_exceptions import TooBigRequestError
from .response_to_pandas import response_to_pandas
from .statbank_logger import logger

# Getting data from Statbank

STATBANK_TABLE_ID_LENGTH = 5
STATBANK_API_V1_ENDPOINT = furl("https://data.ssb.no/api/v0/no/table")
T = TypeVar("T")


def _convert_to_api2_query(
    old_query: QueryWholeType | None,
) -> list[pxwebapi.query_types.Selection]:
    selections: list[pxwebapi.query_types.Selection] = []

    if old_query is None:
        return selections

    for old_select in old_query["query"]:
        variable_code = old_select["code"]
        old_filter = old_select["selection"]["filter"]
        old_values = old_select["selection"]["values"]
        new_values: list[pxwebapi.selection_parser.Expression]
        code_list = None
        if old_filter.lower() == "top":
            if len(old_values) == 1:
                expression = pxwebapi.selection_parser.TopExpression(
                    int(old_values[0]),
                )
            elif len(old_values) == 2:
                expression = pxwebapi.selection_parser.TopExpression(
                    int(old_values[0]),
                    int(old_values[1]),
                )
            else:
                msg = "Invalid TOP select expression"
                raise ValueError(msg)
            new_values = [expression]

        elif old_filter.lower() == "bottom":
            if len(old_values) == 1:
                expression = pxwebapi.selection_parser.BottomExpression(
                    int(old_values[0]),
                )
            elif len(old_values) == 2:
                expression = pxwebapi.selection_parser.BottomExpression(
                    int(old_values[0]),
                    int(old_values[1]),
                )
            else:
                msg = "Invalid BOTTOM select expression"
                raise ValueError(msg)
            new_values = [expression]

        elif old_filter.lower() == "range":
            if len(old_values) != 2:
                msg = "Invalid RANGE select expression"
                raise ValueError(msg)
            expression = pxwebapi.selection_parser.RangeExpression(
                old_values[0],
                old_values[1],
            )
            new_values = [expression]
        elif old_filter.lower() == "to":
            if len(old_values) != 1:
                msg = "Invalid TO select expression"
                raise ValueError(msg)
            expression = pxwebapi.selection_parser.ToExpression(old_values[0])
            new_values = [expression]

        elif old_filter.lower() == "from":
            if len(old_values) != 1:
                msg = "Invalid FROM select expression"
                raise ValueError(msg)
            expression = pxwebapi.selection_parser.FromExpression(old_values[0])
            new_values = [expression]
        else:
            if old_filter not in ("item", "all"):
                code_list = old_filter.replace(":", "_", count=1)
            new_values = [
                pxwebapi.selection_parser.CodeExpression(c) for c in old_values
            ]

        new_select = pxwebapi.query_types.Selection(
            variable_code,
            code_list,
            new_values,
        )
        selections.append(new_select)

    return selections


def apidata_legacy(
    id_or_url: str = "",
    payload: QueryWholeType | None = None,
    include_id: bool = False,
) -> pd.DataFrame:
    """Get the contents of a published statbank-table as a pandas Dataframe, specifying a query to limit the return.

    Args:
        id_or_url (str): The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
        payload (QueryWholeType | None): a dict in the shape of a QueryWhole, to include with the request, can be copied from the statbank-webpage.
        include_id (bool): If you want to include "codes" in the dataframe, set this to True

    Returns:
        pd.DataFrame: The table-content

    Raises:
        ValueError: If the first parameter is not recognized as a statbank ID or a direct url.
    """
    if payload is None:
        payload_now: QueryWholeType = {
            "query": [],
            "response": {"format": "json-stat2"},
        }
    else:
        payload_now = payload
    if len(id_or_url) == STATBANK_TABLE_ID_LENGTH and id_or_url.isdigit():
        url = f"https://data.ssb.no/api/v0/no/table/{id_or_url}/"
    else:
        test_url = urllib.parse.urlparse(id_or_url)
        if not all([test_url.scheme, test_url.netloc]):
            error_msg = (
                "First parameter not recognized as a statbank ID or a direct url"
            )
            raise ValueError(error_msg)
        url = id_or_url

    logger.info(url)
    # Spør APIet om å få resultatet med requests-biblioteket
    with r.Session() as s:
        retries = Retry(total=5, backoff_factor=0.1)
        s.mount("https://", HTTPAdapter(max_retries=retries))
        resultat = s.post(url, json=payload_now, timeout=20)

    if not resultat.ok:
        _read_error(id_or_url, payload_now, resultat)

    # Få pd.DataFrame fra resultatet
    table_data = response_to_pandas(resultat, include_id=include_id)

    return table_data.convert_dtypes()


def apidata(
    id_or_url: str = "",
    payload: QueryWholeType | None = None,
    include_id: bool = False,
    client: httpx.Client | None = None,
) -> pd.DataFrame:
    """Get the contents of a published statbank-table as a pandas Dataframe, specifying a query to limit the return.

    Args:
        id_or_url (str): The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
        payload (QueryWholeType | None): a dict in the shape of a QueryWhole, to include with the request, can be copied from the statbank-webpage.
        include_id (bool): If you want to include "codes" in the dataframe, set this to True

    Returns:
        pd.DataFrame: The table-content

    Raises:
        ValueError: If the first parameter is not recognized as a statbank ID or a direct url.
    """
    table_id = None

    if len(id_or_url) == STATBANK_TABLE_ID_LENGTH and id_or_url.isdigit():
        table_id = id_or_url

    elif (
        STATBANK_API_V1_ENDPOINT.origin == (url := furl(id_or_url)).origin
        and STATBANK_API_V1_ENDPOINT.path.segments == url.path.segments[:-1]
    ):
        table_id = url.path.segments[-1]

    if table_id is None:
        return apidata_legacy(id_or_url, payload, include_id)

    selections = _convert_to_api2_query(payload)
    statbank2 = pxwebapi.PxAPI(pxwebapi.STATBANK_CONFIG, client=client)
    metadata = statbank2.get_table_metadata(table_id)
    df = statbank2.get_table_with_selections(
        table_id,
        selections,
    ).to_pandas()

    drop_columns = [c for c in df.columns if c.endswith("_symbol")] + ["timestamp"]
    df = df.drop(columns=drop_columns)

    if "value" not in df.columns:
        # Ingen tabeller har mer enn én variabel med rollen "metric" (dvs. mer enn én statistikkvariabel)
        metric_id = metadata.role.metric[0]
        metric_name = metadata.dimension[metric_id].label
        other_var = [c for c in df.columns if not c.startswith(metric_id)]

        melted_df = df.melt(
            other_var,
            var_name=metric_name,
            value_name="value",
        )
        melted_df[metric_name] = melted_df[metric_name].str.removeprefix(
            metric_id + "_",
        )
        df = melted_df

    labeled = []

    for dimension in metadata.dimension.values():
        if dimension.label not in df.columns:
            continue
        if not dimension.category.label:
            raise ValueError
        labeled.append(df[dimension.label].map(dimension.category.label))

    if not include_id:
        return pd.concat(
            chain(
                labeled,
                (df["value"]),
            ),
            axis="columns",
        )

    unlabeled = []

    for var_id, dimension in metadata.dimension.items():
        if dimension.label not in df.columns:
            continue
        unlabeled.append(df[dimension.label].rename(var_id))

    interleaved = chain.from_iterable(zip(unlabeled, labeled, strict=False))
    return pd.concat(chain(interleaved, (df["value"],)), axis="columns")


def apidata_all(id_or_url: str = "", include_id: bool = False) -> pd.DataFrame:
    """Get ALL the contents of a published statbank-table as a pandas Dataframe.

    Args:
        id_or_url (str): The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
        include_id (bool): If you want to include "codes" in the dataframe, set this to True

    Returns:
        pd.DataFrame: Table-content
    """
    return apidata(id_or_url, apidata_query_all(id_or_url), include_id=include_id)


def apimetadata(id_or_url: str = "") -> dict[str, Any]:
    """Get the metadata of a published statbank-table as a dict.

    Args:
        id_or_url (str): The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".

    Returns:
        dict[str, Any]: The metadata of the table as the json returned from the API-get-request.

    Raises:
        ValueError: If the first parameter is not recognized as a statbank ID or a direct url.
    """
    if len(id_or_url) == STATBANK_TABLE_ID_LENGTH and id_or_url.isdigit():
        url = f"https://data.ssb.no/api/v0/no/table/{id_or_url}/"
    else:
        test_url = urllib.parse.urlparse(id_or_url)
        if not all([test_url.scheme, test_url.netloc]):
            error_msg = (
                "First parameter not recognized as a statbank ID or a direct url"
            )
            raise ValueError(error_msg)
        url = id_or_url
    res = r.get(url, timeout=5)
    res.raise_for_status()
    meta: dict[str, Any] = res.json()
    return meta


def apicodelist(
    id_or_url: str = "",
    codelist_name: str = "",
) -> dict[str, str] | dict[str, dict[str, str]]:
    """Get one specific or all the codelists of a published statbank-table as a dict or nested dicts.

    Args:
        id_or_url (str): The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
        codelist_name (str): The name of the specific codelist to get.

    Returns:
        dict[str, str] | dict[str, dict[str, str]]: The codelist of the table as a dict or a nested dict.

    Raises:
        ValueError: If the specified codelist_name is not in the returned metadata.
    """
    metadata = apimetadata(id_or_url)
    results = {}
    for col in metadata["variables"]:
        results[col["code"]] = dict(zip(col["values"], col["valueTexts"], strict=False))
    if codelist_name == "":
        return results
    if codelist_name in results:
        return results[codelist_name]
    for col in metadata["variables"]:
        if codelist_name == col["text"]:
            return dict(zip(col["values"], col["valueTexts"], strict=False))
    col_names = ", ".join([col["code"] for col in metadata["variables"]])
    error_msg = f"Cant find {codelist_name} among the available names: {col_names}"
    raise ValueError(error_msg)


def apidata_query_all(id_or_url: str = "") -> QueryWholeType:
    """Builds a query for ALL THE DATA in a table based on a request for metadata on the table.

    Args:
        id_or_url (str): The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".

    Returns:
        QueryWholeType: The prepared query based on all the codes in the table.
    """
    meta = apimetadata(id_or_url)["variables"]
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


# Credit: https://github.com/sehyoun/SSB_API_helper/blob/master/src/ssb_api_helper.py
def apidata_rotate(
    df: pd.DataFrame,
    ind: str = "year",
    val: str = "value",
) -> pd.DataFrame:
    """Rotate the dataframe so that years are used as the index.

    Args:
        df (pd.DataFrame): dataframe (from <get_from_ssb> function)
        ind (str): string of column name denoting time
        val (str): string of column name denoting values

    Returns:
        pd.DataFrame: pivoted dataframe
    """
    return df.pivot_table(
        index=ind,
        values=val,
        columns=[i for i in df.columns if i not in (ind, val)],
    )


# Error handeling and HTTP-return code interpretations follow
def _list_up(sequence: Sequence[str], conjunction: str = "and") -> str:
    if len(sequence) == 1:
        return sequence[0]

    return f"{', '.join(sequence[:-1])} {conjunction} {sequence[-1]}"


def _find_duplicates(items: Iterable[T]) -> list[T]:
    return [item for item, n in Counter(items).items() if n > 1]


def _read_error(id_or_url: str, query: QueryWholeType, response: r.Response) -> None:
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
            error_message = _check_selection(variable, id_or_url, query)
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
    id_or_url: str,
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

    meta = apimetadata(id_or_url)

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
