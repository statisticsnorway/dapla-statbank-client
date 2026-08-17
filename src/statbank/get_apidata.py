from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING
from typing import Any
from typing import cast

import msgspec
import pandas as pd
import pxwebapi
import pxwebapi.expression
import pxwebapi.query_types
import pxwebapi.response_types
from furl import furl
from pxwebapi._msg_hooks import enc_hook

from .get_apidata_internal import apicodelist_internal
from .get_apidata_internal import apidata_all_internal
from .get_apidata_internal import apidata_internal
from .get_apidata_internal import apidata_query_all
from .get_apidata_internal import apimetadata_internal

if TYPE_CHECKING:
    from collections.abc import Iterable

    import httpx

    from .api_types import QueryPartType
    from .api_types import QueryWholeType


# Getting data from Statbank

STATBANK_TABLE_ID_LENGTH = 5
STATBANK_API_V0_ENDPOINT = furl("https://data.ssb.no/api/v0/no/table")


def convert_to_api2_selection(  # noqa: PLR0912
    old_selections: Iterable[QueryPartType],
) -> list[pxwebapi.query_types.Selection]:
    """Converts a Pxweb version 0 selection to a Pxweb version 2 selection.

    Args:
        old_selections: A iterable of v0 selections, the "query" part of a version 0 query.

    Returns:
        A list of new selections. This are represented as `msgspec.Struct`s not dicts.

    Raises:
        ValueError: If the old selection could not be converted.
    """
    new_selections: list[pxwebapi.query_types.Selection] = []
    expression: pxwebapi.expression.Expression

    for old_select in old_selections:
        variable_code = old_select["code"]
        old_filter = old_select["selection"]["filter"]
        old_values = old_select["selection"]["values"]
        new_values: list[pxwebapi.expression.Expression]
        code_list = None
        if old_filter.lower() == "top":
            if len(old_values) == 1:
                expression = pxwebapi.expression.TopExpression(
                    int(old_values[0]),
                )
            elif len(old_values) == 2:
                expression = pxwebapi.expression.TopExpression(
                    int(old_values[0]),
                    int(old_values[1]),
                )
            else:
                msg = "Invalid TOP select expression"
                raise ValueError(msg)
            new_values = [expression]

        elif old_filter.lower() == "bottom":
            if len(old_values) == 1:
                expression = pxwebapi.expression.BottomExpression(
                    int(old_values[0]),
                )
            elif len(old_values) == 2:
                expression = pxwebapi.expression.BottomExpression(
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
            expression = pxwebapi.expression.RangeExpression(
                old_values[0],
                old_values[1],
            )
            new_values = [expression]
        elif old_filter.lower() == "to":
            if len(old_values) != 1:
                msg = "Invalid TO select expression"
                raise ValueError(msg)
            expression = pxwebapi.expression.ToExpression(old_values[0])
            new_values = [expression]

        elif old_filter.lower() == "from":
            if len(old_values) != 1:
                msg = "Invalid FROM select expression"
                raise ValueError(msg)
            expression = pxwebapi.expression.FromExpression(old_values[0])
            new_values = [expression]
        else:
            if old_filter not in ("item", "all"):
                code_list = old_filter.replace(":", "_", 1)
            new_values = [pxwebapi.expression.CodeExpression(c) for c in old_values]

        new_select = pxwebapi.query_types.Selection(
            variable_code,
            code_list,
            new_values,
        )
        new_selections.append(new_select)

    return new_selections


def _get_table_id(id_or_url: str) -> str | None:
    if len(id_or_url) == STATBANK_TABLE_ID_LENGTH and id_or_url.isdigit():
        return id_or_url

    url = furl(id_or_url)

    if (
        STATBANK_API_V0_ENDPOINT.origin == url.origin
        and STATBANK_API_V0_ENDPOINT.path.segments == url.path.segments[:-1]
    ):
        return cast("str", url.path.segments[-1])

    return None


def _stack_table(
    df: pd.DataFrame,
    statbank2: pxwebapi.PxAPI,
    table_id: str,
    metadata: pxwebapi.response_types.DatasetResponse,
    selections: list[pxwebapi.query_types.Selection],
) -> pd.DataFrame:
    """Gjør tabellene man får med parquet som OutputFormat til å bli lik de man fikk med den gamle apidata funksjonen.

    Hvis kun en statistikkvariabel-verdi er valgt, så får denne kolonnenavnet "value" i Parquet-dataene, som før,
    men det er ingen kolonne  som angir hvilen statistikkvariabel som er brukt, slik at den må legges til igjen.
    Er flere verdier valgt stabler API statistikkvariabelen utover, slik at vi må stable variabelen sammen igjen,
    For å etterape det gamle APIet.
    """
    drop_columns = [c for c in df.columns if c.endswith("_symbol")] + ["timestamp"]
    df = df.drop(columns=drop_columns)

    metric_id = metadata.role.metric[0]
    metric_name = metadata.dimension[metric_id].label

    if "value" not in df.columns:
        # Ingen tabeller har mer enn én variabel med rollen "metric" (dvs. mer enn én statistikkvariabel) i Statistikkbanken
        other_var = [c for c in df.columns if not c.startswith(metric_id)]

        melted_df = df.melt(
            other_var,
            var_name=metric_name,
            value_name="value",
        )
        melted_df[metric_name] = melted_df[metric_name].str.removeprefix(
            metric_id + "_",
        )
        return melted_df

    try:
        metric_selection = next(
            filter(lambda s: s.variable_code == metric_id, selections),
        )
    except StopIteration:
        metric_selection = pxwebapi.query_types.Selection(
            metric_id,
            value_codes=[pxwebapi.expression.CodeExpression("*")],
        )
    else:
        if not metric_selection:
            metric_selection = pxwebapi.query_types.Selection(
                metric_id,
                value_codes=[pxwebapi.expression.CodeExpression("*")],
            )

    matcher = statbank2.get_expression_matcher(table_id, metric_selection)
    selected_code = matcher.get_codes_from_expressions()[0]
    df.insert(len(df.columns) - 1, metric_name, selected_code)

    return df


def _label_table(
    df: pd.DataFrame,
    metadata: pxwebapi.response_types.DatasetResponse,
    include_id: bool,
) -> pd.DataFrame:
    labeled = []

    for dimension in metadata.dimension.values():
        if dimension.label not in df.columns:
            continue
        labeled.append(df[dimension.label].map(dimension.category.label))

    if not include_id:
        return pd.concat(
            chain(
                labeled,
                (df["value"],),
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


def apidata(
    id_or_url: str,
    payload: QueryWholeType | None = None,
    include_id: bool = False,
    client: httpx.Client | None = None,
) -> pd.DataFrame:
    """Get the contents of a Statbank-table as a pandas Dataframe, specifying a query to limit the return.

        Queries to the old external Statbank is automatically redirected to the new Statbank API.
        If a ID is used, or a URL matching the old API external Statbank, the new v2 of the API is used.
        If a URL to a internal Statbank API or any other pxweb v0 compatible API is used (like FHI's "Folkehelsestatistikk"),
        the old API is used to fetch the data.

    Args:
        id_or_url: The id of the Statbank-table or a URL to that table on any compatible pxweb v0 API, like the internal Statbank.
        payload: a dict in the shape of a QueryWhole, to include with the request, can be copied from the statbank-webpage.
        include_id: If you want to include "codes" in the dataframe, set this to True.
        client: A optional `httpx.Client` to use, to fetch the data.

    Returns:
        pd.DataFrame: The table-content.
    """
    table_id = _get_table_id(id_or_url)

    if not table_id:
        return apidata_internal(id_or_url, payload, include_id)

    selections = convert_to_api2_selection(payload["query"]) if payload else []
    statbank2 = pxwebapi.PxAPI(pxwebapi.STATBANK_CONFIG, client=client)
    df = statbank2.get_table_with_selections(
        table_id,
        selections,
    ).to_pandas()

    metadata = statbank2.get_table_metadata(table_id)

    df = _stack_table(
        df,
        statbank2,
        table_id,
        metadata,
        selections,
    )

    return _label_table(df, metadata, include_id)


def apidata_all(
    id_or_url: str,
    include_id: bool = False,
    client: httpx.Client | None = None,
) -> pd.DataFrame:
    """Get all the contents of a published statbank-table as a pandas Dataframe.

    Args:
        id_or_url: The id of the Statbank-table or a URL to that table on any compatible pxweb v0 API, like the internal Statbank.
        include_id: If you want to include "codes" in the dataframe, set this to True.
        client: A optional `httpx.Client` to use, to fetch the data.

    Returns:
        pd.DataFrame: Table-content

    """
    table_id = _get_table_id(id_or_url)

    if not table_id:
        return apidata_all_internal(id_or_url, include_id)

    statbank2 = pxwebapi.PxAPI(pxwebapi.STATBANK_CONFIG, client=client)
    query = statbank2.get_view(id_or_url, view=pxwebapi.pxwebapi.TableView.ALL)

    df = statbank2.get_table_with_selections(
        table_id,
        query.selection,
    ).to_pandas()

    metadata = statbank2.get_table_metadata(table_id)

    df = _stack_table(
        df,
        statbank2,
        table_id,
        metadata,
        query.selection,
    )

    return _label_table(df, metadata, include_id)


def apimetadata(id_or_url: str, client: httpx.Client | None = None) -> dict[str, Any]:
    """Get the metadata of a published statbank-table as a dict.

    Args:
        id_or_url: The id of the Statbank-table or a URL to that table on any compatible pxweb v0 API, like the internal Statbank.
        client: A optional `httpx.Client` to use, to fetch the data.

    Returns:
        dict[str, Any]: The metadata of the table as the json returned from the API-get-request.
    """
    table_id = _get_table_id(id_or_url)

    if not table_id:
        return apimetadata_internal(id_or_url)

    statbank2 = pxwebapi.PxAPI(pxwebapi.STATBANK_CONFIG, client=client)
    metadata = statbank2.get_table_metadata(table_id)
    return cast("dict[str, Any]", msgspec.to_builtins(metadata, enc_hook=enc_hook))


def apicodelist(
    id_or_url: str,
    codelist_name: str | None = None,
    client: httpx.Client | None = None,
) -> dict[str, str] | dict[str, dict[str, str]]:
    """Get one specific or all the codelists of a published statbank-table as a dict or nested dicts.

    Args:
        id_or_url: The id of the Statbank-table or a URL to that table on any compatible pxweb v0 API.
        codelist_name (str): The name of the specific codelist to get.
        client: A optional `httpx.Client` to use, to fetch the data.

    Returns:
        dict[str, str] | dict[str, dict[str, str]]: The codelist of the table as a dict or a nested dict.

    Raises:
        ValueError: If the specified codelist_name is not in the returned metadata.
    """
    table_id = _get_table_id(id_or_url)

    if not table_id:
        return apicodelist_internal(id_or_url, codelist_name)

    statbank2 = pxwebapi.PxAPI(pxwebapi.STATBANK_CONFIG, client=client)
    metadata = statbank2.get_table_metadata(table_id)

    if not codelist_name:
        return {
            var_id: variable.category.label
            for var_id, variable in metadata.dimension.items()
        }

    try:
        variable = metadata.dimension[codelist_name]
    except KeyError:
        pass
    else:
        return variable.category.label

    try:
        variable = next(
            v for v in metadata.dimension.values() if v.label == codelist_name
        )
    except StopIteration:
        pass
    else:
        return variable.category.label

    var_id = ", ".join(metadata.id)
    error_msg = f"Cant find {codelist_name} among the available names: {var_id}"
    raise ValueError(error_msg)


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


__all__ = [
    "apicodelist",
    "apidata",
    "apidata_all",
    "apidata_query_all",
    "apidata_rotate",
    "apimetadata",
    "convert_to_api2_selection",
]
