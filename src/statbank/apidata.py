from __future__ import annotations

import urllib
from typing import TYPE_CHECKING

import requests as r
from pyjstat import pyjstat

if TYPE_CHECKING:
    import pandas as pd

    from statbank.api_types import QueryPartType
    from statbank.api_types import QueryWholeType


from statbank.statbank_logger import logger

# Getting data from Statbank

STATBANK_TABLE_ID_LENGTH = 5
REQUESTS_OK_RETURN = 200


def apidata(
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
    resultat = r.post(url, json=payload_now, timeout=10)
    resultat.raise_for_status()
    # Putt teksten i resultatet inn i ett pyjstat-datasett-objekt
    dataset_pyjstat = pyjstat.Dataset.read(resultat.text)
    # Skriv pyjstat-objektet ut som en pandas dataframe
    table_data: pd.DataFrame = dataset_pyjstat.write("dataframe")
    # Om man ønsker IDen påført dataframen, så er vi fancy
    if include_id:
        table_data_ids = dataset_pyjstat.write("dataframe", naming="id")
        skip = 0
        for i, col in enumerate(table_data_ids.columns):
            insert_at = (i + 1) * 2 - 1 - skip
            df_col_tocompare = table_data.iloc[:, insert_at - 1]
            # Sett inn kolonne på rett sted, avhengig av at navnet ikke er brukt
            # og at nabokolonnen ikke har samme verdier.
            if col not in table_data.columns and not table_data_ids[col].equals(
                df_col_tocompare,
            ):
                table_data.insert(insert_at, col, table_data_ids[col])
            # Indexen må justeres, om vi lar være å skrive inn kolonnen
            else:
                skip += 1
    return table_data.convert_dtypes()


def apidata_all(id_or_url: str = "", include_id: bool = False) -> pd.DataFrame:
    """Get ALL the contents of a published statbank-table as a pandas Dataframe.

    Args:
        id_or_url (str): The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
        include_id (bool): If you want to include "codes" in the dataframe, set this to True

    Returns:
        pd.DataFrame: Table-content
    """
    return apidata(id_or_url, apidata_query_all(id_or_url), include_id=include_id)


def apidata_query_all(id_or_url: str = "") -> QueryWholeType:
    """Builds a query for ALL THE DATA in a table based on a request for metadata on the table.

    Args:
        id_or_url (str): The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".

    Returns:
        QueryWholeType: The prepared query based on all the codes in the table.

    Raises:
        ValueError: If the parameter is not a valid statbank ID or a direct url.
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
    meta = res.json()["variables"]
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
