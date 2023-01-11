#!/usr/bin/env python3

import json
import urllib

import pandas as pd
import requests as r
from pyjstat import pyjstat


# Getting data from Statbank


def apidata(
    id_or_url: str = "",
    payload: dict = {"query": [], "response": {"format": "json-stat2"}},  # noqa: B006
    include_id: bool = False,
) -> pd.DataFrame:
    """
    Parameter1 - id_or_url: The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
    Parameter2: Payload, the query to include with the request.
    Parameter3: If you want to include "codes" in the dataframe, set this to True
    Returns: a pandas dataframe with the table
    """
    if len(id_or_url) == 5 and id_or_url.isdigit():
        url = f"https://data.ssb.no/api/v0/no/table/{id_or_url}/"
    else:
        test_url = urllib.parse.urlparse(id_or_url)
        if not all([test_url.scheme, test_url.netloc]):
            raise ValueError(
                "First parameter not recognized as a statbank ID or a direct url"
            )
        url = id_or_url

    repr(url)
    print(url)
    # Spør APIet om å få resultatet med requests-biblioteket
    resultat = r.post(url, json=payload)
    if resultat.status_code == 200:
        # Putt teksten i resultatet inn i ett pyjstat-datasett-objekt
        dataset = pyjstat.Dataset.read(resultat.text)
        # Skriv pyjstat-objektet ut som en pandas dataframe
        df = dataset.write("dataframe")
        # Om man ønsker IDen påført dataframen, så er vi fancy
        if include_id:
            df2 = dataset.write("dataframe", naming="id")
            skip = 0
            for i, col in enumerate(df2.columns):
                insert_at = (i + 1) * 2 - 1 - skip
                df_col_tocompare = df.iloc[:, insert_at - 1]
                # Sett inn kolonne på rett sted, avhengig av at navnet ikke er brukt
                # og at nabokolonnen ikke har samme verdier.
                if col not in df.columns and not df2[col].equals(df_col_tocompare):
                    df.insert(insert_at, col, df2[col])
                # Indexen må justeres, om vi lar være å skrive inn kolonnen
                else:
                    skip += 1
        df = df.convert_dtypes()
        return df
    elif resultat.status_code == 403:
        raise r.ConnectionError(
            f"Too big dataset? Try specifying a query into the function apidata (not apidata_all) to limit the returned data size. Status code {resultat.status_code}: {resultat.text}"
        )
    elif resultat.status_code == 400:
        raise r.ConnectionError(
            f"Bad Request, something might be wrong with your query... Status code {resultat.status_code}: {resultat.text}"
        )
    else:
        raise r.ConnectionError(f"Status code {resultat.status_code}: {resultat.text}")


def apidata_all(id_or_url: str = "", include_id: bool = False) -> pd.DataFrame:
    """
    Parameter1 - id_or_url: The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
    Returns: a pandas dataframe with the table
    """
    return apidata(id_or_url, apidata_query_all(id_or_url), include_id=include_id)


def apidata_query_all(id_or_url: str = "") -> dict:
    """
    Parameter1 - id_or_url: The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
    Returns: A dict of the prepared query based on all the codes in the table.
    """
    if len(id_or_url) == 5 and id_or_url.isdigit():
        url = f"https://data.ssb.no/api/v0/no/table/{id_or_url}/"
    else:
        test_url = urllib.parse.urlparse(id_or_url)
        if not all([test_url.scheme, test_url.netloc]):
            raise ValueError(
                "First parameter not recognized as a statbank ID or a direct url"
            )
        url = id_or_url
    res = r.get(url)
    if res.status_code == 200:
        meta = json.loads(res.text)["variables"]
        code_list = []
        for code in meta:
            tmp = {}
            for k, v in code.items():
                if k == "code":
                    tmp[k] = v
                if k == "values":
                    tmp["selection"] = {"filter": "item", k: v}
            code_list += [tmp]
        code_list
        query = {"query": code_list, "response": {"format": "json-stat2"}}
        return query
    else:
        raise r.ConnectionError(
            f"Can't get query metadata in first of two requests. Status code {res.status_code}: {res.text}"
        )


# Credit: https://github.com/sehyoun/SSB_API_helper/blob/master/src/ssb_api_helper.py
def apidata_rotate(df, ind="year", val="value"):
    """Rotate the dataframe so that years are used as the index
    Args:
        df (pandas.dataframe): dataframe (from <get_from_ssb> function
        ind (str): string of column name denoting time
        val (str): string of column name denoting values
    Returns:
        dataframe: pivoted dataframe
    """
    return df.pivot_table(
        index=ind,
        values=val,
        columns=[i for i in df.columns if i != ind and i != val],
    )
