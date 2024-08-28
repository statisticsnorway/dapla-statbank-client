# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: dapla-statbank-client
#     language: python
#     name: dapla-statbank-client
# ---

# %%
import dapla as dp
import pandas as pd

from statbank import StatbankClient

fileclient = dp.FileClient()
statclient = StatbankClient()

# %%
tableid = "03629"

# %%
df_stat = statclient.apidata(
    tableid,
    {
        "query": [
            {
                "code": "Tid",
                "selection": {
                    "filter": "item",
                    "values": [
                        "2021",
                    ],
                },
            },
        ],
        "response": {
            "format": "json-stat2",
        },
    },
    include_id=True,
)

# %%
df_stat

# %%
df_piv = (
    df_stat.pivot_table(values="value", index="år", columns="ContentsCode")
    .astype("Int64")
    .reset_index()
)
df_piv

# %%
df_piv.melt(id_vars=df_piv.columns[0])

# %%
df = pd.DataFrame()
df["tid"] = df_piv["år"]
df["konfliktar"] = df_piv["Konflikter"]
df["personar"] = df_piv["Arbeidstakarar"]
df["dagar"] = df_piv["TapteArbeidsdagar"]
df["prikk1"] = None
df["prikk2"] = None
df["prikk3"] = None


# %%
df

# %%
df = df.fillna("")

# %%
desc = statclient.get_description(tableid)

# %%
data = desc.transferdata_template(df)

# %%
desc.validate(data)

# %%
desc.codelists
