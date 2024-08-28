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
from statbank import StatbankClient
import pandas as pd
import dapla as dp
from io import StringIO

fileclient = dp.FileClient()
statclient = StatbankClient()

# %%
df_stat = statclient.apidata("11721", {
  "query": [
    {
      "code": "Tid",
      "selection": {
        "filter": "item",
        "values": [
          "2022M10",
        ],
      },
    },
  ],
  "response": {
    "format": "json-stat2",
  },
}, include_id=True).drop(columns=["makrostørrelse", "statistikkvariabel"])
col_order = df_stat["ContentsCode"].unique().tolist()
mnr = df_stat.pivot_table(values="value", columns="ContentsCode", index=["Makrost", "måned"])
mnr = mnr[col_order].reset_index()
#mnr

# %%
desc = statclient.get_description("11721")

# %%
total_cols = (len(desc.variables[0]["variabler"]) +
len(desc.variables[0]["statistikkvariabler"]) +
len(desc.variables[0]["null_prikk_missing"]))
if len(mnr.columns) != total_cols:
    for colnum in range(total_cols - len(mnr.columns)):
        mnr[f"prikkecol_{colnum+1}"] = ""
#mnr

# %%
data = desc.transferdata_template(mnr)

# %%
data = desc.round_data(data)

# %%
data["knrmakrohovmnd1.dat"]

# %%
errors = statclient.validate(data, "11721")

# %%
list(errors.keys())

# %%
statclient.transfer(data, "11721")

# %%
