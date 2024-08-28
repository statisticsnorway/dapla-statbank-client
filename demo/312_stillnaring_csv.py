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
import pandas as pd
from statbank import StatbankClient

# %%
client = StatbankClient()

# %%
uttrekk = client.get_description("08771")

# %%
uttrekk.transferdata_template()

# %%
csv_data = pd.read_csv("stillnaring.csv", sep=";", header=None, index_col=0)
csv_data

# %%
csv_data[4] = csv_data[4].str.replace(",",".", regex=False).astype(float)
csv_data[6] = csv_data[6].str.replace(",",".", regex=False).astype(float)
csv_data = csv_data.fillna("")

# %%
csv_data.dtypes

# %%
data = {"stillnaring1.dat": csv_data}
data = uttrekk.round_data(data)

# %%
transfer_result = client.transfer(data, "08771")
