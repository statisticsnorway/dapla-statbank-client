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

# %% [markdown]
# ### Create client

# %%
client = StatbankClient(  # date="2023-02-21",
    shortuser="cfc",
    cc="thu",
    bcc="tir",
    overwrite=True,
    approve=2,
)

# %% [markdown]
# ### Sett dato interaktivt med widget

# %%
date = client.date_picker()

# %%
client.set_publish_date(date)

# %%
print(client)

# %%
client.log

# %% [markdown]
# ### Get description

# %%
filbeskrivelse_07495 = client.get_description(tableid="07495")

# %% [markdown]
# ### Load data

# %%
df_07495_fylker = pd.read_parquet("07495_statbank_fylker.parquet")
df_07495_landet = pd.read_parquet("07495_statbank_landet.parquet")

# %%
data_07495 = {
    "kargrs01fylker1.dat": df_07495_fylker,
    "kargrs01landet1.dat": df_07495_landet,
}

# %% [markdown]
# ### Validate data

# %%
filbeskrivelse_07495.validate(data_07495, raise_errors=True)

# %% [markdown]
# ### Round data

# %%
data_07495["kargrs01fylker1.dat"]

# %%
data_07495 = filbeskrivelse_07495.round_data(data_07495)

# %% [markdown]
# ### Transfer (actually sending the data)

# %%
transfer_07495 = client.transfer(data_07495, tableid="07495")
