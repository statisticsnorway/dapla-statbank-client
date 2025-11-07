# ---
# jupyter:
#   jupytext:
#     formats: py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.18.1
#   kernelspec:
#     display_name: dapla-statbank-client
#     language: python
#     name: dapla-statbank-client
# ---

# %% [markdown]
# # Enkleste bruk av pakken med "ferdig data"
#
# Pass på at du har innstallert pakken i en "kernel" du kan bruke i notebooket

# %%
import pandas as pd

from statbank import StatbankClient

# %% [markdown]
# ### Få tak i data
# Hovedtabell 07495 har to deltabeller, du har nok ikke tilgang på disse filene. \
# Du bør finne egen data, kanskje fra fjoråret, og bruke seksjonens lastebruker osv. \
# Endre koden til noe du kan teste med.

# %%
df_07495_fylker = pd.read_parquet("07495_statbank_fylker.parquet")
df_07495_landet = pd.read_parquet("07495_statbank_landet.parquet")

# %% [markdown]
# ### Lag statbank-client
# Din lastebruker vil være anderledes

# %%
client = StatbankClient()

# %% [markdown]
# ### Vi må vite hvilke deltabeller hver dataframe representerer
# Din hovedtabell har kanskje bare èn deltabell, men vi trenger fortsatt navnet på den.

# %%
data_07495 = {
    "kargrs01fylker1.dat": df_07495_fylker,
    "kargrs01landet1.dat": df_07495_landet,
}

# %% [markdown]
# ### Du kan validere rett på clienten?

# %%
client.validate(data_07495, "07495")

# %% [markdown]
# ### Overfør data med .transfer()
# Om du har "hovedtabellnavn" istedenfor "tabell-id" så skal det i teorien fungere (kanskje også for interntabeller)

# %%
client.transfer(data_07495, "07495")
