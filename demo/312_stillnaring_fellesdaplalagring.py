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

# %%
fileclient = dp.FileClient()
statclient = StatbankClient()

# %%
stillnaring_path = "gs://ssb-staging-dapla-felles-data-delt/dapla-statbank-client-testing/08771_stillnaring1_seksjon312.txt"

# %%
with fileclient.gcs_open(stillnaring_path, "r") as stillnaring_fil:
    stillnaring = pd.read_csv(stillnaring_fil, sep=";", header=None)
stillnaring = stillnaring.fillna("")

# %%
stillnaring.info()

# %%
stillnaring.head()

# %%
desc = statclient.get_description("08771")

# %%
#desc.variables

# %%
desc.transferdata_template()

# %%
validate_result = statclient.validate({"stillnaring1.dat" : stillnaring}, "08771")

# %%
transfer_result = statclient.transfer({"stillnaring1.dat" : stillnaring}, "08771")

# %%
print(transfer_result)

# %% [markdown]
# ### Det ligger mye interssant i resultatet av overføringen

# %%
transfer_result.oppdragsnummer

# %%
transfer_result.response.text
