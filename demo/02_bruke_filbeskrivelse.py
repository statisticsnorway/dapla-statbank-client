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
client = StatbankClient()

# %%
print(client)

# %%
date = client.date_picker()

# %%
import datetime as dt

# %%
OSLO_TIMEZONE = dt.timezone(dt.timedelta(hours=1))
dt.datetime.combine(
                date.value,
                dt.datetime.min.time(),
            ).astimezone(
                OSLO_TIMEZONE,
            ) + dt.timedelta(hours=1)

# %%
date.value

# %%
client.set_publish_date("2024-05-25")

# %%
client.date

# %% [markdown]
# ### Get description

# %%
filbeskrivelse_07495 = client.get_description(tableid="07495")

# %%
from datetime import datetime

filbesk_tid = datetime.strptime(filbeskrivelse_07495.time_retrieved.split(" ")[0], "%d.%m.%Y")
if filbesk_tid.year != datetime.now().year:
    raise ValueError("Filbeskrivelsen er fra i fjor, hent den på nytt.")

# %% [markdown]
# ### Load data

# %%
df_07495_fylker = pd.read_parquet("07495_statbank_fylker.parquet")
df_07495_landet = pd.read_parquet("07495_statbank_landet.parquet")

# %%
filbeskrivelse_07495.transferdata_template(df_07495_fylker, df_07495_landet)

# %%
data_07495 = {"kargrs01fylker1.dat": df_07495_fylker,
              "kargrs01landet1.dat": df_07495_landet}

# %% [markdown]
# ### Validate data

# %%
resultat_validering = filbeskrivelse_07495.validate(data_07495)

# %% [markdown]
# ### Round data

# %%
data_07495["kargrs01fylker1.dat"]

# %%
1.5
2

2.50000000
2

# %%
data_07495 = filbeskrivelse_07495.round_data(data_07495)

# %% [markdown]
# ### Transfer (actually sending the data)

# %%
transfer_07495 = client.transfer(data_07495, "07495")
