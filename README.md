# dapla-statbank-client
Used internally by SSB (Statistics Norway).
Validates and transfers data from Dapla to Statbank.
Gets data from public and internal statbank.


### Installing from Pypi with Poetry
If the project-folder doesnt already have a pyproject.toml with poetry-info, run this in the dapla-jupyterlab-terminal:
```bash
poetry init
```
When poetry is initialized in the project-folder, install the package from Pypi, and create a kernel:
```bash
poetry add dapla-statbank-client
poetry run python -m ipykernel install --user --name test_statbank
```
Make a notebook with the kernel you just made, try this code to verify the package is available:
```python
from statbank import StatbankClient
stat_client = StatbankClient(loaduser = "LASTEBRUKER")
# Change LASTEBRUKER to your load-statbank-username
# Fill out password
# Default publishing-date is TOMORROW
print(stat_client)
```

### Building datasets
You can look at the "filbeskrivelse" which is returned from `stat_client.get_description()` in its own local class: StatbankUttrekksBeskrivelse
```python
description_06339 = stat_client.get_description(tableid="06339")
print(description_06339)
```
This should have all the information you are used to reading out from the old "Filbeskrivelse". And describes how you should construct your data.
```python
# Interesting attributes
description_06339.subtables
description_06339.variables
description_06339.codelists
description_06339.suppression
```
After starting to construct your data, you can validate it against the Uttrekksbeskrivelse, using the validate-method, without starting a transfer, like this:
```python
stat_client.validate(df_06339, tableid="06339")
```
Validation will happen by default on user-side, in Python.
Validation happens on the number of tables, number of columns, code usage in categorical columns, code usage in "suppression-columns" (prikkekolonner), and on timeformats (both length and characters used).

Get the "template" for the dictionary that needs to be transferred like this:
```python
description_06339.transferdata_template()
```
This both returns the dict, and prints it, depending on what you want to do with it. Use it to insert your own DataFrames into, and send it to .transfer()


### Usage Transferring

```python
stat_client.transfer({"deltabellfilnavn.dat" : df_06399}, "06339")
```
The simplest form of usage, is directly-transferring using the transfer-method under the client-class. If the statbanktable expects multiple "deltabeller", dataframes must be passed in a list, in the correct order.


### Getting apidata

```python
df_06339 = stat_client.apidata_all("06339", include_id=True)
```
`apidata_all`, does not need a specified query, it will build its own query, trying to get *all the data* from the table. This might be too much, resulting in an error.

The `include_id`-parameter is a bit *magical*, it gets both codes and value-columns for categorical columns, and tries to merge these next to each other, it also makes a check if the content is the same, then it will not include the content twice.

If you want to specify a query, to limit the response, use the method `apidata` instead.\
Here we are requesting an "internal table" which only people at SSB have access to, with a specified URL and query.
```python
query = {'query': [{'code': 'Region', 'selection': {'filter': 'vs:Landet', 'values': ['0']}}, {'code': 'Alder', 'selection': {'filter': 'vs:AldGrupp19', 'values': ['000', '001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011', '012', '013', '014', '015', '016', '017', '018', '019', '020', '021', '022', '023', '024', '025', '026', '027', '028', '029', '030', '031', '032', '033', '034', '035', '036', '037', '038', '039', '040', '041', '042', '043', '044', '045', '046', '047', '048', '049', '050', '051', '052', '053', '054', '055', '056', '057', '058', '059', '060', '061', '062', '063', '064', '065', '066', '067', '068', '069', '070', '071', '072', '073', '074', '075', '076', '077', '078', '079', '080', '081', '082', '083', '084', '085', '086', '087', '088', '089', '090', '091', '092', '093', '094', '095', '096', '097', '098', '099', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116', '117', '118', '119+']}}, {'code': 'Statsbrgskap', 'selection': {'filter': 'vs:Statsborgerskap', 'values': ['000']}}, {'code': 'Tid', 'selection': {'filter': 'item', 'values': ['2022']}}], 'response': {'format': 'json-stat2'}}

df_folkemengde = stat_client.apidata("https://i.ssb.no/pxwebi/api/v0/no/prod_24v_intern/START/be/be01/folkemengde/Rd0002Aa",
                                     query,
                                     include_id = True
                                    )
```

`apidata_rotate` is a thin wrapper around pivot_table. Stolen from: https://github.com/sehyoun/SSB_API_helper/blob/master/src/ssb_api_helper.py
```python
df_folkemengde_rotert = stat_client.rotate(df_folkemengde, 'tidskolonne', "verdikolonne")
```


To import the apidata-functions outside the client (no need for password) do the imports like this:
```python
from statbank.apidata import apidata_all, apidata, apidata_rotate
```


### Saving and restoring Uttrekksbeskrivelser and Transfers as json

From `stat_client.transfer()` you will recieve a StatbankTransfer object, from `stat_client.get_description` a StatbankUttrekksBeskrivelse-object. These can be serialized and saved to disk, and later be restored.

```python
filbesk_06339 = stat_client.get_description("06339")
filbesk_06339.to_json("path.json")
# Later the file can be restored with
filbesk_06339_new = stat_client.read_description_json("path.json")
```

Some deeper data-structures, like the dataframes in the transfer will not be serialized and stored with the transfer-object in its json.

---

### Version history
- 0.0.5 Still some parameter issues
- 0.0.4 More test coverage, some bugs fixed in rounding checks and parameter-passing 
- 0.0.3 Removed batches, stripping uttrekk from transfer, rounding function on uttrekk, data required in as a dict of dataframes, with "deltabell-navn". Tableid now works to transfer to instead of only "hovedtabellnavn"
- 0.0.2 Starting alpha, fine-tuning release to Pypi on github-release
- 0.0.1 Client, transfer, description, apidata. Quite a lot of work done already. Pre-alpha.
