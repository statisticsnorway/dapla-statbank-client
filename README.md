# dapla-statbank-client
Used internally by SSB (Statistics Norway).
Validates and transfers data from Dapla to Statbank.
Gets data from public and internal statbank.


### Usage Transferring
```python
from statbank import StatbankTransfer
job1 = StatbankTransfer(df_06399, 
                           tabellid="06339", 
                           lastebruker="LASTEBRUKER")
```
The simplest form of usage, is directly-transferring using the StatbankTransfer-class. If the statbanktable expects multiple "deltabeller", dataframes must be passed in a list, in the correct order.

Validation will happen by default on user-side, in Python, using the "UttrekksBeskrivelse". Validation can be disabled using one of StatbankTransfer's parameters.
Validation happens on the number of tables, number of columns, code usage in categorical columns, code usage in "suppression-columns" (prikkkolonner), and on timeformats (both length and characters used).

You can validate the data using the "UttrekksBeskrivelse", without starting a transfer, like this:

```python
from statbank import StatbankUttrekksBeskrivelse
filbeskrivelse_06339 = StatbankUttrekksBeskrivelse(tabellid="06339", lastebruker="LASTEBRUKER")
filbeskrivelse_06339.validate_dfs(df_06339)
```

If you have multiple statbank-tables you want to transfer at the same time, you can use "BatchTransfer". You will only be asked for password once. The username for statbank must be the same for all tables, and will be copied from the first job in the batch, to the whole batch. For this to work, all jobs must be "delayed".

```python
from statbank import StatbankTransfer, StatbankBatchTransfer
job1 = StatbankTransfer(df_06399, 
                           tabellid="06339", 
                           lastebruker="LASTEBRUKER",
                         delay = True )
job2 = StatbankTransfer(df_08531, 
                           tabellid="08531", 
                           lastebruker="LASTEBRUKER",
                         delay = True )
StatbankBatchTransfer([job1, job2])
```


### Usage get apidata

```python
from statbank import apidata_all
apidata_all("06339", include_id=True)
```
`apidata_all`, does not need a specified query, it will build its own query, trying to get *all the data* from the table. This might be too much, resulting in an error.

The `include_id`-parameter is a bit *magical*, it gets both codes and value-columns for categorical columns, and tries to merge these next to each other, it also makes a check if the content i the same, then it will not include the content twice.

If you want to specify a query, to limit the response, use the function `apidata` instead.\
Here we are requesting an "internal table" which only people at SSB have access to, with a specified URL and query.
```python
from statbank import apidata
query = {'query': [{'code': 'Region', 'selection': {'filter': 'vs:Landet', 'values': ['0']}}, {'code': 'Alder', 'selection': {'filter': 'vs:AldGrupp19', 'values': ['000', '001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011', '012', '013', '014', '015', '016', '017', '018', '019', '020', '021', '022', '023', '024', '025', '026', '027', '028', '029', '030', '031', '032', '033', '034', '035', '036', '037', '038', '039', '040', '041', '042', '043', '044', '045', '046', '047', '048', '049', '050', '051', '052', '053', '054', '055', '056', '057', '058', '059', '060', '061', '062', '063', '064', '065', '066', '067', '068', '069', '070', '071', '072', '073', '074', '075', '076', '077', '078', '079', '080', '081', '082', '083', '084', '085', '086', '087', '088', '089', '090', '091', '092', '093', '094', '095', '096', '097', '098', '099', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116', '117', '118', '119+']}}, {'code': 'Statsbrgskap', 'selection': {'filter': 'vs:Statsborgerskap', 'values': ['000']}}, {'code': 'Tid', 'selection': {'filter': 'item', 'values': ['2022']}}], 'response': {'format': 'json-stat2'}}
apidata("https://i.ssb.no/pxwebi/api/v0/no/prod_24v_intern/START/be/be01/folkemengde/Rd0002Aa",
        query,
        include_id = True
       )
```


