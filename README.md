# dapla-statbank-client
Used internally by SSB.
Can transfer data from Dapla to Statbank.
Can get data from public and internal statbank-


### Usage Transferring
```python
from statbank import StatbankTransfer
job1 = StatbankTransfer(df_06399, 
                           tabellid="06339", 
                           lastebruker="LAST360")
```
Enkleste formen for bruk av koden, sender inn en dataframe til en statbanktabell som forventer en "deltabell".

StatbankTransfer benytter seg av StatbankUttrekksBeskrivelse for å validere innholdet som sendes inn. Antall tabeller, antall kolonner, benyttede koder i kategoriske kolonner, benyttede koder i "prikkekolonner", format på tid.
Man kan validere dataene, uten å sende dem til Transfer, slik:

```python
from statbank import StatbankUttrekksBeskrivelse
filbeskrivelse_06339 = StatbankUttrekksBeskrivelse(tabellid="06339", lastebruker="LAST360")
filbeskrivelse_06339.validate_dfs(df_06339)
```

Om man har flere tabeller, man ønsker å sende samtidig, kan man lage en BatchTransfer. Da blir man bare spurt om å skrive inn passord én gang, for å sende alle tabellene. For at dette skal fungere så må Transferene være "delayed".

```python
from statbank import StatbankTransfer, StatbankBatchTransfer
job1 = StatbankTransfer(df_06399, 
                           tabellid="06339", 
                           lastebruker="LAST360",
                         delay = True )
job2 = StatbankTransfer(df_08531, 
                           tabellid="08531", 
                           lastebruker="LAST360",
                         delay = True )
StatbankBatchTransfer([job1, job2])
```


### Usage get apidata

```python
from statbank import apidata_all
apidata_all("06339", include_id=True)
```
"apidata_all", trenger ingen spesifisert query, den bygger sin egen, men henter da *all data* fra tabellen. Dette kan fort bli for mye...\
Med "include_id"-parameteret satt til `True` vil funksjonen flette id/kode-kolonner med kodenes verdier, der disse er ulike. (Prøv med og uten, for å se forskjellen.)

Med "apidata" må man spesifisere en query. Her hentes en "intern" tabell, som trenger en query for å ikke bli for stor...
```python
from statbank import apidata
query = {'query': [{'code': 'Region', 'selection': {'filter': 'vs:Landet', 'values': ['0']}}, {'code': 'Alder', 'selection': {'filter': 'vs:AldGrupp19', 'values': ['000', '001', '002', '003', '004', '005', '006', '007', '008', '009', '010', '011', '012', '013', '014', '015', '016', '017', '018', '019', '020', '021', '022', '023', '024', '025', '026', '027', '028', '029', '030', '031', '032', '033', '034', '035', '036', '037', '038', '039', '040', '041', '042', '043', '044', '045', '046', '047', '048', '049', '050', '051', '052', '053', '054', '055', '056', '057', '058', '059', '060', '061', '062', '063', '064', '065', '066', '067', '068', '069', '070', '071', '072', '073', '074', '075', '076', '077', '078', '079', '080', '081', '082', '083', '084', '085', '086', '087', '088', '089', '090', '091', '092', '093', '094', '095', '096', '097', '098', '099', '100', '101', '102', '103', '104', '105', '106', '107', '108', '109', '110', '111', '112', '113', '114', '115', '116', '117', '118', '119+']}}, {'code': 'Statsbrgskap', 'selection': {'filter': 'vs:Statsborgerskap', 'values': ['000']}}, {'code': 'Tid', 'selection': {'filter': 'item', 'values': ['2022']}}], 'response': {'format': 'json-stat2'}}
apidata("https://i.ssb.no/pxwebi/api/v0/no/prod_24v_intern/START/be/be01/folkemengde/Rd0002Aa",
        query,
        include_id = True
       )
```


