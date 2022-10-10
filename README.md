# dapla-statbank-client
Used internally by SSB.
Can transfer data from Dapla to Statbank.
Can get data from public and internal statbank-


**Usage Transferring**
```python
from dapla_toolbelt.dapla.statbank import StatbankTransfer
job1 = StatbankTransfer(df_06399, 
                           tabellid="06339", 
                           lastebruker="LAST360")
```
Enkleste formen for bruk av koden, sender inn en dataframe til en statbanktabell som forventer en "deltabell".

StatbankTransfer benytter seg av StatbankUttrekksBeskrivelse for å validere innholdet som sendes inn. Antall tabeller, antall kolonner, benyttede koder i kategoriske kolonner, benyttede koder i "prikkekolonner", format på tid.
Man kan validere dataene, uten å sende dem til Transfer, slik:

```python
from dapla_toolbelt.dapla.statbank import StatbankUttrekksBeskrivelse
filbeskrivelse_06339 = StatbankUttrekksBeskrivelse(tabellid="06339", lastebruker="LAST360")
filbeskrivelse_06339.validate_dfs(df_06339)
```

Om man har flere tabeller, man ønsker å sende samtidig, kan man lage en BatchTransfer. Da blir man bare spurt om å skrive inn passord én gang, for å sende alle tabellene. For at dette skal fungere så må Transferene være "delayed".

```python
from dapla_toolbelt.dapla.statbank import StatbankTransfer, StatbankBatchTransfer
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

