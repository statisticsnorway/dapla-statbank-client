#!/usr/bin/env python3

from .auth import StatbankAuth
from .uttrekk import StatbankUttrekksBeskrivelse
from .transfer import StatbankTransfer
from .batchtransfer import StatbankBatchTransfer

import datetime
from datetime import timedelta as td
import pandas as pd
import ipywidgets as widgets
import os

class StatbankClient(StatbankAuth):
    def __init__(self,
            loaduser = "",
            date: datetime.datetime = datetime.datetime.now() + td(days=1),
            shortuser: str = "",
            cc: str = "",
            bcc: str = "",
            overwrite: str = '1',
            approve: str = '2',
            validation: bool = True,
            ):
        self.loaduser = loaduser
        self.date = date
        self.shortuser = shortuser
        self.cc = cc
        self.bcc = bcc
        self.overwrite = overwrite
        self.approve = approve
        self.validation = validation
        self._validate_params_init()
        self.__headers = self._build_headers()
        self.log = []

    # Representation
    def __str__(self):
        return f'''StatbankClient for user {self.loaduser}
        Publishing at {self.date}
        Shortuser {self.shortuser}
        Sending mail to {self.cc}
        And sending mail to {self.bcc}
        Overwrite set to {self.overwrite}
        Approve set to {self.approve}
        Validation set to {self.validation}
        
        Log:
        ''' + "\n\t".join(self.log)

    def __repr__(self):
        return f'StatbankClient(loaduser = "{self.loaduser}")'
    
    # Publishing date handeling
    def date_picker(self) -> None:        
        datepicker =  widgets.DatePicker(
            description='Publish-date',
            disabled=False,
            value=self.date
        )
        display(datepicker)
        self.log.append(f'Datepicker created at {datetime.datetime.now().strptime("%Y-%m-%d %H:%M")}')
        return datepicker

    def set_publish_date(self, date: datetime.datetime) -> None:
        if isinstance(date, widgets.widget_date.DatePicker):
            self.date = date.value
        elif isinstance(date, str):
            self.date = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M")
        else:
            self.date = date
        print("Publishing date set to:", self.date)
        self.log.append(f'Date set to {self.date} at {datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}')
        return self.date

    # Descriptions
    def get_description(self, tableid: str = "00000") -> StatbankUttrekksBeskrivelse:
        self._validate_params_action(tableids=[tableid])
        self.log.append(f'Getting description for tableid {tableid} at {datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}')
        return StatbankUttrekksBeskrivelse(tabellid=tableid,
                                          loaduser=self.loaduser,
                                          headers=self.__headers)
    
    
    def get_description_batch(self, tableids: list) -> dict:
        self._validate_params_action(tableids=tableids)
        descriptions = {}
        for tableid in tableids:
            descriptions[tableid] = StatbankUttrekksBeskrivelse(tabellid=tableid,
                                        loaduser=self.loaduser,
                                        headers=self.__headers)
            self.log.append(f'Got description for tableid {tableid} at {datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}')
        return descriptions
    
    # Validation
    def validate(self, 
                 dfs: pd.DataFrame, 
                 tableid: str = "00000") -> dict:
        self._validate_params_action([tableid])
        validator = StatbankUttrekksBeskrivelse(tabellid=tableid,
                                        loaduser=self.loaduser,
                                        headers=self.__headers)
        validator.validate_dfs(dfs)
        self.log.append(f'Validated data for tableid {tableid} at {datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}')

    def validate_batch(self, data: dict) -> dict:
        self._validate_params_action(list(data.keys()))
        validators = {}
        for tableid, dfs in data.items():
            validator = StatbankUttrekksBeskrivelse(tabellid=tableid,
                                        loaduser=self.loaduser,
                                        headers=self.__headers)
            validator.validate_dfs(dfs)
            self.log.append(f'Validated data for tableid {tableid} at {datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}')

    # Transfers
    def transfer(self,
                 dfs: pd.DataFrame, 
                 tableid: str = "00000") -> StatbankTransfer:
        self._validate_params_action([tableid])
        self.log.append(f'Transferring tableid {tableid} at {datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}')
        return StatbankTransfer(dfs,
                                tabellid=tableid,
                                loaduser=self.loaduser,
                                headers=self.__headers)

    def transfer_batch(self, data: dict) -> dict:
        self._validate_params_action(list(data.keys()))
        transfers = {}
        for tableid, dfs in data.items():
            transfers[tableid] = StatbankTransfer(dfs,
                                                  tabellid=tableid,
                                                  loaduser=self.loaduser,
                                                  headers=self.__headers)
            self.log.append(f'Transferred tableid {tableid} at {datetime.datetime.now().strftime("%Y-%m-%d %H:%M")}')
        return transfers
    
    # Class meta-validation
    def _validate_params_action(self, tableids: list) -> None:
        for tableid in tableids:
            if not isinstance(tableid, str):
                raise TypeError(f"{tableid} is not a string.")
            if not len(tableid) == 5:
                raise ValueError(f"{tableid} is not 5 characters long.")

    def _validate_params_init(self) -> None:
        if not self.loaduser or not isinstance(self.loaduser, str):
            raise TypeError('Please pass in "loaduser" as a string.')
        if not self.shortuser:
            self.shortuser = os.environ['JUPYTERHUB_USER'].split("@")[0]
        if not self.cc:
            self.cc = self.shortuser
        if not self.bcc:
            self.bcc = self.cc
        if self.overwrite not in ['0', '1']:
            raise ValueError("(String) Set overwrite to either '0' = no overwrite (dublicates give errors), or  '1' = automatic overwrite")
        if self.approve not in ['0', '1', '2']:
            raise ValueError("(String) Set approve to either '0' = manual, '1' = automatic (immediatly), or '2' = JIT-automatic (just-in-time)")