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

    def __str__(self):
        pass

    def __repr__(self):
        pass

    def date_picker(self) -> None:        
        datepicker =  widgets.DatePicker(
            description='Publish-date',
            disabled=False,
            value=self.date
        )
        display(datepicker)
        return datepicker

    def set_publish_date(self, date: datetime.datetime) -> None:
        if isinstance(date, widgets.widget_date.DatePicker):
            self.date = date.value
        elif isinstance(date, str):
            self.date = datetime.datetime.strptime(date, "%Y-%m-%d")
        else:
            self.date = date
        print("Publishing date set to:", self.date)
        return self.date

    def get_description(self, tableid: str = "00000") -> StatbankUttrekksBeskrivelse:
        self._validate_params_action(tableids=[tableid])
        return StatbankUttrekksBeskrivelse(tabellid=tableid,
                                          lastebruker=self.loaduser)
    
    def get_description_batch(self, tableids: list) -> dict:
        self._validate_params_action(tableids=tableids)
        headers = self._build_headers()
        try:
            descriptions = {}
            for tableid in tableids:
                descriptions[tableid] = StatbankUttrekksBeskrivelse(tabellid=tableid,
                                            lastebruker=self.loaduser,
                                            headers=headers)
        finally:
            del headers
        return descriptions
    

    def validate(self, 
                 dfs: pd.DataFrame, 
                 table_id: str = "00000") -> dict:
        self._validate_params_action()

    def validate_batch(self, data: dict) -> dict:
        self._validate_params_action()

    def transfer(self,
                 dfs: pd.DataFrame, 
                 table_id: str = "00000") -> StatbankTransfer:
        self._validate_params_action()

    def transfer_batch(self) -> StatbankBatchTransfer:
        self._validate_params_action()
        
    def _validate_params_action(self, tableids: list) -> None:
        pass

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
            