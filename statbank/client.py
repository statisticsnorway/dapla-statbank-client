#!/usr/bin/env python3

from .uttrekk import StatbankUttrekksBeskrivelse
from .transfer import StatbankTransfer
from .batchtransfer import StatbankBatchTransfer

from datetime import datetime as dt
import pandas as pd

class StatbankClient:
    def __init__(self,
            loaduser = "",
            date: dt = dt.now() + td(days=1),
            shortuser: str = "",
            cc: str = "",
            bcc: str = "",
            overwrite: str = '1',
            approve: str = '2',
            validation: bool = True,
            ):
        self.user = user
        self.date = date
        self.shortuser = shortuser
        self.cc = cc
        self.bcc = bcc
        self.overwrite = overwrite
        self.approve = approve
        self.validation = validation
        self._validate_params()

    def __str__(self):
        pass

    def __repr__(self):
        pass

    def get_uttrekksbeskrivelse() -> StatbankUttrekksBeskrivelse:
        pass

    def validate(dfs: pd.DataFrame, table_id: str = "00000") -> dict:
        pass

    def validate_batch(data: dict) -> dict:
        pass

    def transfer(dfs: pd.DataFrame, table_id: str = "00000") -> StatbankTransfer:
        pass

    def transfer_batch():
        pass

    def _validate_params(self):
        pass