#!/usr/bin/env python3

import datetime
import json
import os
from datetime import timedelta as td

import ipywidgets as widgets
import pandas as pd
from IPython.display import display

from statbank.apidata import apidata, apidata_all, apidata_rotate
from statbank.auth import StatbankAuth
from statbank.transfer import StatbankTransfer
from statbank.uttrekk import StatbankUttrekksBeskrivelse


TOMORROW = datetime.datetime.now() + td(days=1)


class StatbankClient(StatbankAuth):
    """
    This is the main interface towards the rest of the statbank-package.
    An initialized client, an object of this class, will contain data/parameters
    that often is shared, among all transfers within a statistical production.
    Call methods under this client to:
    - transfer the data
        .transfer()
    - only validate the data against a description
        .validate()
    - get transfer/data description (filbeskrivelse)
        .get_description()
    - set the publish date with a datepicker
        .date_picker() + .set_publish_date()
    - get published data from the external or internal API of statbanken
        apidata_all() / apidata()
    ...

    Attributes
    ----------
    loaduser : str
        Username for Statbanken, not the same as "tbf"
        or "common personal username" in other SSB-systems
    date : str
        Date for publishing the transfer. Shape should be "yyyy-mm-dd",
        like "2022-01-01".
        Statbanken only allows publishing four months into the future?
    shortuser : str
        The abbrivation of username at ssb. Three letters, like "cfc".
        If not specified,
        we will try to get this from daplas environement variables.
    cc : str
        First person to be notified by email of transfer.
        Defaults to the same as "shortuser"
    bcc : str
        Second person to be notified by email of transfer.
        Defaults to the same as "cc"
    overwrite : bool
        False = no overwrite
        True = overwrite
    approve : int
        0 = manual approval
        1 = automatic approval at transfer-time (immediately)
        2 = JIT (Just In Time), approval right before publishing time
    log: list
        Each "action" (method used) on the client is appended to the log.
        Nice to use for appending to your own logging after you are done,
        or printing it in a try-except-block to see what the last actions were,
        before error being raised.

    Methods
    -------
    get_description(tableid):
        Get the "uttrekksbeskrivelse" for the tableid, which describes metadata
        about shape of data to be transferred, and metadata about the table
        itself in Statbankens system, like ID, name and content of codelists.
        Returns an object of the internal class "StatbankUttrekksBeskrivelse"
    validate(data, tableid):
        Gets an "uttrekksbeskrivelse" and validates the data against this.
        All validation happens locally, so dont be afraid of any data
        being sent to statbanken using this method.
        Logic is built in Python, and can probably be expanded upon.
    transfer(data, tableid):
        Transfers your data to Statbanken.
        First it gets an uttrekksbeskrivelse, validates against this,
        then makes the actual transfer. Validation can be set to False,
        to avoid this checking beforehand.

        Make sure you've set the publish-date correctly before sending.

    date = date_picker():
        Shows a date-picker widget using ipywidget.
    set_publish_date(date):
        To actually set the date,
        the result of the picker must be sent into this function after editing.
        This method also excepts a datetime, datetime.date,
        or a string in the format YYYY-mm-dd.

    apidata_all(tableid):
        Finds "all the codes" of data for the table, using a first request.
        Then builds a query from this to get all the data using apidata().
        Use this if you want "all the data" from a table, and this isnt too big.
    apidata(tableid, query):
        Lets you specify a query, to limit the data in the response.
        Get this query from the bottom of the statbank-webpage (API-spørring).

    read_description_json(path.json):
        Tries to restore a StatbankUttrekksBeskrivelse-object from a stored, serialized json.
    read_transfer_json(path.json):
        Tries to restore a StatbankTransfer-object from a stored, serialized json.

    __init__():
        Sets attributes, validates them, builds header, initializes log.
    """

    def __init__(
        self,
        loaduser: str = "",
        date: datetime.datetime = TOMORROW,
        shortuser: str = "",
        cc: str = "",
        bcc: str = "",
        overwrite: bool = True,
        approve: int = 2,  # Changing back to 2, after wish from Rakel Gading
        check_username_password: bool = True,
    ):
        self.loaduser = loaduser
        self.shortuser = shortuser
        self.cc = cc
        self.bcc = bcc
        self.overwrite = overwrite
        self.approve = approve
        self._validate_params_init()
        self.__headers = self._build_headers()
        self.log = []
        if isinstance(date, str):
            self.date = datetime.datetime.strptime(date, "%Y-%m-%d")
        else:
            self.date = date
        self.date = self.date.replace(hour=8, minute=0, second=0, microsecond=0)
        self._validate_date()
        if check_username_password:
            self.get_description(
                "05300", printing=False
            )  # Random tableid to double check username&password early
        print(f"Publishing date set to {self.date.isoformat('T', 'seconds')}")

    # Representation
    def __str__(self):
        return f"""StatbankClient for user {self.loaduser}
        Publishing at {self.date}
        Shortuser {self.shortuser}
        Sending mail to {self.cc}
        And sending mail to {self.bcc}
        Overwrite set to {self.overwrite}
        Approve set to {self.approve}

        Log:
        """ + "\n\t".join(
            self.log
        )

    def __repr__(self):
        return f'StatbankClient(loaduser = "{self.loaduser}")'

    # Publishing date handeling
    def date_picker(self) -> None:
        """Displays a datapicker-widget.
        Assign it to a variable, that you after editing the date,
        pass into set_publish_date()
        date = client.datepicker()
        # Edit date
        client.set_publish_date(date)

        Returns
        -------
        A datepicker widget from ipywidgets, with its date set to what the client currently holds.
        """
        datepicker = widgets.DatePicker(
            description="Publish-date", disabled=False, value=self.date
        )
        display(datepicker)
        return datepicker

    def set_publish_date(self, date: datetime.datetime) -> None:
        """Set the publishing date on the client.
        Takes the widget from date_picker assigned to a variable, which is probably the intended use.
        If sending a string, use the format 2000-12-31, you can also send in a datetime.
        Hours, minutes and seconds are replaced with statbankens publish time: 08:00:00

        Parameters
        -------
        date: datetime, date-picker widget, or a date-string formatted as 2000-12-31
        """
        if isinstance(date, widgets.widget_date.DatePicker):
            self.date = date.value
        elif isinstance(date, str):
            self.date = datetime.datetime.strptime(date, "%Y-%m-%d")
        else:
            self.date = date
        self.date = self.date.replace(hour=8, minute=0, second=0, microsecond=0)
        self._validate_date()
        print("Publishing date set to:", self.date)
        self.log.append(
            f"Date set to {self.date.isoformat('T', 'seconds')} at {datetime.datetime.now().isoformat('T', 'seconds')}"
        )

    # Descriptions
    def get_description(
        self, tableid: str = "00000", printing=True
    ) -> StatbankUttrekksBeskrivelse:
        """Get the "uttrekksbeskrivelse" for the tableid, which describes metadata
        about shape of data to be transferred, and metadata about the table
        itself in Statbankens system, like ID, name and content of codelists.

        Parameters
        -------
        tableid: str, The tableid of the "hovedtabell" in statbanken, a 5 digit string.

        Returns
        -------
        An instance of the class StatbankUttrekksBeskrivelse, which is comparable to the old "filbeskrivelse".
        """
        self._validate_params_action(tableid)
        self.log.append(
            f"Getting description for tableid {tableid} at {datetime.datetime.now().isoformat('T', 'seconds')}"
        )
        return StatbankUttrekksBeskrivelse(
            tableid=tableid,
            loaduser=self.loaduser,
            headers=self.__headers,
            printing=printing,
        )

    @staticmethod
    def read_description_json(json_path_or_str: str) -> StatbankUttrekksBeskrivelse:
        """Re-initializes a StatbankUttrekksBeskrivelse from a stored json file/string.
        Checks if provided string exists on disk, if it does, tries to load it as json.
        Otherwise expects you to provide a json-string that works for json.loads.
        Inserts first layer in json as attributes under a blank StatbankUttrekksBeskrivelse-object.

        Parameters
        -------
        json_path_or_str: str or path, Either a path on local storage, or a loaded json-string

        Returns
        -------
        An instance of the class StatbankUttrekksBeskrivelse, which is comparable to the old "filbeskrivelse".
        """
        if os.path.exists(json_path_or_str):
            with open(json_path_or_str) as json_file:
                json_path_or_str = json_file.read()
        new = StatbankUttrekksBeskrivelse.__new__(StatbankUttrekksBeskrivelse)
        for k, v in json.loads(json_path_or_str).items():
            setattr(new, k, v)
        return new

    # Validation
    def validate(
        self,
        dfs: dict,
        tableid: str = "00000",
        raise_errors: bool = False,
        printing: bool = True,
    ) -> dict:
        """Gets an "uttrekksbeskrivelse" and validates the data against this.
        All validation happens locally, so dont be afraid of any data
        being sent to statbanken using this method.

        Parameters
        -------
        dfs: dict of dataframes, The data to validate in a dictionary of deltabell-names as keys and pandas-dataframes as values.
        tableid: str, The tableid of the "hovedtabell" in statbanken, a 5 digit string.
        raise_errors: bool, True/False based on if you want the method to raise its own errors or not.
        printing: bool, True/False based on if you want a verbose printing of everything the validation checks.

        Returns
        -------
        A dictionary of the errors the validation wants to raise.
        """
        self._validate_params_action(tableid)
        validator = StatbankUttrekksBeskrivelse(
            tableid=tableid,
            loaduser=self.loaduser,
            raise_errors=raise_errors,
            headers=self.__headers,
        )
        validation_errors = validator.validate(dfs, printing=printing)
        self.log.append(
            f"Validated data for tableid {tableid} at {datetime.datetime.now().isoformat('T', 'seconds')}"
        )
        return validation_errors

    # Transfer
    def transfer(self, dfs: dict, tableid: str = "00000") -> StatbankTransfer:
        """Transfers your data to Statbanken.
        Make sure you've set the publish-date correctly before sending.

        Parameters
        -------
        dfs: dict of dataframes, The data to validate in a dictionary of deltabell-names as keys and pandas-dataframes as values.
        tableid: str, The tableid of the "hovedtabell" in statbanken, a 5 digit string.

        Returns
        -------
        An instance of the class StatbankTransfer, which details the content of a successful transfer.
        """
        self._validate_params_action(tableid)
        self.log.append(
            f"Transferring tableid {tableid} at {datetime.datetime.now().isoformat('T', 'seconds')}"
        )
        return StatbankTransfer(
            dfs,
            tableid=tableid,
            loaduser=self.loaduser,
            headers=self.__headers,
            shortuser=self.shortuser,
            date=self.date,
            cc=self.cc,
            bcc=self.bcc,
            overwrite=self.overwrite,
            approve=self.approve,
        )

    @staticmethod
    def read_transfer_json(json_path_or_str: str) -> StatbankTransfer:
        """Checks if provided string exists on disk, if it does, tries to load it as json.
        Otherwise expects you to provide a json-string that works for json.loads.
        Inserts first layer in json as attributes under a blank StatbankTransfer-object.

        Parameters
        -------
        json_path_or_str: str or path, Either a path on local storage, or a loaded json-string

        Returns
        -------
        An instance of the class StatbankTransfer, missing the data transferred and some other bits probably.
        """
        if os.path.exists(json_path_or_str):
            with open(json_path_or_str) as json_file:
                json_path_or_str = json_file.read()
        new = StatbankTransfer.__new__(StatbankTransfer)
        for k, v in json.loads(json_path_or_str).items():
            setattr(new, k, v)
        return new

    # Get apidata
    @staticmethod
    def apidata(
        id_or_url: str = "",
        payload: dict = None,
        include_id: bool = False,
    ) -> pd.DataFrame:
        """
        Get the contents of a published statbank-table as a pandas Dataframe, specifying a query to limit the return.

        Parameters
        -------
        id_or_url: str, The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
        payload: dict, a dict of the query to include with the request, can be copied from the statbank-webpage.
        include_id: bool, If you want to include "codes" in the dataframe, set this to True

        Returns
        -------
        A pandas dataframe with the table-content
        """
        if not payload:
            payload = {"query": [], "response": {"format": "json-stat2"}}
        return apidata(id_or_url=id_or_url, payload=payload, include_id=include_id)

    @staticmethod
    def apidata_all(id_or_url: str = "", include_id: bool = False) -> pd.DataFrame:
        """
        Get ALL the contents of a published statbank-table as a pandas Dataframe.

        Parameters
        -------
        id_or_url: str, The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
        include_id: bool, If you want to include "codes" in the dataframe, set this to True

        Returns
        -------
        A pandas dataframe with the table-content
        """
        return apidata_all(id_or_url=id_or_url, include_id=include_id)

    @staticmethod
    def apidata_rotate(df, ind="year", val="value"):
        """Rotate the dataframe so that time is used as the index

        Parameters
        -------
        df: pandas.dataframe, dataframe (from <get_from_ssb> function
        ind: str, string of column name denoting time
        val: str: string of column name denoting values

        Returns
        -------
        dataframe: pivoted dataframe
        """
        return apidata_rotate(df, ind, val)

    def _validate_date(self) -> None:
        """Validates dates provided to the client"""
        if not (
            isinstance(self.date, datetime.datetime)
            or isinstance(self.date, datetime.date)
        ):
            raise TypeError("Date must be a datetime.datetime or datetime.date")
        # Date should not be on a weekend
        if self.date.weekday() in [5, 6]:
            print(
                "Warning, you are publishing during a weekend, this is not common practice."
            )

    # Class meta-validation
    def _validate_params_action(self, tableid: str) -> None:
        """Validates tableid mainly, more actively than other params"""
        if not isinstance(tableid, str):
            raise TypeError(f"{tableid} is not a string.")
        if (
            tableid.isdigit() and not len(tableid) == 5
        ):  # Allow for "hovednavn" in addition to tableid
            raise ValueError(f"{tableid} is numeric, but not 5 characters long.")

    def _validate_params_init(self) -> None:
        """Validates many of the parameters sent in on client-initialization."""
        if not self.loaduser or not isinstance(self.loaduser, str):
            raise TypeError('Please pass in "loaduser" as a string.')
        if not self.shortuser:
            self.shortuser = os.environ["JUPYTERHUB_USER"].split("@")[0]
        if not self.cc:
            self.cc = self.shortuser
        if not self.bcc:
            self.bcc = self.cc
        if not isinstance(self.overwrite, bool):
            raise ValueError(
                "(Bool) Set overwrite to either False = no overwrite (dublicates give errors), or  True = automatic overwrite"
            )
        if not isinstance(self.approve, int) or self.approve not in [0, 1, 2]:
            raise ValueError(
                "(Int) Set approve to either 0 = manual, 1 = automatic (immediatly), or 2 = JIT-automatic (just-in-time)"
            )
