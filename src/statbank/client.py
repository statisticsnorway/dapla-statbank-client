from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

import datetime as dt
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING

import ipywidgets as widgets
from IPython.display import display

if TYPE_CHECKING:
    from statbank.api_types import QueryWholeType
from statbank.apidata import apidata
from statbank.apidata import apidata_all
from statbank.apidata import apidata_rotate
from statbank.auth import StatbankAuth
from statbank.globals import APPROVE_DEFAULT_JIT
from statbank.globals import OSLO_TIMEZONE
from statbank.globals import STATBANK_TABLE_ID_LEN
from statbank.globals import TOMORROW
from statbank.globals import Approve
from statbank.globals import _approve_type_check
from statbank.statbank_logger import logger
from statbank.transfer import StatbankTransfer
from statbank.uttrekk import StatbankUttrekksBeskrivelse


class StatbankClient(StatbankAuth):
    """This is the main interface towards the rest of the statbank-package.

    An initialized client, an object of this class, will contain data/parameters
    that often is shared, among all transfers within a statistical production.
    Call methods under this client to:
    - transfer the data: .transfer()
    - only validate the data against a description: .validate()
    - get transfer/data description (filbeskrivelse): .get_description()
    - set the publish date with a datepicker: .date_picker() + .set_publish_date()
    - get published data from the external or internal API of statbanken: apidata_all() / apidata()

    Attributes:
        loaduser (str): Username for Statbanken, not the same as "tbf"
            or "common personal username" in other SSB-systems
        date (str): Date for publishing the transfer. Shape should be "yyyy-mm-dd",
            like "2022-01-01".
            Statbanken only allows publishing four months into the future?
        shortuser (str): The abbrivation of username at ssb. Three letters, like "cfc".
            If not specified,
            we will try to get this from daplas environement variables.
        cc (str): First person to be notified by email of transfer.
            Defaults to the same as "shortuser"
        bcc (str): Second person to be notified by email of transfer.
            Defaults to the same as "cc"
        overwrite (bool): False = no overwrite
            True = overwrite
        approve (Approve | str | int):
            0 = MANUAL approval
            1 = AUTOMATIC approval at transfer-time (immediately)
            2 = JIT (Just In Time), approval right before publishing time
        log (list[str]): Each "action" (method used) on the client is appended to the log.
            Nice to use for appending to your own logging after you are done,
            or printing it in a try-except-block to see what the last actions were,
            before error being raised.
    """

    def __init__(  # noqa: PLR0913
        self,
        loaduser: str = "",
        date: str | dt.datetime = TOMORROW,
        shortuser: str = "",
        cc: str = "",
        bcc: str = "",
        overwrite: bool = True,
        approve: (
            int | str | Approve
        ) = APPROVE_DEFAULT_JIT,  # Changing back to 2, after wish from Rakel Gading
        check_username_password: bool = True,
    ) -> None:
        """Initialize the client, storing password etc. on the client."""
        self.loaduser = loaduser
        self.shortuser = shortuser
        self.cc = cc
        self.bcc = bcc
        self.overwrite = overwrite
        self.approve = _approve_type_check(approve)
        self.check_username_password = check_username_password
        self._validate_params_init()
        self.__headers = self._build_headers()
        self.log: list[str] = []
        if isinstance(date, str):
            self.date: dt.datetime = dt.datetime.strptime(date, "%Y-%m-%d").astimezone(
                OSLO_TIMEZONE,
            ) + dt.timedelta(
                hours=1,
            )  # Compensate for setting the timezone, stop publishing date from moving
        else:
            self.date = date
        self._validate_date()
        self.date = self.date.replace(hour=8, minute=0, second=0, microsecond=0)
        if self.check_username_password:
            self.get_description(
                "05300",
            )  # Random tableid to double check username&password early
        logger.info("Publishing date set to %s", self.date.isoformat("T", "seconds"))

    # Representation
    def __str__(self) -> str:
        """Print a human readable text of the clients attributes."""
        return f"""StatbankClient for user {self.loaduser}
        Publishing at {self.date}
        Shortuser {self.shortuser}
        Sending mail to {self.cc}
        And sending mail to {self.bcc}
        Overwrite set to {self.overwrite}
        Approve set to {self.approve}

        Log:
        """ + "\n\t".join(
            self.log,
        )

    def __repr__(self) -> str:
        """Represent the class with the necessary argument to replicate."""
        result = f'StatbankClient(loaduser = "{self.loaduser}"'
        if self.date != TOMORROW:
            result += f', date = "{self.date.isoformat("T", "seconds")}")'
        if self.shortuser:
            result += f', shortuser = "{self.shortuser}")'
        if self.cc:
            result += f', cc = "{self.cc}")'
        if self.bcc:
            result += f', bcc = "{self.bcc}")'
        if not self.overwrite:
            result += f", overwrite = {self.overwrite})"
        if self.approve != APPROVE_DEFAULT_JIT:
            result += f", approve = {self.approve})"
        if self.check_username_password:
            result += f", check_username_password = {self.check_username_password})"
        result += ")"
        return result

    # Publishing date handeling
    def date_picker(self) -> widgets.DatePicker:
        """Display a datapicker-widget.

        Assign it to a variable, that you after editing the date,
        pass into set_publish_date()
        date = client.datepicker()
        # Edit date
        client.set_publish_date(date)

        Returns:
            widgets.DatePicker: A datepicker widget from ipywidgets, with its date set to what the client currently holds.
        """
        datepicker = widgets.DatePicker(
            description="Publish-date",
            disabled=False,
            value=self.date,
        )
        display(datepicker)  # type: ignore[no-untyped-call]
        return datepicker

    def set_publish_date(self, date: dt.datetime | str | widgets.DatePicker) -> None:
        """Set the publishing date on the client.

        Takes the widget from date_picker assigned to a variable, which is probably the intended use.
        If sending a string, use the format 2000-12-31, you can also send in a datetime.
        Hours, minutes and seconds are replaced with statbankens publish time: 08:00:00

        Args:
            date (datetime): date-picker widget, or a date-string formatted as 2000-12-31

        Raises:
            TypeError: If the date-parameter is of type other than datetime, string, or ipywidgets.DatePicker.
        """
        if isinstance(date, widgets.DatePicker):
            date_date: dt.datetime = dt.datetime.combine(
                date.value.today(),
                dt.datetime.min.time(),
            )
        elif isinstance(date, str):
            date_date = dt.datetime.strptime(date, "%Y-%m-%d").astimezone(
                OSLO_TIMEZONE,
            ) + dt.timedelta(hours=1)
        elif isinstance(date, dt.datetime):
            date_date = date
        else:
            error_msg = f"date-parameter is of type {type(date)} must be a string, datetime, or ipywidgets.DatePicker"
            raise TypeError(error_msg)

        self.date = date_date
        self.date = self.date.replace(hour=8, minute=0, second=0, microsecond=0)
        self._validate_date()
        logger.info("Publishing date set to: %s", self.date)
        self.log.append(
            f"Date set to {self.date.isoformat('T', 'seconds')} at {(dt.datetime.now().astimezone(OSLO_TIMEZONE) + dt.timedelta(hours=1)).isoformat('T', 'seconds')}",
        )

    # Descriptions
    def get_description(
        self,
        tableid: str = "00000",
    ) -> StatbankUttrekksBeskrivelse:
        """Get the "uttrekksbeskrivelse" for the tableid, which describes metadata.

        about shape of data to be transferred, and metadata about the table
        itself in Statbankens system, like ID, name and content of codelists.

        Args:
            tableid (str): The tableid of the "hovedtabell" in statbanken, a 5 digit string.

        Returns:
            StatbankUttrekksBeskrivelse: An instance of the class StatbankUttrekksBeskrivelse, which is comparable to the old "filbeskrivelse".
        """
        self._validate_params_action(tableid)
        self.log.append(
            f"Getting description for tableid {tableid} at {(dt.datetime.now().astimezone(OSLO_TIMEZONE,) + dt.timedelta(hours=1)).isoformat('T', 'seconds')}",
        )
        return StatbankUttrekksBeskrivelse(
            tableid=tableid,
            loaduser=self.loaduser,
            headers=self.__headers,
        )

    @staticmethod
    def read_description_json(json_path_or_str: str) -> StatbankUttrekksBeskrivelse:
        """Re-initializes a StatbankUttrekksBeskrivelse from a stored json file/string.

        Checks if provided string exists on disk, if it does, tries to load it as json.
        Otherwise expects you to provide a json-string that works for json.loads.
        Inserts first layer in json as attributes under a blank StatbankUttrekksBeskrivelse-object.

        Args:
            json_path_or_str (str): Either a path on local storage, or a loaded json-string

        Returns:
            StatbankUttrekksBeskrivelse: An instance of the class StatbankUttrekksBeskrivelse, which is comparable to the old "filbeskrivelse".
        """
        content = json_path_or_str
        try:
            try_path = json_path_or_str
            if Path(try_path).exists():
                with Path(try_path).open("r") as json_file:
                    content = json_file.read()
        except OSError as e:
            logger.debug(
                "Assuming you sent a json-string to open as description, cause that path does not exist. %s",
                str(e),
            )
        new = StatbankUttrekksBeskrivelse.__new__(StatbankUttrekksBeskrivelse)
        for k, v in json.loads(content).items():
            setattr(new, k, v)
        return new

    # Validation
    def validate(
        self,
        dfs: dict[str, pd.DataFrame],
        tableid: str = "00000",
        raise_errors: bool = False,
    ) -> dict[str, ValueError]:
        """Gets an "uttrekksbeskrivelse" and validates the data against this.

        All validation happens locally, so dont be afraid of any data
        being sent to statbanken using this method.

        Args:
            dfs (dict[str, pd.DataFrame): The data to validate in a dictionary of deltabell-names as keys and pandas-dataframes as values.
            tableid (str): The tableid of the "hovedtabell" in statbanken, a 5 digit string. Defaults to "00000".
            raise_errors (bool): True/False based on if you want the method to raise its own errors or not. Defaults to False.

        Returns:
            dict[str, str]: A dictionary of the errors the validation wants to raise.
        """
        self._validate_params_action(tableid)
        validator = StatbankUttrekksBeskrivelse(
            tableid=tableid,
            loaduser=self.loaduser,
            raise_errors=raise_errors,
            headers=self.__headers,
        )
        validation_errors = validator.validate(dfs)
        self.log.append(
            f"Validated data for tableid {tableid} at {(dt.datetime.now().astimezone(OSLO_TIMEZONE) + dt.timedelta(hours=1)).isoformat('T', 'seconds')}",
        )
        return validation_errors

    def transfer(
        self,
        dfs: dict[str, pd.DataFrame],
        tableid: str = "00000",
    ) -> StatbankTransfer:
        """Transfers your data to Statbanken.

        Make sure you've set the publish-date correctly before sending.

        Args:
            dfs (dict[str, pd.DataFrame]): The data to validate in a dictionary of deltabell-names as keys and pandas-dataframes as values.
            tableid (str): The tableid of the "hovedtabell" in statbanken, a 5 digit string.

        Returns:
            StatbankTransfer: An instance of the class StatbankTransfer, which details the content of a successful transfer.
        """
        self._validate_params_action(tableid)
        self.log.append(
            f"Transferring tableid {tableid} at {(dt.datetime.now().astimezone(OSLO_TIMEZONE) + dt.timedelta(hours=1)).isoformat('T', 'seconds')}",
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

        Args:
            json_path_or_str (str): Either a path on local storage, or a loaded json-string

        Returns:
            StatbankTransfer: An instance of the class StatbankTransfer, missing the data transferred and some other bits probably.
        """
        content = json_path_or_str
        try:
            try_path = json_path_or_str
            if Path(try_path).exists():
                with Path(try_path).open("r") as json_file:
                    content = json_file.read()
        except OSError as e:
            logger.debug(
                "Assuming you sent a json-string to open as transfer, cause that path does not exist. %s",
                str(e),
            )
        new = StatbankTransfer.__new__(StatbankTransfer)
        for k, v in json.loads(content).items():
            setattr(new, k, v)
        return new

    @staticmethod
    def apidata(
        id_or_url: str = "",
        payload: QueryWholeType | None = None,
        include_id: bool = False,
    ) -> pd.DataFrame:
        """Get the contents of a published statbank-table as a pandas Dataframe, specifying a query to limit the return.

        Args:
            id_or_url (str): The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
            payload (dict[str, str]|None): a dict of the query to include with the request, can be copied from the statbank-webpage.
            include_id (bool): If you want to include "codes" in the dataframe, set this to True

        Returns:
            pd.DataFrame: A pandas dataframe with the table-content
        """
        replace_payload: QueryWholeType = {
            "query": [],
            "response": {"format": "json-stat2"},
        }
        if payload is None:
            payload = replace_payload
        return apidata(id_or_url=id_or_url, payload=payload, include_id=include_id)

    @staticmethod
    def apidata_all(id_or_url: str = "", include_id: bool = False) -> pd.DataFrame:
        """Get ALL the contents of a published statbank-table as a pandas Dataframe.

        Args:
            id_or_url (str): The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
            include_id (bool): If you want to include "codes" in the dataframe, set this to True

        Returns:
            pd.DataFrame: A pandas dataframe with the table-content
        """
        return apidata_all(id_or_url=id_or_url, include_id=include_id)

    @staticmethod
    def apidata_rotate(
        df: pd.DataFrame,
        ind: str = "year",
        val: str = "value",
    ) -> pd.DataFrame:
        """Rotate the dataframe so that time is used as the index.

        Args:
            df (pd.dataframe): dataframe (from <get_from_ssb> function
            ind (str): string of column name denoting time
            val (str): string of column name denoting values

        Returns:
            pd.DataFrame: pivoted dataframe
        """
        return apidata_rotate(df, ind, val)

    def _validate_date(self) -> None:
        """Validate dates provided to the client."""
        if not (isinstance(self.date, (dt.date, dt.datetime))):
            error_msg = "Date must be a datetime.datetime or datetime.date"  # type: ignore[unreachable]
            raise TypeError(error_msg)
        # Date should not be on a weekend
        if self.date.weekday() in [5, 6]:
            logger.warning(
                "Warning, you are publishing during a weekend, this is not common practice.",
            )

    # Class meta-validation
    def _validate_params_action(self, tableid: str) -> None:
        """Validates tableid mainly, more actively than other params."""
        if not isinstance(tableid, str):
            error_msg = f"{tableid} is not a string."  # type: ignore[unreachable]
            raise TypeError(error_msg)
        if (
            tableid.isdigit() and len(tableid) != STATBANK_TABLE_ID_LEN
        ):  # Allow for "hovednavn" in addition to tableid
            error_msg = f"{tableid} is numeric, but not 5 characters long."
            raise ValueError(error_msg)

    def _validate_params_init(self) -> None:
        """Validates many of the parameters sent in on client-initialization."""
        if not self.loaduser or not isinstance(self.loaduser, str):
            error_msg = "Please pass in a string for loaduser."
            raise TypeError(error_msg)
        if not self.shortuser:
            self.shortuser = self._get_user_tbf()
        if not self.cc:
            self.cc = self.shortuser
        if not self.bcc:
            self.bcc = self.cc
        if not isinstance(self.overwrite, bool):
            error_msg = "(Bool) Set overwrite to either False = no overwrite (dublicates give errors), or  True = automatic overwrite"  # type: ignore[unreachable]
            raise TypeError(error_msg)
        if not isinstance(self.approve, int) or self.approve not in iter(Approve):
            error_msg = "(Approve) Set approve to either 0 = manual, 1 = automatic (immediatly), or 2 = JIT-automatic (just-in-time)"
            raise ValueError(error_msg)

    @staticmethod
    def _get_user_tbf() -> str:
        user_mail = os.environ.get("GIT_USER_MAIL", "")
        if not user_mail:
            user_mail = os.environ.get("JUPYTERHUB_USER", "")
        if "@" in user_mail:
            user_mail = user_mail.split("@")[0]
        return user_mail
