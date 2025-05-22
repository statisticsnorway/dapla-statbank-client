from __future__ import annotations

from typing import TYPE_CHECKING
from typing import Any
from typing import Literal

if TYPE_CHECKING:
    from collections.abc import Callable

    import pandas as pd

import datetime
import getpass
import json
import os
import shutil
import subprocess
from functools import partial
from pathlib import Path
from typing import cast

import ipywidgets as widgets
from IPython.display import display

if TYPE_CHECKING:
    from statbank.api_types import QueryWholeType
from statbank.auth import StatbankAuth
from statbank.auth import UseDb
from statbank.get_apidata import apicodelist
from statbank.get_apidata import apidata
from statbank.get_apidata import apidata_all
from statbank.get_apidata import apidata_rotate
from statbank.get_apidata import apimetadata
from statbank.globals import APPROVE_DEFAULT_JIT
from statbank.globals import OSLO_TIMEZONE
from statbank.globals import SSB_TBF_LEN
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
        date (dt.datetime): Date for publishing the transfer.
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
        use_db (UseDb | str | None):
            If you are in PROD-dapla and want to send to statbank test-database, set this to "TEST".
            When sending from TEST-environments you can only send to TEST-db, so this parameter is then ignored.
            Be aware that metadata tends to be outdated in the test-database.
    """

    def __init__(
        self,
        date: str | datetime.date | datetime.datetime = TOMORROW,
        shortuser: str = "",
        cc: str = "",
        bcc: str = "",
        overwrite: bool = True,
        approve: (
            int | str | Approve
        ) = APPROVE_DEFAULT_JIT,  # Changing back to 2, after wish from Rakel Gading
        check_username_password: bool = True,
        use_db: Literal["TEST", "PROD"] | None = None,
    ) -> None:
        """Initialize the client, storing password etc. on the client."""
        self.shortuser = shortuser
        self.cc = cc
        self.bcc = bcc
        self.overwrite = overwrite
        self.approve = _approve_type_check(approve)
        self.check_username_password = check_username_password
        StatbankAuth.__init__(self, use_db)
        self._validate_params_init()
        self.__headers = self._build_headers()
        self.log: list[str] = []
        self.date: datetime.date
        if isinstance(date, str):
            try:
                self.date = (
                    datetime.datetime.strptime(
                        date,
                        "%Y-%m-%d",
                    )
                    .astimezone(OSLO_TIMEZONE)
                    .date()
                )
            except ValueError as e:
                error_msg = f"Loaduser parameter removed, please do not use it in your code. OR: {e}"
                raise ValueError(error_msg) from e
        elif isinstance(date, datetime.datetime):
            self.date = self.date = date.date()
        else:
            self.date = date

        self._validate_date()
        if self.check_username_password:
            logger.info(
                "Checking filbeskrivelse of random tableid 05300 to double-check username & password early.",
            )
            self.get_description(
                "05300",
            )
        logger.info("Publishing date set to %s", self.date.isoformat())

    # Representation
    def __str__(self) -> str:
        """Print a human readable text of the clients attributes."""
        return f"""StatbankClient
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
        result = "StatbankClient("
        if self.date != TOMORROW:
            result += f'date = "{self.date.isoformat()}", '
        if self.shortuser:
            result += f'shortuser = "{self.shortuser}", '
        if self.cc:
            result += f'cc = "{self.cc}", '
        if self.bcc:
            result += f', bcc = "{self.bcc}", '
        if not self.overwrite:
            result += f"overwrite = {self.overwrite}), "
        if self.approve != APPROVE_DEFAULT_JIT:
            result += f"approve = {self.approve}, "
        if self.check_username_password:
            result += f"check_username_password = {self.check_username_password}"
        result = result.strip(" ").strip(",")
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

    def set_publish_date(
        self,
        date: datetime.date | datetime.datetime | str | widgets.DatePicker,
    ) -> None:
        """Set the publishing date on the client.

        Takes the widget from date_picker assigned to a variable, which is probably the intended use.
        If sending a string, use the format 2000-12-31, you can also send in a datetime.
        Hours, minutes and seconds are replaced with statbankens publish time: 08:00:00

        Args:
            date (datetime): date-picker widget, or a date-string formatted as 2000-12-31

        Raises:
            TypeError: If the date-parameter is of type other than datetime, string, or ipywidgets.DatePicker.
        """
        match date:
            case widgets.DatePicker():
                self.date = cast("datetime.date", date.value)
            case datetime.datetime():
                self.date = date.date()
            case datetime.date():
                self.date = date
            case str():
                self.date = (
                    datetime.datetime.strptime(
                        date,
                        "%Y-%m-%d",
                    )
                    .astimezone(OSLO_TIMEZONE)
                    .date()
                )
            case _:
                error_msg = f"date-parameter is of type {type(date)} must be a string, datetime, or ipywidgets.DatePicker"
                raise TypeError(error_msg)

        self._validate_date()
        logger.info("Publishing date set to: %s", self.date)
        self.log.append(
            f"Date set to {self.date.isoformat()} at {datetime.datetime.now(tz=OSLO_TIMEZONE).isoformat('T', 'seconds')}",
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
            f"Getting description for tableid {tableid} at {datetime.datetime.now(tz=OSLO_TIMEZONE).isoformat('T', 'seconds')}",
        )
        return StatbankUttrekksBeskrivelse(
            tableid=tableid,
            headers=self.__headers,
            use_db=self.use_db,
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
        if isinstance(new.use_db, str):
            new.use_db = UseDb[new.use_db]
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
            raise_errors=raise_errors,
            headers=self.__headers,
            use_db=self.use_db,
        )
        validation_errors = validator.validate(dfs)
        self.log.append(
            f"Validated data for tableid {tableid} at {datetime.datetime.now(tz=OSLO_TIMEZONE).isoformat('T', 'seconds')}",
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
            f"Transferring tableid {tableid} at {datetime.datetime.now(tz=OSLO_TIMEZONE).isoformat('T', 'seconds')}",
        )
        return StatbankTransfer(
            dfs,
            tableid=tableid,
            headers=self.__headers,
            shortuser=self.shortuser,
            date=self.date,
            cc=self.cc,
            bcc=self.bcc,
            overwrite=self.overwrite,
            approve=self.approve,
            use_db=self.use_db,
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
    def apimetadata(id_or_url: str = "") -> dict[str, Any]:
        """Get the metadata of a published statbank-table as a dict.

        Args:
            id_or_url (str): The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".

        Returns:
            dict[str, Any]: The metadata of the table as the json returned from the API-get-request.
        """
        return apimetadata(id_or_url=id_or_url)

    @staticmethod
    def apicodelist(
        id_or_url: str = "",
        codelist_name: str = "",
    ) -> dict[str, str] | dict[str, dict[str, str]]:
        """Get one specific or all the codelists of a published statbank-table as a dict or nested dicts.

        Args:
            id_or_url (str): The id of the STATBANK-table to get the total query for, or supply the total url, if the table is "internal".
            codelist_name (str): The name of the specific codelist to get.

        Returns:
            dict[str, str] | dict[str, dict[str, str]]: The codelist of the table as a dict or a nested dict.
        """
        return apicodelist(id_or_url=id_or_url, codelist_name=codelist_name)

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
        if not (isinstance(self.date, datetime.date)):
            error_msg = "Date must be a datetime.datetime"  # type: ignore[unreachable]
            raise TypeError(error_msg)
        if self.date < datetime.datetime.now().astimezone(OSLO_TIMEZONE).date():
            logger.warning(
                "Publishing date usually should be in the future, not in the past.",
            )
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
        if not self.shortuser:
            self.shortuser = self._get_user_initials()
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
    def _get_user_initials() -> str:
        attempts: list[Callable[[], str] | partial[str | None]] = [
            partial(os.environ.get, "DAPLA_USER"),
            partial(os.environ.get, "JUPYTERHUB_USER"),
        ]

        # This is to satisfy Ruff and mypy, git might not be installed (it is), and not qualifying gits full path is a security risk (its probably not)
        git_path = shutil.which("git")
        if isinstance(git_path, str):
            attempts += [
                (
                    lambda: subprocess.check_output(  # noqa: S603
                        [git_path, "config", "user.email"],
                    )
                    .decode("utf8")
                    .strip()
                ),
            ]

        attempts += [
            getpass.getuser,
            partial(input, "Brukerinitialer (tre bokstaver): "),
        ]

        for func in attempts:
            initials_or_email: str | None = func()

            if not initials_or_email:
                continue

            initials: str = initials_or_email.partition("@")[0]
            if not (len(initials) == SSB_TBF_LEN and initials.isalpha()):
                continue
            return initials

        error_message = "Can't find the users email or initials in the system."
        raise ValueError(error_message)
