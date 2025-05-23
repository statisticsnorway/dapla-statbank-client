from __future__ import annotations

import datetime
import gc
import json
import os
import re
import urllib
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Literal
from typing import cast

import pandas as pd
import requests as r

from statbank.auth import StatbankAuth
from statbank.auth import UseDb
from statbank.globals import APPROVE_DEFAULT_JIT
from statbank.globals import OSLO_TIMEZONE
from statbank.globals import SSB_TBF_LEN
from statbank.globals import TOMORROW
from statbank.globals import Approve
from statbank.globals import _approve_type_check
from statbank.statbank_logger import logger

if TYPE_CHECKING:
    from statbank.api_types import TransferResultType


class StatbankTransfer(StatbankAuth):
    """Class for talking with the "transfer-API", which actually recieves the data from the user and sends it to Statbank.

    Attributes:
        data (dict[str, pd.DataFrame]):  name of "deltabell.dat" as keys.
            Number of DataFrames needs to match the number of "deltabeller" in
            the uttakksbeskrivelse.
            Dict-shape can be retrieved and validated before transfer with the
            Uttakksbeskrivelses-class.
        tableid (str): The numeric id of the table, matching the one found on the website.
            Should be a 5-length numeric-string. Alternatively it should be possible to send in the "hovedtabellnavn" instead of the tableid.
        shortuser (str): The abbrivation of username at ssb. Three letters, like "cfc"
        date (str): Date for publishing the transfer. Shape should be "yyyy-mm-dd", like "2022-01-01".
            Statbanken only allows publishing four months into the future?
        cc (str): First person to be notified by email of transfer. Defaults to the same as "shortuser"
        bcc (str): Second person to be notified by email of transfer. Defaults to the same as "cc"
        overwrite (bool):
            - False = no overwrite
            - True = overwrite
        approve (Approve | str | int):
            - 0 = MANUAL approval
            - 1 = AUTOMATIC approval at transfer-time (immediately)
            - 2 = JIT (Just In Time), approval right before publishing time
        use_db (UseDb | str | None):
            If you are in PROD-dapla and want to send to statbank test-database, set this to "TEST".
            When sending from TEST-environments you can only send to TEST-db, so this parameter is then ignored.
            Be aware that metadata tends to be outdated in the test-database.
        validation (bool):
            - True, if you want the python-validation code to run user-side.
            - False, if its slow and unnecessary.
        boundary (str): String that defines the splitting of the body in the transfer-post-request.
            Kept here for uniform choice through the class.
        urls (dict[str, str]): Urls for transfer, observing the result etc.,
            built from environment variables.
        headers (dict[str, str]): Might be deleted without warning.
            Temporarily holds the Authentication for the request.
        params (dict[str, str]): This dict will be built into the post request.
            Keep it in this nice shape for later introspection.
        body (str): The data parsed into the body-shape the Statbank-API expects in the transfer-post-request.
        response (requests.Response): The resulting response from the transfer-request. Headers might be deleted without warning.
    """

    def __init__(  # noqa: PLR0913
        self,
        data: dict[str, pd.DataFrame],
        tableid: str = "",
        shortuser: str = "",
        date: str | datetime.date | datetime.datetime = TOMORROW,
        cc: str = "",
        bcc: str = "",
        overwrite: bool = True,
        approve: int | str | Approve = APPROVE_DEFAULT_JIT,
        use_db: UseDb | Literal["TEST", "PROD"] | None = None,
        validation: bool = True,
        delay: bool = False,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Make the transfer to statbanken at the end of initializing the object.

        May run the validations from the StatbankValidation class before the transfer.
        """
        self._set_user_attrs(shortuser=shortuser, cc=cc, bcc=bcc)
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
                error_msg = "Skriv inn datoformen for publisering som 1900-01-01"
                raise TypeError(error_msg) from e
        elif isinstance(date, datetime.datetime):
            self.date = self.date = date.date()
        else:
            self.date = date

        self.data = data
        if not (isinstance(tableid, str) and tableid.isdigit()):
            error_msg = "Loaduser is no longer a parameter, make sure the tableid parameter is a string of digits."
            raise ValueError(error_msg)
        self.tableid = tableid
        self.overwrite = overwrite
        self.approve = _approve_type_check(approve)
        StatbankAuth.__init__(self, use_db)
        self.validation = validation
        self.__delay = delay
        self.oppdragsnummer: str = ""
        self.boundary = "12345"
        if validation:
            self._validate_original_parameters()

        self.urls = self._build_urls()
        if not self.delay:
            if headers:
                self.transfer(headers)
            else:
                self.transfer()

    def transfer(self, headers: dict[str, str] | None = None) -> None:
        """Transfers your data to Statbanken.

        Make sure you've set the publish-date correctly before sending.
        Will only work if the transfer has not already been sent, meaning it was "delayed".

        Args:
            headers (dict[str, str] | None): Mostly for internal use by the package.
                Needs to be a finished compiled headers for a request including Authorization.

        Raises:
            ValueError: If the transfer is already transferred.
        """
        # In case transfer has already happened, don't transfer again
        if self.oppdragsnummer:
            error_msg = f"Already transferred? {self.urls['gui'] + self.oppdragsnummer} Remake the StatbankTransfer-object if intentional."
            raise ValueError(error_msg)
        if headers is None:
            self.headers = self._build_headers()
        else:
            self.headers = headers
        try:
            self.params = self._build_params()
            self._validate_datatype()
            self.body = self._body_from_data()

            url_load_params = self.urls["loader"] + urllib.parse.urlencode(self.params)
            urllib.parse.urlparse(url_load_params)  # Test to see if url is valid format
            self.response = self._make_transfer_request(url_load_params)
            self._cleanup_response()
        finally:
            del self.headers  # Cleaning up auth-storing
            self.__delay = False
        self._handle_response()

    def __str__(self) -> str:
        """Print a string with the status of the transfer."""
        first_line = f"Overføring for statbanktabell {self.tableid}.\n"
        if self.delay:
            result = f"""{first_line}Ikke overført enda."""
        else:
            result = f"""{first_line}Publisering: {self.date.strftime('%d-%m-%Y,')}.\nLastelogg: {self.urls['gui'] + self.oppdragsnummer}"""
        return result

    def __repr__(self) -> str:
        """Get a representation of how to recreate the object using parameters."""
        return f'StatbankTransfer([data], tableid="{self.tableid}")'

    @property
    def delay(self) -> bool:
        """Obfuscate the delay a bit from the user. We dont want transfers transferring again without recreating the object."""
        return self.__delay

    def _set_user_attrs(
        self,
        shortuser: str = "",
        cc: str = "",
        bcc: str = "",
    ) -> None:
        if shortuser:
            self.shortuser = shortuser
        else:
            self.shortuser = os.environ["JUPYTERHUB_USER"].split("@")[0]
        if cc:
            self.cc = cc
        else:
            self.cc = self.shortuser
        if bcc:
            self.bcc = bcc
        else:
            self.bcc = self.cc

    def to_json(self, path: str = "") -> str | None:
        """Store a copy of the current state of the transfer-object as a json.

        If path is provided, tries to write to it,
        otherwise will return a json-string for you to handle like you wish.

        Args:
            path (str): if provided, will try to write a json to a local path.

        Returns:
            None: If path is provided, tries to write a json there and returns nothing.
            str: If path is not provided, returns the json-string for you to handle as you wish.
        """
        logger.warning(
            "Warning, some nested, deeper data-structures"
            " like dataframes and other class-objects will not be serialized",
        )
        json_content = json.dumps(self.__dict__, default=lambda _: "<not serializable>")
        # If path provided write to it, otherwise return the string-content
        if path:
            logger.info("Writing to %s", path)
            path_path = Path(path)
            path_path.parent.mkdir(parents=True, exist_ok=True)
            with path_path.open(mode="w") as json_file:
                json_file.write(json_content)
        else:
            return json.dumps(json_content)
        return None

    def _validate_original_parameters(self) -> None:
        for _, shortuser in enumerate([self.shortuser, self.cc, self.bcc]):
            if len(shortuser) != SSB_TBF_LEN or not isinstance(shortuser, str):
                error_msg = f'Brukeren {shortuser} - "trebokstavsforkortelse" - må være tre bokstaver...'
                raise ValueError(error_msg)

        if not isinstance(self.date, datetime.date):
            error_msg = "(datetime.date) Sett publiseringsdatoen til et gyldig datetime.date objekt."  # type: ignore[unreachable]
            raise TypeError(error_msg)

        if not isinstance(self.overwrite, bool):
            error_msg = "(Bool) Sett overwrite til enten False = ingen overskriving (dubletter gir feil), eller  True = automatisk overskriving."  # type: ignore[unreachable]
            raise TypeError(error_msg)

        if self.approve not in iter(Approve):
            error_msg = "(Integer) Sett approve til enten 0 = manuell, 1 = automatisk (umiddelbart), eller 2 = JIT-automatisk (just-in-time)"
            raise ValueError(error_msg)

    def _validate_datatype(self) -> None:
        for deltabell_name, deltabell_data in self.data.items():
            if not isinstance(deltabell_name, str):
                error_msg = f"{deltabell_name} is not a string."  # type: ignore[unreachable]
                raise TypeError(error_msg)
            if not isinstance(deltabell_data, pd.DataFrame):
                error_msg = f"Data for {deltabell_name}, must be a pandas DataFrame"  # type: ignore[unreachable]
                raise TypeError(error_msg)

    def _body_from_data(self) -> str:
        # Data should be a iterable of pd.DataFrames at this point,
        # reshape to body
        body = ""
        for filename, elem in self.data.items():
            # Replace all nans in data
            elem_fillna = elem.copy().astype("string").fillna("")
            body += f"--{self.boundary}"
            body += f"\nContent-Disposition:form-data; filename={filename}"
            body += "\nContent-type:text/plain\n\n"
            csv_content = elem_fillna.to_csv(sep=";", index=False, header=False)
            body += str(csv_content)
        body += f"\n--{self.boundary}--"
        return body.replace("\n", "\r\n")  # Statbank likes this?

    def _build_params(self) -> dict[str, str]:
        return {
            "initialier": self.shortuser,
            "hovedtabell": self.tableid,
            "publiseringsdato": self.date.strftime("%Y-%m-%d"),
            "fagansvarlig1": self.cc,
            "fagansvarlig2": self.bcc,
            "auto_overskriv_data": str(int(self.overwrite)),
            "auto_godkjenn_data": str(int(self.approve)),
        }

    def _make_transfer_request(
        self,
        url_params: str,
    ) -> r.Response:
        result = r.post(url_params, headers=self.headers, data=self.body, timeout=100)
        # Trying to clean all auth etc out of response
        try:
            result.raise_for_status()
        except r.HTTPError:
            logger.error(result.text)
            raise
        return result

    def _cleanup_response(self) -> None:
        if hasattr(self.response.request, "headers"):
            del (
                self.response.request.headers
            )  # Auth is stored here also, for some reason
        if hasattr(self.response, "cookies"):
            del self.response.cookies
        if hasattr(self.response, "raw"):
            del self.response.raw
        gc.collect()  # Hoping this removes the del-ed stuff from memory

    def _handle_response(self) -> None:
        pattern_work_number = re.compile(r"lasteoppdragsnummer:(\d+)")
        pattern_publish_date = re.compile(
            r"Publiseringsdato '(\d{2}.\d{2}.\d{4} \d{2}:\d{2}:\d{2})'",
        )
        pattern_publish_time = re.compile(r"Publiseringstid '(\d{2}):(\d{2})'")

        resp_json: TransferResultType = self.response.json()
        response_msg = resp_json["TotalResult"]["Message"]
        match_oppdragsnummer = cast(
            "re.Match[str]",
            pattern_work_number.search(response_msg),
        )
        match_publish_date = cast(
            "re.Match[str]",
            pattern_publish_date.search(response_msg),
        )
        match_publish_time = cast(
            "re.Match[str]",
            pattern_publish_time.search(response_msg),
        )

        if not match_oppdragsnummer[1].isdigit():
            error_msg = f"Lasteoppdragsnummer: {match_oppdragsnummer[1]} er ikke ett rent nummer."
            raise ValueError(error_msg)

        self.oppdragsnummer = match_oppdragsnummer[1]
        publish_date = datetime.datetime.strptime(
            match_publish_date[1],
            r"%d.%m.%Y %H:%M:%S",
        ).astimezone(OSLO_TIMEZONE)
        publish_hour, publish_minute = map(int, match_publish_time.groups())
        publish_date = publish_date.replace(hour=publish_hour, minute=publish_minute)
        logger.info("Publisering satt til: %s", publish_date.isoformat("T", "seconds"))
        logger.info(
            "Følg med på lasteloggen (tar noen minutter): %s",
            {self.urls["gui"] + self.oppdragsnummer},
        )
        self.resp_json = resp_json
