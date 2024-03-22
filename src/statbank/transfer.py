from __future__ import annotations

import gc
import json
import math
import os
import urllib
from datetime import datetime as dt
from datetime import timedelta as td
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import requests as r

from statbank.auth import StatbankAuth
from statbank.globals import APPROVE_DEFAULT_JIT
from statbank.globals import OSLO_TIMEZONE
from statbank.globals import SSB_TBF_LEN
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
        loaduser (str): Username for Statbanken, not the same as "shortuser" or
            "common personal username" in other SSB-systems
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
        loaduser: str = "",
        shortuser: str = "",
        date: dt | str | None = None,
        cc: str = "",
        bcc: str = "",
        overwrite: bool = True,
        approve: int | str | Approve = APPROVE_DEFAULT_JIT,
        validation: bool = True,
        delay: bool = False,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Make the transfer to statbanken at the end of initializing the object.

        May run the validations from the StatbankValidation class before the transfer.
        """
        self._set_user_attrs(loaduser=loaduser, shortuser=shortuser, cc=cc, bcc=bcc)
        self._set_date(date=date)
        self.data = data
        self.tableid = tableid
        self.overwrite = overwrite
        self.approve = _approve_type_check(approve)
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
            self.response = self._make_transfer_request(url_load_params)
            self._cleanup_response()
        finally:
            del self.headers  # Cleaning up auth-storing
            self.__delay = False
        self._handle_response()

    def __str__(self) -> str:
        """Print a string with the status of the transfer."""
        first_line = f"Overføring for statbanktabell {self.tableid}.\nloaduser: {self.loaduser}.\n"
        if self.delay:
            result = f"""{first_line}Ikke overført enda."""
        else:
            result = f"""{first_line}Publisering: {self.date}.\nLastelogg: {self.urls['gui'] + self.oppdragsnummer}"""
        return result

    def __repr__(self) -> str:
        """Get a representation of how to recreate the object using parameters."""
        return f'StatbankTransfer([data], tableid="{self.tableid}", loaduser="{self.loaduser}")'

    @property
    def delay(self) -> bool:
        """Obfuscate the delay a bit from the user. We dont want transfers transferring again without recreating the object."""
        return self.__delay

    def _set_user_attrs(
        self,
        loaduser: str = "",
        shortuser: str = "",
        cc: str = "",
        bcc: str = "",
    ) -> None:
        if isinstance(loaduser, str) and loaduser != "":
            self.loaduser = loaduser
        else:
            error_msg = "You must set loaduser as a parameter"
            raise ValueError(error_msg)

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

    def _set_date(self, date: dt | str | None = None) -> None:
        # At this point we want date to be a string?
        if date is None:
            date = dt.now().astimezone(OSLO_TIMEZONE) + td(days=1, hours=1)
        if isinstance(date, str):
            self.date: str = date
        else:
            self.date = date.strftime("%Y-%m-%d")

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
            with Path(path).open(mode="w") as json_file:
                json_file.write(json_content)
        else:
            return json.dumps(json_content)
        return None

    def _validate_original_parameters(self) -> None:
        for _, shortuser in enumerate([self.shortuser, self.cc, self.bcc]):
            if len(shortuser) != SSB_TBF_LEN or not isinstance(shortuser, str):
                error_msg = f'Brukeren {shortuser} - "trebokstavsforkortelse" - må være tre bokstaver...'
                raise ValueError(error_msg)

        if not isinstance(self.date, str) or not self._valid_date_form(self.date):
            error_msg = "Skriv inn datoformen for publisering som 1900-01-01"
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

    @staticmethod
    def _round_up(n: float, decimals: int = 0) -> int:
        """Python uses "round to even" as default, wanted behaviour is "round up".

        So let's implement our own.
        """
        multiplier = 10**decimals
        return int(math.ceil(n * multiplier) / multiplier)

    def _body_from_data(self) -> str:
        # Data should be a iterable of pd.DataFrames at this point,
        # reshape to body
        body = ""
        for filename, elem in self.data.items():
            # Replace all nans in data
            elem_fillna = elem.copy().fillna("")
            body += f"--{self.boundary}"
            body += f"\nContent-Disposition:form-data; filename={filename}"
            body += "\nContent-type:text/plain\n\n"
            csv_content = elem_fillna.to_csv(sep=";", index=False, header=False)
            body += str(csv_content)
        body += f"\n--{self.boundary}--"
        return body.replace("\n", "\r\n")  # Statbank likes this?

    @staticmethod
    def _valid_date_form(date: str) -> bool:
        if (date[:4] + date[5:7] + date[8:]).isdigit() and (
            (date[4] + date[7]) == "--"
        ):
            return True
        return False

    def _build_params(self) -> dict[str, str | int]:
        if isinstance(self.date, dt):  # type: ignore[unreachable]
            date = self.date.strftime("%Y-%m-%d")  # type: ignore[unreachable]
        else:
            date = self.date
        return {
            "initialier": self.shortuser,
            "hovedtabell": self.tableid,
            "publiseringsdato": date,
            "fagansvarlig1": self.cc,
            "fagansvarlig2": self.bcc,
            "auto_overskriv_data": str(int(self.overwrite)),
            "auto_godkjenn_data": self.approve,
        }

    def _make_transfer_request(
        self,
        url_params: str,
    ) -> r.Response:
        result = r.post(url_params, headers=self.headers, data=self.body, timeout=15)
        # Trying to clean all auth etc out of response
        result.raise_for_status()
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
        resp_json: TransferResultType = self.response.json()
        response_msg = resp_json["TotalResult"]["Message"]
        self.oppdragsnummer = response_msg.split("lasteoppdragsnummer:")[1].split(" =")[
            0
        ]
        if not self.oppdragsnummer.isdigit():
            error_msg = (
                f"Lasteoppdragsnummer: {self.oppdragsnummer} er ikke ett rent nummer."
            )
            raise ValueError(error_msg)

        publish_date = dt.strptime(
            response_msg.split("Publiseringsdato '")[1].split("',")[0],
            "%d.%m.%Y %H:%M:%S",
        ).astimezone(OSLO_TIMEZONE) + td(hours=1)
        publish_hour = int(response_msg.split("Publiseringstid '")[1].split(":")[0])
        publish_minute = int(
            response_msg.split("Publiseringstid '")[1].split(":")[1].split("'")[0],
        )
        publish_time = publish_hour * 3600 + publish_minute * 60
        publish_date = publish_date + td(0, publish_time)
        logger.info("Publisering satt til: %s", publish_date.isoformat("T", "seconds"))
        logger.info(
            "Følg med på lasteloggen (tar noen minutter): %s",
            {self.urls["gui"] + self.oppdragsnummer},
        )
        self.resp_json = resp_json
