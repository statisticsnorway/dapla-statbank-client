#!/usr/bin/env python3

import json
import math
import os
import urllib
from datetime import datetime as dt
from datetime import timedelta as td

import pandas as pd
import requests as r

from .auth import StatbankAuth


class StatbankTransfer(StatbankAuth):
    """
    Class for talking with the "transfer-API",
    which actually recieves the data from the user and sends it to Statbank.
    ...

    Attributes
    ----------
    data : pd.DataFrame or list of pd.DataFrames
        Number of DataFrames needs to match the number of "deltabeller" in
        the uttakksbeskrivelse.
        Data-shape can be validated before transfer with the
        Uttakksbeskrivelses-class.
    loaduser : str
        Username for Statbanken, not the same as "shortuser" or
        "common personal username" in other SSB-systems
    tableid: str
        The numeric id of the table, matching the one found on the website.
        Should be a 5-length numeric-string. Alternativley it should be
        possible to send in the "hovedtabellnavn" instead of the tableid.
    shortuser : str
        The abbrivation of username at ssb. Three letters, like "cfc"
    date : str
        Date for publishing the transfer. Shape should be "yyyy-mm-dd",
        like "2022-01-01".
        Statbanken only allows publishing four months into the future?
    cc : str
        First person to be notified by email of transfer.
        Defaults to the same as "shortuser"
    bcc : str
        Second person to be notified by email of transfer.
        Defaults to the same as "cc1"
    overwrite : bool
        False = no overwrite
        True = overwrite
    approve : int
        0 = manual approval
        1 = automatic approval at transfer-time (immediately)
        2 = JIT (Just In Time), approval right before publishing time
    validation : bool
        Set to True, if you want the python-validation code to run user-side.
        Set to False, if its slow and unnecessary.
    boundary : str
        String that defines the splitting of
        the body in the transfer-post-request.
        Kept here for uniform choice through the class.
    urls : dict
        Urls for transfer, observing the result etc.,
        built from environment variables in Dapla-environment
    headers: dict
        Might be deleted without warning.
        Temporarily holds the Authentication for the request.
    params: dict
        This dict will be built into the url in the post request.
        Keep it in this nice shape for later introspection.
    body: str
        The data parsed into the body-shape the Statbank-API expects in
        the transfer-post-request.
    response: requests.response
        The resulting response from the transfer-request.
        Headers might be deleted without warning.
    delay:
        Not editable, please dont try.
        Indicates if the Transfer has been sent yet, or not.

    Methods
    -------
    transfer():
        If Transfer was delayed,
        you can make the transfer by calling this method.
    _validate_original_parameters():
        Validating "pure" parameters on the way into the class.
    _build_urls():
        INHERITED - See description under StatbankAuth
    _build_headers():
        INHERITED - See description under StatbankAuth
    _build_params():
        Builds the params to be attached to the url
    _validate_datatype():
        Validates the data to be a dict of strings and Dataframes.
    _body_from_data():
        Converts data to .body for the transfer request to add to
        json/data/body.
    _handle_response():
        Handles the response back from the transfer post-request
    __init__():
    """

    def __init__(
        self,
        data: dict,
        tableid: str = None,
        loaduser: str = "",
        shortuser: str = "",
        date: dt = dt.now() + td(days=1),  # noqa: B008
        cc: str = "",
        bcc: str = "",
        overwrite: bool = True,
        approve: int = 1,
        validation: bool = True,
        delay: bool = False,
        headers=None,
    ):
        self.data = data
        self.tableid = tableid

        if isinstance(loaduser, str) and loaduser != "":
            self.loaduser = loaduser
        else:
            raise ValueError("You must set loaduser as a parameter")

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

        if isinstance(date, str):
            self.date = date
        else:
            self.date = date.strftime("%Y-%m-%d")

        self.overwrite = overwrite
        self.approve = approve
        self.validation = validation
        self.__delay = delay

        self.boundary = "12345"
        if validation:
            self._validate_original_parameters()

        self.urls = self._build_urls()
        if not self.delay:
            if headers:
                self.transfer(headers)
            else:
                self.transfer()

    def transfer(self, headers: dict = {}):  # noqa: B006
        """The headers-parameter is for a future implemention
        of a possible BatchTransfer, dont use it please."""
        # In case transfer has already happened, dont transfer again
        if hasattr(self, "oppdragsnummer"):
            raise ValueError(
                f"""Already transferred?
                {self.urls['gui'] +self.oppdragsnummer}
                Remake the StatbankTransfer-object if intentional."""
            )
        if not headers:
            self.headers = self._build_headers()
        else:
            self.headers = headers
        try:
            self.params = self._build_params()
            self._validate_datatype()
            self.body = self._body_from_data()

            url_load_params = self.urls["loader"] + urllib.parse.urlencode(self.params)
            self.response = self._make_transfer_request(url_load_params)
            if self.response.status_code == 200:
                del (
                    self.response.request.headers
                )  # Auth is stored here also, for some reason
        finally:
            del self.headers  # Cleaning up auth-storing
            self.__delay = False
        self._handle_response()

    def __str__(self):
        if self.delay:
            return f"""Overføring for statbanktabell {self.tableid}.
            loaduser: {self.loaduser}.
            Ikke overført enda."""
        else:
            return f"""Overføring for statbanktabell {self.tableid}.
    loaduser: {self.loaduser}.
    Publisering: {self.date}.
    Lastelogg: {self.urls['gui'] + self.oppdragsnummer}"""

    def __repr__(self):
        return (
            "StatbankTransfer([data],"
            + f'tableid="{self.tableid}", loaduser="{self.loaduser}")'
        )

    @property
    def delay(self):
        return self.__delay

    def to_json(self, path: str = "") -> dict:
        """If path is provided, tries to write to it,
        otherwise will return a json-string for you to handle like you wish.
        """
        print(
            "Warning, some nested, deeper data-structures"
            + " like dataframes and other class-objects will not be serialized"
        )
        json_content = json.dumps(self.__dict__, default=lambda o: "<not serializable>")
        # If path provided write to it, otherwise return the string-content
        if path:
            print(f"Writing to {path}")
            with open(path, mode="w") as json_file:
                json_file.write(json_content)
        else:
            return json.dumps(json_content)

    def _validate_original_parameters(self) -> None:

        for _, shortuser in enumerate([self.shortuser, self.cc, self.bcc]):

            if len(shortuser) != 3 or not isinstance(shortuser, str):
                raise ValueError(
                    f'Brukeren {shortuser} - "trebokstavsforkortelse"'
                    + " - må være tre bokstaver..."
                )

        if not isinstance(self.date, dt):
            if not self._valid_date_form(self.date):
                raise ValueError(
                    "Skriv inn datoformen for publisering" + " som 1900-01-01"
                )

        if not isinstance(self.overwrite, bool):
            raise ValueError(
                "(Bool) Sett overwrite til enten False = "
                + "ingen overskriving (dubletter gir feil), "
                + "eller  True = automatisk overskriving"
            )

        if self.approve not in [0, 1, 2]:
            raise ValueError(
                "(Integer) Sett approve til enten 0 = manuell, "
                + "1 = automatisk (umiddelbart), "
                + "eller 2 = JIT-automatisk (just-in-time)"
            )

    def _validate_datatype(self):
        for deltabell_name, deltabell_data in self.data.items():
            if not isinstance(deltabell_name, str):
                raise TypeError(f"{deltabell_name} is not a string.")
            if not isinstance(deltabell_data, pd.DataFrame):
                raise TypeError(
                    f"Data for {deltabell_name}, must be a pandas DataFrame"
                )

    @staticmethod
    def _round_up(n, decimals=0):
        """Python uses "round to even" as default,
        wanted behaviour is "round up".
        So let's implement our own."""
        multiplier = 10**decimals
        return math.ceil(n * multiplier) / multiplier

    def _body_from_data(self) -> str:
        # Data should be a iterable of pd.DataFrames at this point,
        # reshape to body
        body = ""
        for filename, elem in self.data.items():
            # Replace all nans in data
            elem = elem.copy().fillna("")
            body += f"--{self.boundary}"
            body += f"\nContent-Disposition:form-data; filename={filename}"
            body += "\nContent-type:text/plain\n\n"
            csv_content = elem.to_csv(sep=";", index=False, header=False)
            body += str(csv_content)
        body += f"\n--{self.boundary}--"
        body = body.replace("\n", "\r\n")  # Statbank likes this?
        return body

    @staticmethod
    def _valid_date_form(date) -> bool:
        if (date[:4] + date[5:7] + date[8:]).isdigit() and (
            (date[4] + date[7]) == "--"
        ):
            return True
        return False

    def _build_params(self) -> dict:
        if isinstance(self.date, dt):
            self.date = self.date.strftime("%Y-%m-%d")
        return {
            "initialier": self.shortuser,
            "hovedtabell": self.tableid,
            "publiseringsdato": self.date,
            "fagansvarlig1": self.cc,
            "fagansvarlig2": self.bcc,
            "auto_overskriv_data": str(int(self.overwrite)),
            "auto_godkjenn_data": self.approve,
        }

    def _make_transfer_request(
        self,
        url_params: str,
    ):
        return r.post(url_params, headers=self.headers, data=self.body)

    def _handle_response(self) -> None:
        resp_json = json.loads(self.response.text)
        if self.response.status_code == 200:
            response_msg = resp_json["TotalResult"]["Message"]
            try:
                self.oppdragsnummer = response_msg.split("lasteoppdragsnummer:")[
                    1
                ].split(" =")[0]
            except Exception:
                raise ValueError(resp_json)
            if not self.oppdragsnummer.isdigit():
                raise ValueError(
                    f"Lasteoppdragsnummer: {self.oppdragsnummer}"
                    + "er ikke ett rent nummer."
                )

            publish_date = dt.strptime(
                response_msg.split("Publiseringsdato '")[1].split("',")[0],
                "%d.%m.%Y %H:%M:%S",
            )
            publish_hour = int(response_msg.split("Publiseringstid '")[1].split(":")[0])
            publish_minute = int(
                response_msg.split("Publiseringstid '")[1].split(":")[1].split("'")[0]
            )
            publish_time = publish_hour * 3600 + publish_minute * 60
            publish = publish_date + td(0, publish_time)
            publish = publish.strftime("%Y-%m-%d %H:%M")
            print(f"Publisering satt til: {publish}")
            print(
                "Følg med på lasteloggen (tar noen minutter): "
                + f"{self.urls['gui'] + self.oppdragsnummer}"
            )
            print(f"Og evt APIen?: {self.urls['api'] + self.oppdragsnummer}")
            self.resp_json = resp_json
        else:
            print(
                "Take a closer look at StatbankTransfer.response.text"
                + "for more info about connection issues."
            )
            raise ConnectionError(resp_json)
