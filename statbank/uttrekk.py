#!/usr/bin/env python3

import copy
import json
from decimal import ROUND_HALF_UP, Decimal, localcontext

import pandas as pd
import requests as r
from requests.exceptions import ConnectionError

from statbank.auth import StatbankAuth
from statbank.uttrekk_validations import StatbankUttrekkValidators


class StatbankUttrekksBeskrivelse(StatbankAuth, StatbankUttrekkValidators):
    """
    Class for talking with the "uttrekksbeskrivelses-API",
    which describes metadata about shape of data to be transferred.
    And metadata about the table itself in Statbankens system,
    like ID, name of codelists etc.
    ...

    Attributes
    ----------
    loaduser : str
        Username for Statbanken, not the same as "tbf" or
        "common personal username" in other SSB-systems
    url : str
        Main url for transfer
    time_retrieved : str
        Time of getting the Uttrekksbeskrivelse
    tableid: str
        Originally the ID of the main table, which to get the
        Uttrekksbeskrivelse on,
        but is reset based on the info in the Uttrekksbeskrivelse.
        To compansate for the possibility of the user sending in
        "tablename"-name as tableid.
        That should work also, probably...
    tablename : str
        The name of the main table in Statbanken, not numbers, like the ID is.
    subtables : dict
        Names and descriptions of the individual "table-parts" that needs
        to be sent in as different DataFrames.
    variables : dict
        Metadata about the columns in the different table-parts.
    codelists : dict
        Metadata about column-contents, like formatting on time,
        or possible values ("codes").
    suppression : dict
        Details around extra columns which describe main column's "prikking",
        meaning their suppression-type.
    headers : dict
        The headers for the request, might be sent in
        from a StatbankTransfer-object.
    filbeskrivelse : dict
        The "raw" json returned from the API-get-request, loaded into a dict.

    Methods
    -------
    validate(data=pd.DataFrame, raise_errors=bool):
        Checks sent data against UttrekksBeskrivelse,
        raises errors at end of checking, if raise_errors not set to False.
    _get_uttrekksbeskrivelse():
        Handles the response from the API, prints some status.
    _make_request():
        Makes the actual get-request, split out for easier mocking
    _split_attributes():
        After a successful response,
        split recieved data into attributes for easier access
    __init__():

    """

    def __init__(
        self, tableid, loaduser, raise_errors=False, headers=None, printing=True
    ):
        self.loaduser = loaduser
        self.url = self._build_urls()["uttak"]
        self.time_retrieved = ""
        self.tableid = tableid
        self.raise_errors = raise_errors
        self.tablename = ""
        self.subtables = {}
        self.variables = {}
        self.codelists = {}
        self.suppression = None
        if headers:
            self.headers = headers
        else:
            self.headers = self._build_headers()
        try:
            self._get_uttrekksbeskrivelse(printing)
        finally:
            if hasattr(self, "headers"):
                del self.headers
        self._split_attributes()

    def __str__(self):
        variabel_text = ""
        for i, deltabell in enumerate(self.variables):
            variabel_text += f"""\nDeltabell (DataFrame) nummer {i+1}:
                {deltabell["deltabell"]}
                """
            variables = [*deltabell["variabler"], *deltabell["statistikkvariabler"]]
            if "null_prikk_missing" in deltabell.keys():
                variables += deltabell["null_prikk_missing"]
            variabel_text += f"Antall kolonner: {len(variables)}"
            for i, variabel in enumerate(variables):
                variabel_text += f"\n\tKolonne {i+1}: "
                if "Kodeliste_text" in variabel.keys():
                    variabel_text += variabel["Kodeliste_text"]
                elif "Text" in variabel.keys():
                    variabel_text += variabel["Text"]
                elif "gjelder_for_text" in variabel.keys():
                    variabel_text += f"""Suppression for column
                    {variabel["gjelder_for__kolonner_nummer"]}:
                    {variabel["gjelder_for_text"]}"""
            variabel_text += f'\nEksempellinje: {deltabell["eksempel_linje"]}'

        return f"""Uttrekksbeskrivelse for statbanktabell {self.tableid}.
        loaduser: {self.loaduser}.

        Hele filbeskrivelsen "rå" ligger under .filbeskrivelse
        Andre attributter:
        .subtables, .codelists, .suppression, .variables
{variabel_text}
        """

    def __repr__(self):
        return (
            'StatbankUttrekksBeskrivelse(tableid="'
            + f'{self.tableid}", loaduser="{self.loaduser}")'
        )

    def transferdata_template(self) -> dict:
        """Get the shape the data should have to name the "deltabeller".
        If we didnt use a dictionary we would have to rely on the order of a list of "deltabeller".
        Instead we chose to explicitly name the deltabller in this package.

        Returns
        -------
        A dictionary with correct keys, but placeholders for where the dataframes should go.
        """
        template = {k: f"df{i}" for i, (k, v) in enumerate(self.subtables.items())}
        print("{")
        for k, v in template.items():
            print(f'"{k}" : {v},')
        print("}")
        return template

    def to_json(self, path: str = "") -> dict:
        """Store a copy of the current state of the uttrekk-object as a json.
        If path is provided, tries to write to it,
        otherwise will return a json-string for you to handle like you wish.

        Parameters
        -------
        path: if provided, will try to write a json to a local path

        Returns
        -------
        If path is provided, tries to write a json there and returns nothing.
        If path is not provided, returns the json-string for you to handle as you wish.
        """
        # Need to this because im stupidly adding methods from other class as attributes
        content = {k: v for k, v in self.__dict__.items() if not callable(v)}

        if path:
            print(f"Writing to {path}")
            with open(path, mode="w") as json_file:
                json_file.write(json.dumps(content))
        else:
            return json.dumps(content)

    def validate(self, data, raise_errors: bool = False, printing: bool = True) -> dict:
        """Uses the contents of itself to validate the data against.
        All validation happens locally, so dont be afraid of any data
        being sent to statbanken using this method.

        Parameters
        -------
        data: The data to validate in a dictionary of deltabell-names as keys and pandas-dataframes as values.
        raise_errors: True/False based on if you want the method to raise its own errors or not.
        printing: True/False based on if you want a verbose printing of everything the validation checks.

        Returns
        -------
        A dictionary of the errors the validation wants to raise.
        """
        if not raise_errors:
            raise_errors = self.raise_errors

        validation_errors = {}
        if printing:
            print("\nvalidating...")

        self._validate_number_dataframes(data=data)
        validation_errors = self._validate_number_columns(
            data, validation_errors, printing
        )
        (
            categorycode_outside,
            categorycode_missing,
            validation_errors,
        ) = self._category_code_usage(data, validation_errors, printing)
        validation_errors = self._check_for_floats(data, validation_errors, printing)
        validation_errors = self._check_rounding(data, validation_errors, printing)
        validation_errors = self._check_time_formats(data, validation_errors, printing)
        validation_errors = self._check_suppression(data, validation_errors, printing)
        validation_errors = self._check_unique_combinations_categories(
            data, validation_errors, printing
        )

        if raise_errors and validation_errors:
            raise Exception(list(validation_errors.values()))
        return validation_errors

    def round_data(self, data) -> dict:
        """Checks that all decimal numbers are converted to strings,
        with specific length kept after the decimal-seperator ","
        IMPORTANT: Rounds "real halves" UP, instead of "to even numbers" like Python does by default.
        This is maybe the behaviour staticians are used to from Excel, SAS etc.

        Parameters
        -------
        data: The data to validate in a dictionary of deltabell-names as keys and pandas-dataframes as values.

        Returns
        -------
        A dictionary in the same shape as sent in, but with dataframes altered to correct for rounding.
        """
        data_copy = copy.deepcopy(data)
        for deltabell in self.variables:
            deltabell_name = deltabell["deltabell"]
            for variabel in deltabell["variabler"] + deltabell["statistikkvariabler"]:
                if "Antall_lagrede_desimaler" in variabel.keys():
                    col_num = int(variabel["kolonnenummer"]) - 1
                    decimal_num = int(variabel["Antall_lagrede_desimaler"])
                    # Nan-handling?
                    if (
                        "float"
                        in str(data_copy[deltabell_name].dtypes[col_num]).lower()
                    ):  # If column is passed in as a float, we can handle it
                        print(
                            f"Rounding column {col_num + 1} in {deltabell_name} into a string, with {decimal_num} decimals."
                        )
                        data_copy[deltabell_name][
                            data_copy[deltabell_name].columns[col_num]
                        ] = (
                            data_copy[deltabell_name]
                            .iloc[:, col_num]
                            .astype("Float64")
                            .apply(self._round_up, decimals=decimal_num)
                            .astype(str)
                            .str.replace("<NA>", "", regex=False)
                            .str.replace(".", ",", regex=False)
                        )
                    else:
                        print(
                            "not a float",
                            col_num,
                            ":",
                            data_copy[deltabell_name].dtypes[col_num],
                        )
                else:
                    print(f"Not rounding {variabel['kolonnenummer']}")
        return data_copy

    @staticmethod
    def _round_up(n: float, decimals: int = 0) -> str:
        with localcontext() as ctx:
            ctx.rounding = ROUND_HALF_UP
            if pd.isnull(n):
                return ""
            elif decimals and n:
                n = round(Decimal(n), decimals)
            elif n:
                n = Decimal(n).to_integral_value()
        return str(n)

    def _get_uttrekksbeskrivelse(self, printing) -> dict:
        filbeskrivelse_url = self.url + "tableId=" + self.tableid
        try:
            filbeskrivelse = self._make_request(filbeskrivelse_url, self.headers)
        finally:
            if hasattr(self, "headers"):
                del self.headers

        if filbeskrivelse.status_code != 200:
            raise ConnectionError(filbeskrivelse, filbeskrivelse.text)
        # Rakel encountered an error with a tab-character in the json, should we just strip this?
        filbeskrivelse = filbeskrivelse.text.replace("\t", "")
        # Also deletes / overwrites returned Auth-header from get-request
        filbeskrivelse = json.loads(filbeskrivelse)
        if printing:
            print(
                f"""Hentet uttaksbeskrivelsen for {filbeskrivelse['Huvudtabell']},
            med tableid: {self.tableid}
            den {filbeskrivelse['Uttaksbeskrivelse_lagd']}"""
            )

        # reset tableid and hovedkode after content of request
        self.filbeskrivelse = filbeskrivelse

    def _make_request(self, url: str, header: dict):
        return r.get(url, headers=self.headers)

    def _split_attributes(self) -> None:
        # tableid might have been "hovedkode" up to this point, as both are valid in the URI
        self.time_retrieved = self.filbeskrivelse["Uttaksbeskrivelse_lagd"]
        self.tableid = self.filbeskrivelse["TabellId"]
        self.tablename = self.filbeskrivelse["Huvudtabell"]
        self.subtables = {
            x["Filnavn"]: x["Filtext"] for x in self.filbeskrivelse["DeltabellTitler"]
        }
        self.variables = self.filbeskrivelse["deltabller"]
        self.codelists = {}
        if "kodelister" in self.filbeskrivelse.keys():
            for kodeliste in self.filbeskrivelse["kodelister"]:
                new_kodeliste = {}
                for kode in kodeliste["koder"]:
                    new_kodeliste[kode["kode"]] = kode["text"]
                self.codelists[kodeliste["kodeliste"]] = {"koder": new_kodeliste}
                remain_keys = list(kodeliste.keys())
                remain_keys.remove("koder")
                remain_keys.remove("kodeliste")
                for k in remain_keys:
                    self.codelists[kodeliste["kodeliste"]][k] = kodeliste[k]

        if "null_prikk_missing_kodeliste" in self.filbeskrivelse.keys():
            self.suppression = self.filbeskrivelse["null_prikk_missing_kodeliste"]
