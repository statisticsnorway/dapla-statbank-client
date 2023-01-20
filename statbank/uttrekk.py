#!/usr/bin/env python3

import copy
import json
from decimal import ROUND_HALF_UP, Decimal, localcontext

import pandas as pd
import requests as r
from requests.exceptions import ConnectionError

from .auth import StatbankAuth


class StatbankUttrekksBeskrivelse(StatbankAuth):
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

    def __init__(self, tableid, loaduser, raise_errors=False, headers=None):
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
            self._get_uttrekksbeskrivelse()
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
        template = {k: f"df{i}" for i, (k, v) in enumerate(self.subtables.items())}
        print("{")
        for k, v in template.items():
            print(f'"{k}" : {v},')
        print("}")
        return template

    def to_json(self, path: str = "") -> dict:
        """If path is provided, tries to write to it,
        otherwise will return a json-string for you to handle like you wish.
        """
        if path:
            print(f"Writing to {path}")
            with open(path, mode="w") as json_file:
                json_file.write(json.dumps(self.__dict__))
        else:
            return json.dumps(self.__dict__)

    def validate(self, data, raise_errors: bool = False, printing: bool = True) -> dict:
        if not raise_errors:
            raise_errors = self.raise_errors

        validation_errors = {}
        if printing:
            print("\nvalidating...")

        self._validate_number_dataframes(data)
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

        if raise_errors and validation_errors:
            raise Exception(list(validation_errors.values()))
        return validation_errors

    def round_data(self, data) -> dict:
        """Checks that all decimal numbers are converted to strings,
        with specific length after the decimal-seperator "," """
        data_copy = copy.deepcopy(data)
        for deltabell in self.variables:
            deltabell_name = deltabell["deltabell"]
            for variabel in deltabell["variabler"] + deltabell["statistikkvariabler"]:
                print(variabel)
                if "Antall_lagrede_desimaler" in variabel.keys():
                    col_num = int(variabel["kolonnenummer"]) - 1
                    decimal_num = int(variabel["Antall_lagrede_desimaler"])
                    # Nan-handling?
                    if (
                        "float"
                        in str(data_copy[deltabell_name].dtypes[col_num]).lower()
                    ):  # If column is passed in as a float, we can handle it
                        print(
                            f"Converting column {col_num} in {deltabell_name} into a string, with {decimal_num} decimals."
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
                    print("Not converting")
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

    def _validate_number_dataframes(self, data: dict):
        # Number subtables should match length of data-iterable
        if len(self.subtables.values()) != len(data.values()):
            raise TypeError(
                f"""Please put one or more pandas Dataframes in a dict as your data.
                Keys in the dict should be "deltabell-navn": {self.subtables.keys()}
                """
            )
        for k, df in data.items():
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"{k}'s value is not a dataframe")

    def _validate_number_columns(self, data, validation_errors: dict, printing) -> dict:
        # Number of columns in data must match beskrivelse
        for deltabell_num, deltabell in enumerate(self.variables):
            deltabell_navn = deltabell["deltabell"]
            col_num = len(deltabell["variabler"]) + len(
                deltabell["statistikkvariabler"]
            )  # Mangler prikke-kolonner?
            if "null_prikk_missing" in deltabell.keys():
                col_num += len(deltabell["null_prikk_missing"])
            if len(data[deltabell_navn].columns) != col_num:
                validation_errors[f"col_count_data_{deltabell_num}"] = ValueError(
                    f"""
                    EXPECTING {col_num} COLUMNS IN DATFRAME NUMBER
                    {deltabell_num}: {deltabell_navn}
                    ONLY FOUND {len(data[deltabell_navn].columns)}
                    """
                )
        for k in validation_errors.keys():
            if "col_count_data" in k:
                if printing:
                    print(validation_errors[k])
                break
        else:
            if printing:
                print("Correct number of columns...")
        return validation_errors

    def _check_for_floats(self, data: dict, validation_errors: dict, printing) -> dict:
        for name, df in data.items():
            for col in df.columns:
                if "float" in str(df[col].dtype).lower():
                    error_text = f"""{col} in {name} is a float.
                    Consider running the dict of dataframes through:
                    data = uttrekksbeskrivelse.round_data(data),
                    this rounds UP like SAS and Excel, not to-even as
                    Python does otherwise."""
                    validation_errors[f"contains_floats_{name}_{col}"] = error_text
                    if printing:
                        print(error_text)
        return validation_errors

    def _check_time_formats(self, data, validation_errors: dict, printing) -> dict:
        # Time-columns should follow time format
        for deltabell in self.variables:
            for variabel in deltabell["variabler"]:
                if "Kodeliste_text" in variabel.keys():
                    if "format = " in variabel["Kodeliste_text"]:
                        validation_errors = self._check_time_columns(
                            deltabell["deltabell"],
                            variabel,
                            data,
                            validation_errors,
                            printing,
                        )
        for k in validation_errors.keys():
            if "time_non_digit_column" in k:
                break
            elif "character_match_column" in k:
                break
            elif "special_character_match_column" in k:
                break
            elif "time_single_length_format" in k:
                break
            elif "time_formatlength" in k:
                break
        else:
            if printing:
                print("Timeformat validation ok.")

        return validation_errors

    def _check_time_columns(
        self, deltabell_name, variabel, data, validation_errors: dict, printing
    ) -> dict:
        col_num = int(variabel["kolonnenummer"]) - 1
        timeformat_raw = (
            variabel["Kodeliste_text"].split(" format = ")[1].strip().replace("Å", "å")
        )
        # Check length of coloumn matches length of format
        if not 1 == len(
            data[deltabell_name].iloc[:, col_num].astype(str).str.len().unique()
        ):
            validation_errors[f"time_single_length_format_{col_num}"] = ValueError(
                f"""Column number {col_num} does not have
                a single time format
                in the shape: {timeformat_raw}"""
            )
        if not len(timeformat_raw) == (
            data[deltabell_name].iloc[:, col_num].astype(str).str.len().unique()[0]
        ):
            validation_errors[f"time_formatlength_{col_num}"] = ValueError(
                f"""Column number {col_num} does not match
                time format in the shape: {timeformat_raw}"""
            )

        timeformat = {
            "nums": [i for i, c in enumerate(timeformat_raw) if c.islower()],
            "chars": {i: c for i, c in enumerate(timeformat_raw) if c.isupper()},
            "specials": {i: c for i, c in enumerate(timeformat_raw) if not c.isalnum()},
        }

        if timeformat["nums"]:
            for num in timeformat["nums"]:
                if not all(
                    data[deltabell_name].iloc[:, col_num].str[num].str.isdigit()
                ):
                    validation_errors[f"time_non_digit_column{col_num}"] = ValueError(
                        f"Character number {num} in column {col_num} in DataFrame {deltabell_name}, does not match format {timeformat_raw}"
                    )
        if timeformat["chars"]:
            for i, char in timeformat["chars"].items():
                if not all(data[deltabell_name].iloc[:, col_num].str[i] == char):
                    validation_errors[f"character_match_column{col_num}"] = ValueError(
                        f"Should be capitalized character? Character {char}, character number {num} in column {col_num} in DataFrame {deltabell_name}, does not match format {timeformat_raw}"
                    )
        if timeformat["specials"]:
            for i, special in timeformat["specials"].items():
                if not all(data[deltabell_name].iloc[:, col_num].str[i] == special):
                    validation_errors[
                        f"special_character_match_column{col_num}"
                    ] = ValueError(
                        f"Should be the special character {special}, character number {num} in column {col_num} in DataFrame {deltabell_name}, does not match format {timeformat_raw}"
                    )
        return validation_errors

    def _check_suppression(self, data, validation_errors: dict, printing) -> dict:
        if self.suppression:
            prikk_codes = [code["Kode"] for code in self.suppression]
            prikk_codes += [""]
            for deltabell in self.variables:
                deltabell_name = deltabell["deltabell"]
                if "null_prikk_missing" in deltabell.keys():
                    for prikk_col in deltabell["null_prikk_missing"]:
                        col_num = int(prikk_col["kolonnenummer"]) - 1
                        if not all(
                            data[deltabell_name].iloc[:, col_num].isin(prikk_codes)
                        ):
                            validation_errors[
                                f"prikke_character_match_column{col_num}"
                            ] = ValueError(
                                f"Prikke-code not among allowed prikkecodes: {prikk_codes}, in column {col_num} in DataFrame {deltabell_name}."
                            )
        for k in validation_errors.keys():
            if "prikke_character_match_column" in k:
                break
        else:
            if printing:
                print("suppression-codes validation ok / No prikke-columns in use.")

        return validation_errors

    def _category_code_usage(self, data, validation_errors, printing):
        categorycode_outside = []
        categorycode_missing = []

        check_codes = {}
        for deltabell in self.variables:
            deltabell_navn = deltabell["deltabell"]
            check_codes[deltabell_navn] = {}
            for variabel in deltabell["variabler"]:
                if "Kodeliste_id" in variabel.keys():
                    if variabel["Kodeliste_id"] != "-":
                        check_codes[deltabell_navn][variabel["kolonnenummer"]] = list(
                            self.codelists[variabel["Kodeliste_id"]]["koder"].keys()
                        )

        for deltabell_name, variabel in check_codes.items():
            for col_num, codelist in variabel.items():
                col_unique = data[deltabell_name].iloc[:, int(col_num) - 1].unique()
                for kod in col_unique:
                    if kod not in codelist:
                        categorycode_outside += [
                            f"""Code {kod} in data, but not in uttrekksbeskrivelse,
                            add to statbank admin? From column number
                            {col_num}, in deltabell {deltabell_name}"""
                        ]
                for kod in codelist:
                    if kod not in col_unique:
                        categorycode_missing += [
                            f"""Code {kod} missing from column number
                            {col_num}, in deltabell {deltabell_name}"""
                        ]
        # No values outside, warn of missing from codelists on categorical columns
        if categorycode_outside:
            if printing:
                print("Codes in data, outside codelist:")
            if printing:
                print("\n".join(categorycode_outside))
            if printing:
                print()
            validation_errors["categorycode_outside"] = ValueError(categorycode_outside)
        else:
            if printing:
                print("No codes in categorical columns outside codelist.")
        if categorycode_missing:
            if printing:
                print(
                    """Category codes missing from data (This is ok,
                just make sure missing data is intentional):"""
                )
            if printing:
                print("\n".join(categorycode_missing))
            if printing:
                print()
        else:
            if printing:
                print("No codes missing from categorical columns.")
        return categorycode_outside, categorycode_missing, validation_errors

    def _check_rounding(self, data: dict, validation_errors: dict, printing) -> dict:
        """If a column should have a set number of decimals,
        check if its a string, and how many places are used after the
        decimal seperator: ","
        """
        for deltabell in self.variables:
            deltabell_name = deltabell["deltabell"]
            for variabel in deltabell["variabler"] + deltabell["statistikkvariabler"]:
                if "Antall_lagrede_desimaler" in variabel.keys():
                    col_num = int(variabel["kolonnenummer"]) - 1
                    decimal_num = int(variabel["Antall_lagrede_desimaler"])
                    error = False
                    column = (
                        data[deltabell_name]
                        .iloc[:, col_num]
                        .copy()
                        .astype(str)
                        .str.replace(".", ",", regex=False)
                    )
                    if decimal_num:
                        if any(decimal_num != column.str.split(",").str[-1].str.len()):
                            error = True
                    elif not column.str.isdigit().all():
                        error = True

                    if error:
                        error_text = f"""Check that string column {col_num} in
                        {deltabell_name} that should be rounded to
                        {decimal_num} decimal places,
                        has correct number of decimals. And consider converting from a
                        non-rounded float to a string with this method:
                        data = uttrekksbeskrivelse.round_data(data),
                    this rounds UP like SAS and Excel, not to-even as
                    Python does otherwise."""
                        validation_errors[
                            f"rounding_error_{deltabell_name}_{col_num}"
                        ] = ValueError(error_text)
                        if printing:
                            print(error_text)
        return validation_errors

    def _get_uttrekksbeskrivelse(self) -> dict:
        filbeskrivelse_url = self.url + "tableId=" + self.tableid
        try:
            filbeskrivelse = self._make_request(filbeskrivelse_url, self.headers)
        finally:
            if hasattr(self, "headers"):
                del self.headers

        if filbeskrivelse.status_code != 200:
            raise ConnectionError(filbeskrivelse, filbeskrivelse.text)
        # Also deletes / overwrites returned Auth-header from get-request
        filbeskrivelse = json.loads(filbeskrivelse.text)
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
