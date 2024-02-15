#!/usr/bin/env python3
from __future__ import annotations

import copy
import json
import math
from decimal import ROUND_HALF_UP
from decimal import Decimal
from decimal import localcontext
from pathlib import Path
from typing import Any

import pandas as pd
import requests as r

from statbank.auth import StatbankAuth
from statbank.logger import logger
from statbank.uttrekk_validations import StatbankUttrekkValidators
from statbank.uttrekk_validations import StatbankValidateError


class StatbankUttrekksBeskrivelse(StatbankAuth, StatbankUttrekkValidators):
    """Class for talking with the "uttrekksbeskrivelses-API", which describes metadata about shape of data to be transferred.

    And metadata about the table itself in Statbankens system,
    like ID, name of codelists etc.


    Attributes:
        loaduser (str): Username for Statbanken, not the same as "tbf" or "common personal username" in other SSB-systems
        url (str): Main url for transfer
        time_retrieved  (str): Time of getting the Uttrekksbeskrivelse
        tableid (str): Originally the ID of the main table, which to get the
            Uttrekksbeskrivelse on, but is reset based on the info in the Uttrekksbeskrivelse.
            To compansate for the possibility of the user sending in "tablename"-name as tableid.
        tablename (str): The name of the main table in Statbanken, not numbers, like the ID is.
        subtables (dict): Names and descriptions of the individual "table-parts"
            that needs to be sent in as different DataFrames.
        variables (dict): Metadata about the columns in the different table-parts.
        codelists (dict): Metadata about column-contents, like formatting on time, or possible values ("codes").
        suppression (dict): Details around extra columns which describe main column's "prikking", meaning their suppression-type.
        headers (dict): The headers for the request, might be sent in from a StatbankTransfer-object.
        filbeskrivelse (dict): The "raw" json returned from the API-get-request, loaded into a dict.

    Methods:
        validate(data=pd.DataFrame, raise_errors=bool):
            Checks sent data against UttrekksBeskrivelse,
            raises errors at end of checking, if raise_errors not set to False.
        transferdata_template():
        round_data():
        to_json():
        get_totalcodes_dict():


    """

    def __init__(
        self,
        tableid: str,
        loaduser: str,
        raise_errors: bool = False,
        headers: dict[str, str] | None = None,
    ) -> None:
        """Makes a request to the Statbank-API, populates the objects attributes with parts of the return values."""
        self.loaduser = loaduser
        self.url = self._build_urls()["uttak"]
        self.time_retrieved = ""
        self.tableid = tableid
        self.raise_errors = raise_errors
        self.tablename = ""
        self.subtables: dict[str, str | dict[str, Any]] = {}
        self.variables: dict[str, str | dict[str, Any]] = {}
        self.codelists: dict[str, str | dict[str, Any]] = {}
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

    def __str__(self) -> str:
        """Returns a string representation of the object, which is the Uttrekksbeskrivelse."""
        variabel_text = ""
        for i, deltabell in enumerate(self.variables):
            variabel_text += f"""\nDeltabell (DataFrame) nummer {i+1}:
                {deltabell["deltabell"]}
                """
            variables = [*deltabell["variabler"], *deltabell["statistikkvariabler"]]
            if "null_prikk_missing" in deltabell:
                variables += deltabell["null_prikk_missing"]
            if "internasjonal_rapportering" in deltabell:
                variables += deltabell["internasjonal_rapportering"]
            variabel_text += f"Antall kolonner: {len(variables)}"
            for j, variabel in enumerate(variables):
                variabel_text += f"\n\tKolonne {j+1}: "
                if "Kodeliste_text" in variabel:
                    variabel_text += variabel["Kodeliste_text"]
                elif "Text" in variabel:
                    variabel_text += variabel["Text"]
                elif "gjelder_for_text" in variabel:
                    variabel_text += f"""Suppression for column
                    {variabel["gjelder_for__kolonner_nummer"]}:
                    {variabel["gjelder_for_text"]}"""
            variabel_text += f'\nEksempellinje: {deltabell["eksempel_linje"]}'

        mult_codelists = math.prod([len(x["koder"]) for x in self.codelists.values()])
        variabel_text += f'\n"Ekspandert matrise/antall koder i kodelistene ganget med hverandre er: {mult_codelists}'

        return f"""Uttrekksbeskrivelse for statbanktabell {self.tableid}.
        loaduser: {self.loaduser}.

        Hele filbeskrivelsen "rå" ligger under .filbeskrivelse
        Andre attributter:
        .subtables, .codelists, .suppression, .variables
{variabel_text}
        """

    def __repr__(self) -> str:
        """Return a string representation of how to instantiate this object again."""
        return f'StatbankUttrekksBeskrivelse(tableid="{self.tableid}", loaduser="{self.loaduser}")'

    def transferdata_template(
        self,
        *dfs: list[pd.DataFrame] | pd.DataFrame,
    ) -> dict[str, str | pd.DataFrame]:
        """Get the shape the data should have to name the "deltabeller".

        If we didnt use a dictionary we would have to rely on the order of a list of "deltabeller".
        Instead we chose to explicitly name the deltabller in this package, and make you check this after creation.

        Parameters
        -------
        dfs: if provided, will try to use pandas dataframes sent in to populate the dict returned by the method.
            Send in one dataframe, several, a list of dataframes or similar.
            ORDER IS IMPORTANT make sure the result is what you expect.

        Returns:
        -------
        A dictionary with correct keys, but placeholders for where the dataframes should go if no Dataframes are passed.
        A dict of dataframes as values if a list of Dataframes are sent in, or dataframes as individual parameters.
        """
        # If sending in a list, unwrap one layer
        if dfs:
            if not isinstance(dfs[0], pd.DataFrame) and len(dfs) == 1:
                dfs = dfs[0]
            if not all(isinstance(df, pd.DataFrame) for df in dfs):
                error_msg = "All elements sent in to transferdata_template must be pandas dataframes."
                raise TypeError(error_msg)
            if len(dfs) != len(self.subtables):
                error_msg = (
                    "Number of dataframes in must match the number of subtables."
                )
                raise KeyError(error_msg)
            template = {k: dfs[i] for i, k in enumerate(self.subtables.keys())}
        else:
            template = {k: f"df{i}" for i, k in enumerate(self.subtables.keys())}

        msg = "{\n"
        for k, v in template.items():
            if isinstance(v, pd.DataFrame):
                msg += f'"{k}" : Dataframe with column-names: {v.columns}\n'
            else:
                msg += f'"{k}" : {v},\n'
        msg += "}"
        logger.info(msg)
        return template

    def to_json(self, path: str = "") -> None | str:
        """Store a copy of the current state of the uttrekk-object as a json.

        If path is provided, tries to write to it,
        otherwise will return a json-string for you to handle like you wish.

        Args:
            path (str): if provided, will try to write a json to a local path

        Returns:
            None | str: If path is provided, tries to write a json to a file and returns nothing.
                If path is not provided, returns the json-string for you to handle as you wish.
        """
        # Need to this because im stupidly adding methods from other class as attributes
        content = {k: v for k, v in self.__dict__.items() if not callable(v)}

        if path:
            logger.info("Writing to %s", path)
            with Path(path).open(mode="w") as json_file:
                json_file.write(json.dumps(content))
        else:
            return json.dumps(content)
        return None

    def validate(
        self,
        data: dict[str, pd.DataFrame],
        raise_errors: bool = False,
    ) -> dict[str, Exception]:
        """Uses the contents of itself to validate the data against.

        All validation happens locally, so dont be afraid of any data
        being sent to statbanken using this method.

        Args:
            data (dict[str, pd.DataFrame]): The data to validate in a dictionary of deltabell-names as keys and pandas-dataframes as values.
            raise_errors (bool): True/False based on if you want the method to raise its own errors or not.

        Returns:
            dict[str, Exception]: A dictionary of the errors the validation wants to raise.
        """
        if not raise_errors:
            raise_errors = self.raise_errors

        validation_errors = {}
        logger.info("\nvalidating...")

        self._validate_number_dataframes(data=data)
        validation_errors = self._validate_number_columns(
            data,
            validation_errors,
        )
        (
            categorycode_outside,
            categorycode_missing,
            validation_errors,
        ) = self._category_code_usage(data, validation_errors)
        validation_errors = self._check_for_floats(data, validation_errors)
        validation_errors = self._check_for_literal_nans_in_strings(
            data,
            validation_errors,
        )
        validation_errors = self._check_rounding(data, validation_errors)
        validation_errors = self._check_time_formats(data, validation_errors)
        validation_errors = self._check_suppression(data, validation_errors)
        validation_errors = self._check_unique_combinations_categories(
            data,
            validation_errors,
        )

        if raise_errors and validation_errors:
            raise StatbankValidateError(list(validation_errors.values()))
        return validation_errors

    def get_totalcodes_dict(self) -> dict[str, str]:
        """Makes a dict from each codelist where a code for "totals" is included.

        Keys being the name of the codelist, values being the code to put into categorical columns, that describes totals.
        This dict can be passed into the parameters "fillna_dict" and "grand_total" in the function "agg_all_combos" in the package ssb-fagfunksjoner.

        Returns:
            dict[str, str]: A dictionary with the codelist as keys, the total-codes as values.
        """
        result = {}
        for name, kodeliste in self.codelists.items():
            if "SumIALtTotalKode" in list(kodeliste.keys()):
                result[name] = kodeliste["SumIALtTotalKode"]
        return result

    def round_data(self, data: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        """Converts all decimal numbers to strings, with the correct number of decimals.

        IMPORTANT: Rounds "real halves" (0.5) UP, instead of "to even numbers" like Python does by default.
        This is maybe the behaviour staticians are used to from Excel, SAS etc.

        Args:
            data (dict[str, pd.DataFrame]): The data to validate in a dictionary of deltabell-names as keys and pandas-dataframes as values.

        Returns:
            dict[str, pd.DataFrame]: A dictionary in the same shape as sent in, but with dataframes altered to correct for rounding.
        """
        data_copy = copy.deepcopy(data)
        for deltabell in self.variables:
            deltabell_name = deltabell["deltabell"]
            for variabel in deltabell["variabler"] + deltabell["statistikkvariabler"]:
                if "Antall_lagrede_desimaler" in variabel:
                    col_num = int(variabel["kolonnenummer"]) - 1
                    decimal_num = int(variabel["Antall_lagrede_desimaler"])
                    # Nan-handling?
                    if (
                        "float"
                        in str(data_copy[deltabell_name].dtypes[col_num]).lower()
                    ):  # If column is passed in as a float, we can handle it
                        logger.info(
                            "Rounding column %s in %s into a string, with %s decimals.",
                            (col_num + 1, deltabell_name, decimal_num),
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
                        logger.info(
                            "not a float %s: %s",
                            (col_num, str(data_copy[deltabell_name].dtypes[col_num])),
                        )
                else:
                    logger.info(
                        "Not rounding %s",
                        str(variabel["kolonnenummer"]),
                    )
        return data_copy

    @staticmethod
    def _round_up(n: float, decimals: int = 0) -> str:
        with localcontext() as ctx:
            ctx.rounding = ROUND_HALF_UP
            if pd.isna(n):
                n = ""
            elif decimals and n:
                n = round(Decimal(n), decimals)
            elif n:
                n = Decimal(n).to_integral_value()
        return str(n)

    def _get_uttrekksbeskrivelse(self) -> None:
        filbeskrivelse_url = self.url + "tableId=" + self.tableid
        try:
            filbeskrivelse_response = self._make_request(filbeskrivelse_url)
        finally:
            if hasattr(self, "headers"):
                del self.headers

        # Rakel encountered an error with a tab-character in the json, should we just strip this?
        filbeskrivelse_json = filbeskrivelse_response.text.replace("\t", "")
        # Also deletes / overwrites returned Auth-header from get-request
        filbeskrivelse = json.loads(filbeskrivelse_json)
        logger.info(
            "Hentet uttaksbeskrivelsen for %s, med tableid: %s den %s",
            filbeskrivelse["Huvudtabell"],
            self.tableid,
            str(filbeskrivelse["Uttaksbeskrivelse_lagd"]),
        )

        # reset tableid and hovedkode after content of request
        self.filbeskrivelse = filbeskrivelse

    def _make_request(self, url: str) -> r.Response:
        response = r.get(url, headers=self.headers, timeout=10)
        response.raise_for_status()
        return response

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
        if "kodelister" in self.filbeskrivelse:
            kodelister = self.filbeskrivelse["kodelister"]
            if "IRkodelister" in self.filbeskrivelse:
                kodelister = [*kodelister, *self.filbeskrivelse["IRkodelister"]]
            for kodeliste in kodelister:
                new_kodeliste = {}
                for kode in kodeliste["koder"]:
                    new_kodeliste[kode["kode"]] = kode["text"]
                self.codelists[kodeliste["kodeliste"]] = {"koder": new_kodeliste}
                remain_keys = list(kodeliste.keys())
                remain_keys.remove("koder")
                remain_keys.remove("kodeliste")
                for k in remain_keys:
                    self.codelists[kodeliste["kodeliste"]][k] = kodeliste[k]

        if "null_prikk_missing_kodeliste" in self.filbeskrivelse:
            self.suppression = self.filbeskrivelse["null_prikk_missing_kodeliste"]
