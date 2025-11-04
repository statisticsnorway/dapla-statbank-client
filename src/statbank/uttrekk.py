from __future__ import annotations

import copy
import datetime
import itertools
import json
import math
import sys
from decimal import ROUND_HALF_UP
from decimal import Decimal
from decimal import localcontext
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Literal
from typing import TypedDict
from typing import overload

import pandas as pd
import requests as r
import requests.auth

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

if sys.version_info >= (3, 13):
    from warnings import deprecated
else:
    from typing_extensions import deprecated

if TYPE_CHECKING:
    from furl import furl
    from pathlib_abc import WritablePath

    from .api_types import DelTabellType
    from .api_types import FilBeskrivelseType
    from .api_types import KodelisteTypeParsed
    from .api_types import KolonneInternasjonalRapporteringType
    from .api_types import KolonneStatistikkvariabelType
    from .api_types import KolonneVariabelType
    from .api_types import SuppressionCodeListType
    from .api_types import SuppressionDeltabellCodeListType

from .auth import StatbankAuth
from .auth import StatbankConfig
from .globals import DATETIME_FORMAT
from .globals import UseDb
from .statbank_logger import logger
from .uttrekk_validations import StatbankUttrekkValidators
from .uttrekk_validations import StatbankValidateError


class _UttrekksBeskrivelseDump(TypedDict):
    use_db: Literal["TEST", "PROD"]
    time_retrieved: str
    tableid: str
    tablename: str
    raise_errors: bool
    subtables: dict[str, str]
    variables: list[DelTabellType]
    codelists: dict[str, KodelisteTypeParsed]
    suppression: list[SuppressionCodeListType] | None


class UttrekksBeskrivelseData:
    """Holds parsed data for StatbankUttrekksBeskrivelse.

    Attributes:
        time_retrieved: Time of getting the Uttrekksbeskrivelse
        tableid: The ID of the main table.
        tablename: The name of the main table in Statbanken.
        subtables: Names and descriptions of the individual "table-parts"
          that needs to be sent in as different DataFrames.
        variables: Metadata about the columns in the different table-parts.
        codelists: Metadata about column-contents, like formatting on time, or possible values ("codes").
        suppression: Details around extra columns which describe main column's "prikking", meaning their suppression-type.
        headers: Deprecated attribute. Authinfo not stored here anymore.
    """

    __slots__ = (
        "codelists",
        "subtables",
        "suppression",
        "tableid",
        "tablename",
        "time_retrieved",
        "variables",
    )

    def __init__(  # noqa: D107
        self: Self,
        *,
        tableid: int,
        tablename: str,
        time_retrieved: datetime.datetime,
        subtables: dict[str, str],
        variables: list[DelTabellType],
        codelists: dict[str, KodelisteTypeParsed],
        suppression: list[SuppressionCodeListType] | None,
    ) -> None:
        self.tableid: int = tableid
        self.tablename: str = tablename
        self.time_retrieved: datetime.datetime = time_retrieved
        self.subtables: dict[str, str] = subtables
        self.variables: list[DelTabellType] = variables
        self.codelists: dict[str, KodelisteTypeParsed] = codelists
        self.suppression: list[SuppressionCodeListType] | None = suppression

    @classmethod
    def from_filbeskrivelse(
        cls: type[Self],
        filbeskrivelse: FilBeskrivelseType,
    ) -> Self:
        """Parses "filbeskrivelse" from Statbank API."""
        time_retrieved = datetime.datetime.strptime(  # noqa: DTZ007
            filbeskrivelse["Uttaksbeskrivelse_lagd"],
            DATETIME_FORMAT,
        )
        tableid = int(filbeskrivelse["TabellId"])
        tablename = filbeskrivelse["Huvudtabell"]
        subtables = {
            x["Filnavn"]: x["Filtext"] for x in filbeskrivelse["DeltabellTitler"]
        }
        variables = filbeskrivelse["deltabller"]

        codelists: dict[str, KodelisteTypeParsed] = {}
        kodelister = filbeskrivelse.get("kodelister", [])
        irkodelister = filbeskrivelse.get("IRkodelister", [])
        alle_kodelister = itertools.chain(kodelister, irkodelister)

        for kodeliste in alle_kodelister:
            codelist: KodelisteTypeParsed = {
                "koder": {kode["kode"]: kode["text"] for kode in kodeliste["koder"]},
            }
            if "SumIALtTotalKode" in kodeliste:
                codelist["SumIALtTotalKode"] = kodeliste["SumIALtTotalKode"]

            codelists[kodeliste["kodeliste"]] = codelist

        if "null_prikk_missing_kodeliste" in filbeskrivelse:
            suppression = filbeskrivelse["null_prikk_missing_kodeliste"]
        else:
            suppression = None

        return cls(
            tableid=tableid,
            tablename=tablename,
            time_retrieved=time_retrieved,
            subtables=subtables,
            variables=variables,
            codelists=codelists,
            suppression=suppression,
        )

    @classmethod
    def from_mapping(cls: type[Self], json_object: _UttrekksBeskrivelseDump) -> Self:
        """Reads a mapping to recreate a instance of this class."""
        time_retrieved = datetime.datetime.strptime(  # noqa: DTZ007
            json_object["time_retrieved"],
            DATETIME_FORMAT,
        )

        return cls(
            tableid=int(json_object["tableid"]),
            tablename=json_object["tablename"],
            time_retrieved=time_retrieved,
            subtables=json_object["subtables"],
            variables=json_object["variables"],
            codelists=json_object["codelists"],
            suppression=json_object.get("suppression", None),
        )


class StatbankUttrekksBeskrivelse(StatbankAuth, StatbankUttrekkValidators):
    """Class for talking with the "uttrekksbeskrivelses-API", which describes metadata about shape of data to be transferred.

    And metadata about the table itself in Statbankens system,
    like ID, name of codelists etc.

    Attributes:
        url (str): Main url for transfer
        filbeskrivelse (dict): The "raw" json returned from the API-get-request, loaded into a dict.
        use_db (UseDb | str | None):
            If you are in PROD-dapla and want to send to statbank test-database, set this to "TEST".
            When sending from TEST-environments you can only send to TEST-db, so this parameter is then ignored.
            Be aware that metadata tends to be outdated in the test-database.

    """

    @overload
    def __init__(
        self: Self,
        tableid: str,
        raise_errors: bool = ...,
        headers: None = None,
        use_db: UseDb | Literal["TEST", "PROD"] | None = ...,
        *,
        data: None = None,
        config: StatbankConfig | None = ...,
        auth: requests.auth.AuthBase | None = ...,
    ) -> None: ...

    @overload
    def __init__(
        self: Self,
        tableid: None = None,
        raise_errors: bool = ...,
        headers: None = None,
        use_db: UseDb | Literal["TEST", "PROD"] | None = ...,
        *,
        data: UttrekksBeskrivelseData = ...,
        config: StatbankConfig | None = ...,
        auth: requests.auth.AuthBase | None = ...,
    ) -> None: ...

    @overload
    @deprecated("Headers parameter not used, use auth attribute to pass in auth")
    def __init__(
        self: Self,
        tableid: str,
        raise_errors: bool = ...,
        headers: dict[str, str] = ...,
        use_db: UseDb | Literal["TEST", "PROD"] | None = ...,
        *,
        data: UttrekksBeskrivelseData | None = ...,
        config: StatbankConfig | None = ...,
        auth: requests.auth.AuthBase | None = ...,
    ) -> None: ...

    def __init__(
        self: Self,
        tableid: str | int | None = None,
        raise_errors: bool = False,
        headers: dict[str, str] | None = None,  # noqa: ARG002
        use_db: UseDb | Literal["TEST", "PROD"] | None = None,
        *,
        data: UttrekksBeskrivelseData | None = None,
        config: StatbankConfig | None = None,
        auth: requests.auth.AuthBase | None = None,
    ) -> None:
        """Makes a request to the Statbank-API, populates the objects attributes with parts of the return values."""
        super().__init__(use_db, config=config, auth=auth)
        self.url = self._build_urls()["uttak"]
        self.raise_errors = raise_errors
        if data is None:
            if tableid is None:
                error_message = "Må inialiseres med tabellnummer, tabellnavn, eller UttrekksBeskrivelseData"
                raise ValueError(error_message)
            filbeskrivelse = self._get_uttrekksbeskrivelse(tableid)
            self.filbeskrivelse = filbeskrivelse
            data = UttrekksBeskrivelseData.from_filbeskrivelse(filbeskrivelse)

        self._data: UttrekksBeskrivelseData = data

    @property
    def tableid(self: Self) -> str:
        """Originally the ID of the main table, which to get the Uttrekksbeskrivelse on."""
        return str(self._data.tableid)

    @property
    def time_retrieved(self: Self) -> str:
        """Time of getting the Uttrekksbeskrivelse."""
        return self._data.time_retrieved.strftime(DATETIME_FORMAT)

    @property
    def tablename(self: Self) -> str:
        """The name of the main table in Statbanken, not numbers, like the ID is."""
        return self._data.tablename

    @property
    def subtables(self: Self) -> dict[str, str]:
        """Names and descriptions of the individual "table-parts" that needs to be sent in as different DataFrames."""
        return self._data.subtables

    @property
    def variables(self: Self) -> list[DelTabellType]:
        """Metadata about the columns in the different table-parts."""
        return self._data.variables

    @property
    def codelists(self: Self) -> dict[str, KodelisteTypeParsed]:
        """Metadata about column-contents, like formatting on time, or possible values ("codes")."""
        return self._data.codelists

    @property
    def suppression(self: Self) -> list[SuppressionCodeListType] | None:
        """Details around extra columns which describe main column's "prikking", meaning their suppression-type."""
        return self._data.suppression

    @property
    @deprecated("Always none. Authinfo is not stored here anymore")
    def headers(self: Self) -> None:
        """Deprecated attribute, Authinfo is not stored here anymore."""

    def __str__(self) -> str:
        """Returns a string representation of the object, which is the Uttrekksbeskrivelse."""
        variabel_text = ""
        for i, deltabell in enumerate(self.variables):
            variabel_text += f"""\nDeltabell (DataFrame) nummer {i + 1}:
                {deltabell["deltabell"]}
                """
            variables: list[
                KolonneVariabelType
                | KolonneStatistikkvariabelType
                | KolonneInternasjonalRapporteringType
                | SuppressionDeltabellCodeListType
            ] = [*deltabell["variabler"], *deltabell["statistikkvariabler"]]
            if "null_prikk_missing" in deltabell:
                variables += deltabell["null_prikk_missing"]
            if "internasjonal_rapportering" in deltabell:
                variables += deltabell["internasjonal_rapportering"]

            variabel_text += f"Antall kolonner: {len(variables)}"
            for j, variabel in enumerate(variables):
                variabel_text += f"\n\tKolonne {j + 1}: "
                variabel_text += str(variabel.get("Kodeliste_text", ""))
                variabel_text += str(variabel.get("Text", ""))
                supp = variabel.get("gjelder_for_text", "")
                if supp:
                    variabel_text += f"Suppressionfo column {variabel.get('gjelder_for__kolonner_nummer')}: {supp}"
            variabel_text += f"\nEksempellinje: {deltabell['eksempel_linje']}"

        mult_codelists = math.prod([len(x["koder"]) for x in self.codelists.values()])
        variabel_text += f'\n"Ekspandert matrise/antall koder i kodelistene ganget med hverandre er: {mult_codelists}'

        return f"""Uttrekksbeskrivelse for statbanktabell {self.tableid}.

        Hele filbeskrivelsen "rå" ligger under .filbeskrivelse
        Andre attributter:
        .subtables, .codelists, .suppression, .variables
{variabel_text}
        """

    def __repr__(self) -> str:
        """Return a string representation of how to instantiate this object again."""
        return f'StatbankUttrekksBeskrivelse(tableid="{self.tableid}",)'

    def transferdata_template(
        self,
        dfs: list[pd.DataFrame] | None = None,
    ) -> dict[str, str] | dict[str, pd.DataFrame]:
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
        if dfs is not None:
            if isinstance(dfs, pd.DataFrame):  # type: ignore[unreachable]
                dfs = [dfs]  # type: ignore[unreachable]
            if not all(isinstance(df, pd.DataFrame) for df in dfs):
                error_msg = "All elements sent in to transferdata_template must be pandas dataframes."
                raise TypeError(error_msg)
            if len(dfs) != len(self.subtables):
                error_msg = (
                    "Number of dataframes in must match the number of subtables."
                )
                raise KeyError(error_msg)
            template = {k: dfs[i] for i, k in enumerate(self.subtables.keys())}
            msg = "{\n"
            for k, v in template.items():
                msg += f'"{k}" : Dataframe with column-names: {v.columns}\n'
            msg += "}"
            logger.info(msg)
            return template
        non_df_template = {k: f"df{i}" for i, k in enumerate(self.subtables.keys())}
        logger.info(
            """Your template should look like this: %s
            You can also send in a list of dataframes to this function, and get a dict back, but check the order!""",
            non_df_template,
        )
        return non_df_template

    @overload
    def to_json(self, path: str | Path | WritablePath) -> None: ...

    @overload
    def to_json(self, path: None) -> str: ...

    def to_json(self, path: str | Path | WritablePath | None = None) -> None | str:
        """Store a copy of the current state of the uttrekk-object as a json.

        If path is provided, tries to write to it,
        otherwise will return a json-string for you to handle like you wish.

        Args:
            path (str): if provided, will try to write a json to a local path

        Returns:
            None | str: If path is provided, tries to write a json to a file and returns nothing.
                If path is not provided, returns the json-string for you to handle as you wish.
        """
        content = self.to_mapping()

        if path:
            if isinstance(path, str):
                path = Path(path)

            logger.info("Writing to %s", path)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(content))
            return None

        return json.dumps(content)

    def validate(
        self,
        data: dict[str, pd.DataFrame],
        raise_errors: bool = False,
        ignore_codes_not_in_data: bool = False,
    ) -> dict[str, ValueError]:
        """Uses the contents of itself to validate the data against.

        All validation happens locally, so dont be afraid of any data
        being sent to statbanken using this method.

        Args:
            data (dict[str, pd.DataFrame]): The data to validate in a dictionary of deltabell-names as keys and pandas-dataframes as values.
            raise_errors (bool): True/False based on if you want the method to raise its own errors or not.
            ignore_codes_not_in_data (bool): If set to True, will hide messages about codes in klass that are missing from the data.

        Returns:
            dict[str, ValueError]: A dictionary of the errors the validation wants to raise.

        Raises:
            StatbankValidateError: if raise_errors is set to True and there are validation errors.
        """
        if not raise_errors:
            raise_errors = self.raise_errors

        validation_errors: dict[str, ValueError] = {}
        logger.info("validating...")

        self._validate_number_dataframes(data=data)
        validation_errors = self._validate_number_columns(
            data,
            validation_errors,
        )
        validation_errors = self._category_columns_are_strings(data, validation_errors)
        validation_errors = self._category_code_usage(
            data,
            validation_errors,
            ignore_codes_not_in_data,
        )
        validation_errors = self._check_for_floats(data, validation_errors)
        validation_errors = self._check_for_literal_nans_in_strings(
            data,
            validation_errors,
        )
        validation_errors = self._check_statistikkvar_numerical(data, validation_errors)
        validation_errors = self._check_rounding(data, validation_errors)
        validation_errors = self._check_time_same_values_in_deltabeller(
            data,
            validation_errors,
        )
        validation_errors = self._check_time_formats(data, validation_errors)
        validation_errors = self._check_suppression(data, validation_errors)
        validation_errors = self._check_unique_combinations_categories_times(
            data,
            validation_errors,
        )

        if raise_errors and validation_errors:
            raise StatbankValidateError(list(validation_errors.values()))
        logger.info(
            "validation finished (if nothing is logged over info level, everything should be fine.)",
        )
        return validation_errors

    def get_totalcodes_dict(self) -> dict[str, str]:
        """Makes a dict from each codelist where a code for "totals" is included.

        Keys being the name of the codelist, values being the code to put into categorical columns, that describes totals.
        This dict can be passed into the parameters "fillna_dict" and "grand_total" in the function "agg_all_combos" in the package ssb-fagfunksjoner.

        Returns:
            dict[str, str]: A dictionary with the codelist-names as keys, the total-codes as values.
        """
        result: dict[str, str] = {}
        for name, kodeliste in self.codelists.items():
            if "SumIALtTotalKode" in kodeliste:
                result[name] = kodeliste["SumIALtTotalKode"]
        return result

    def round_data(
        self,
        data: dict[str, pd.DataFrame],
        round_up: bool = True,
    ) -> dict[str, pd.DataFrame]:
        """Converts all decimal numbers to strings, with the correct number of decimals.

        IMPORTANT: Rounds "real halves" (0.5) UP, instead of "to even numbers" like Python does by default.
        This is maybe the behaviour staticians are used to from Excel, SAS etc.

        Args:
            data (dict[str, pd.DataFrame]): The data to validate in a dictionary of deltabell-names as keys and pandas-dataframes as values.
            round_up (bool): Default behaviour is rounding up like Excel or SAS. Setting this to False will instead use Python's default "Round towards equal" / "Banker's rounding"

        Returns:
            dict[str, pd.DataFrame]: A dictionary in the same shape as sent in, but with dataframes altered to correct for rounding.
        """
        data_copy = copy.deepcopy(data)
        for deltabell in self.variables:
            deltabell_name = deltabell["deltabell"]
            for variabel in deltabell["statistikkvariabler"]:
                if "Antall_lagrede_desimaler" in variabel:
                    col_num = int(variabel["kolonnenummer"]) - 1
                    decimal_num = int(variabel["Antall_lagrede_desimaler"])
                    # Nan-handling?
                    if (
                        "float"
                        in str(data_copy[deltabell_name].dtypes.iloc[col_num]).lower()
                    ):  # If column is passed in as a float, we can handle it
                        logger.info(
                            "Rounding column %s in %s into a string, with %s decimals.",
                            col_num + 1,
                            deltabell_name,
                            decimal_num,
                        )
                        data_copy[deltabell_name][
                            data_copy[deltabell_name].columns[col_num]
                        ] = (
                            data_copy[deltabell_name]
                            .iloc[:, col_num]
                            .astype("Float64")
                            .apply(self._round, decimals=decimal_num, round_up=round_up)
                            .astype(str)
                            .str.replace("<NA>", "", regex=False)
                            .str.replace(".", ",", regex=False)
                        )
                    else:
                        logger.info(
                            "not a float %s: %s",
                            col_num,
                            str(data_copy[deltabell_name].dtypes.iloc[col_num]),
                        )
        return data_copy

    @staticmethod
    def _round(
        n: float | str | pd.api.typing.NAType,
        decimals: int = 0,
        round_up: bool = True,
    ) -> str:
        if pd.isna(n):
            result: str = ""
        elif round_up and decimals and (n or n == 0):
            with localcontext() as ctx:
                ctx.rounding = ROUND_HALF_UP
                result = str(round(Decimal(n), decimals))
        elif not round_up and decimals and (n or n == 0):
            result = str(round(Decimal(n), decimals))
        elif n or n == 0:
            result = str(Decimal(n).to_integral_value())
        else:
            result = str(n)
        return result

    def _get_uttrekksbeskrivelse(
        self: Self,
        tableid_or_number: str | int,
    ) -> FilBeskrivelseType:
        filbeskrivelse_response = self._make_request(
            self.url,
            {"tableId": tableid_or_number},
        )

        # Rakel encountered an error with a tab-character in the json, should we just strip this?
        filbeskrivelse_json = filbeskrivelse_response.text.replace("\t", "")
        # Also deletes / overwrites returned Auth-header from get-request
        filbeskrivelse: FilBeskrivelseType = json.loads(filbeskrivelse_json)
        logger.info(
            "Hentet uttaksbeskrivelsen for %s, med tableid: %s den %s",
            filbeskrivelse["Huvudtabell"],
            tableid_or_number,
            str(filbeskrivelse["Uttaksbeskrivelse_lagd"]),
        )
        if self.use_db == UseDb.TEST:
            err_msg = "Metadata i TEST-databasen kan være veldig utdatert. Kan hende du bør hente filbeskrivelsen / description fra PROD-databasen?"
            logger.warning(err_msg)

        return filbeskrivelse

    def _make_request(self, url: furl, params: dict[str, str | int]) -> r.Response:
        response = r.get(
            url.url,
            params=params,
            headers=self._build_headers(),
            auth=self._auth,
            timeout=10,
        )
        try:
            response.raise_for_status()
        except r.HTTPError:
            logger.error(response.text)
            raise
        return response

    @classmethod
    def from_mapping(cls: type[Self], json_object: _UttrekksBeskrivelseDump) -> Self:
        """Reads a mapping to recreate a instance of this class."""
        auth = requests.auth.AuthBase()
        use_db = json_object["use_db"]
        raise_errors = json_object["raise_errors"]
        data = UttrekksBeskrivelseData.from_mapping(json_object)
        return cls(raise_errors=raise_errors, data=data, use_db=use_db, auth=auth)

    def to_mapping(self: Self) -> _UttrekksBeskrivelseDump:
        """Create a mapping of this object that can be written as JSON."""
        json_object: _UttrekksBeskrivelseDump = {
            "tableid": self.tableid,
            "tablename": self.tablename,
            "use_db": self.use_db.value,
            "raise_errors": self.raise_errors,
            "time_retrieved": self.time_retrieved,
            "codelists": self.codelists,
            "variables": self.variables,
            "subtables": self.subtables,
            "suppression": self.suppression,
        }

        return json_object
