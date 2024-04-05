from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from statbank.api_types import DelTabellType
    from statbank.api_types import KodelisteTypeParsed
    from statbank.api_types import KolonneStatistikkvariabelType
    from statbank.api_types import KolonneVariabelType
    from statbank.api_types import SuppressionCodeListType

import pandas as pd

from statbank.statbank_logger import logger


class StatbankValidateError(Exception):
    """Use when raising errors stemming from the validators not running cleanly."""


class StatbankUttrekkValidators:
    """Split out from the main Uttrekk-class, this class contains all the validator-methods."""

    def __init__(self) -> None:
        """This init will never be used directly, as this class is always inherited from.

        So, these attribute-settings are for type-checking with mypy.
        """
        self.subtables: dict[str, str] = {}
        self.variables: list[DelTabellType] = []
        self.codelists: dict[str, KodelisteTypeParsed] = {}
        self.suppression: None | list[SuppressionCodeListType] = None

    def _validate_number_dataframes(self, data: dict[str, pd.DataFrame]) -> None:
        # Number subtables should match length of data-iterable
        if len(self.subtables.values()) != len(data.values()):
            error_msg = f"""Please put one or more pandas Dataframes in a dict as your data. Keys in the dict should be "deltabell-navn": {self.subtables.keys()}"""
            raise TypeError(error_msg)
        for k, df in data.items():
            if not isinstance(df, pd.DataFrame):
                error_msg = f"{k}'s value is not a dataframe"  # type: ignore[unreachable]
                raise TypeError(error_msg)

    def _validate_number_columns(
        self,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        # Number of columns in data must match beskrivelse
        for deltabell_num, deltabell in enumerate(self.variables):
            deltabell_navn = deltabell["deltabell"]
            col_num = len(deltabell["variabler"]) + len(
                deltabell["statistikkvariabler"],
            )  # Mangler prikke-kolonner?
            if "null_prikk_missing" in deltabell:
                col_num += len(deltabell["null_prikk_missing"])
            if "internasjonal_rapportering" in deltabell:
                col_num += len(deltabell["internasjonal_rapportering"])
            if len(data[deltabell_navn].columns) != col_num:
                error_msg = f"""
                            EXPECTING {col_num} COLUMNS IN DATAFRAME NUMBER {deltabell_num}:
                            {deltabell_navn}
                            BUT FOUND {len(data[deltabell_navn].columns)}
                            """
                validation_errors[f"col_count_data_{deltabell_num}"] = ValueError(
                    error_msg,
                )
        for k in validation_errors:
            if "col_count_data" in k:
                logger.warning(validation_errors[k])
                break
        else:
            logger.debug("Correct number of columns...")
        return validation_errors

    def _check_for_literal_nans_in_strings(
        self,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        for name, df in data.items():
            string_df = df.select_dtypes(
                include=["object", "string", "string[pyarrow]"],
            )
            cat_df = df.select_dtypes(include=["category"])

            nans = ["nan", "na", "none", "."]
            validation_errors = self._check_for_literal_nans_string_dtype(
                string_df,
                name,
                nans,
                validation_errors,
            )
            validation_errors = self._check_for_literal_nans_categorical_dtype(
                cat_df,
                name,
                nans,
                validation_errors,
            )
        return validation_errors

    @staticmethod
    def _check_for_literal_nans_string_dtype(
        string_df: pd.DataFrame,
        name: str,
        nans: list[str],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        if len(string_df.columns):
            for col in string_df.columns:
                error_text = f"""{col} in {name} has strings, that look like NAs / empty cells,
                (In this list: {nans})
                Which have been converted to literal strings.
                Consider handeling your NAs before converting them to strings.
                Maybe with a .fillna("") before an .astype(str) """
                nan_len = len(string_df[string_df[col].str.lower().isin(nans)])
                if nan_len:
                    validation_errors[f"contains_string_nans_{name}_{col}"] = (
                        ValueError(error_text)
                    )
                    logger.warning(error_text)
        return validation_errors

    @staticmethod
    def _check_for_literal_nans_categorical_dtype(
        cat_df: pd.DataFrame,
        name: str,
        nans: list[str],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        if len(cat_df.columns):
            for col in cat_df.columns:
                error_text = f"""{col} in {name} is a categorical but has strings,
                that look like NAs / empty cells,
                (In this list: {nans})
                Which have been converted to literal strings?
                Consider handeling your NAs before converting them to strings.
                Maybe with a .fillna("") before an .astype(str) """
                nan_cats = [
                    cat for cat in cat_df[col].cat.categories if cat.lower() in nans
                ]
                if nan_cats:
                    validation_errors[
                        f"contains_string_nans_in_category_{name}_{col}"
                    ] = ValueError(error_text)
                    logger.warning(error_text)
        return validation_errors

    def _check_for_floats(
        self,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        for name, df in data.items():
            for col in df.columns:
                if "float" in str(df[col].dtype).lower():
                    error_text = f"""{col} in {name} is a float.
                    Consider running the dict of dataframes through:
                    data = uttrekksbeskrivelse.round_data(data),
                    this rounds UP like SAS and Excel, not to-even as
                    Python does otherwise."""
                    validation_errors[f"contains_floats_{name}_{col}"] = ValueError(
                        error_text,
                    )
                    logger.warning(error_text)
        return validation_errors

    def _check_time_formats(
        self,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        # Time-columns should follow time format
        for deltabell in self.variables:
            for variabel in deltabell["variabler"]:
                if "Kodeliste_text" in variabel and "format = " in variabel.get(
                    "Kodeliste_text",
                    "",
                ):
                    validation_errors = self._check_time_columns(
                        deltabell["deltabell"],
                        variabel,
                        data,
                        validation_errors,
                    )
        for k in validation_errors:
            matches = [
                "time_non_digit_column",
                "character_match_column",
                "special_character_match_column",
                "time_single_length_format",
                "time_formatlength",
            ]
            if [x for x in matches if x in k]:
                break
        else:
            logger.debug("Timeformat validation ok.")

        return validation_errors

    def _check_time_same_values_in_deltabeller(
        self,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        # Time-columns should have same values in all deltabeller

        for deltabell in self.variables:
            for variabel in deltabell["variabler"]:
                if "Kodeliste_text" in variabel and "format = " in variabel.get(
                    "Kodeliste_text",
                    "",
                ):
                    validation_errors = (
                        self._check_time_same_values_in_deltabeller_data(
                            deltabell,
                            variabel,
                            data,
                            validation_errors,
                        )
                    )

        return validation_errors

    @staticmethod
    def _check_time_same_values_in_deltabeller_data(
        deltabell: DelTabellType,
        variabel: KolonneVariabelType,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        found: list[str] = []
        col_num = int(variabel["kolonnenummer"]) - 1
        for del_data in data.values():
            unique_times = [str(x) for x in del_data.iloc[:, col_num].unique()]
            if found == []:
                found = unique_times
            else:
                times_missing = [x for x in found if x not in unique_times]
                times_extra = [x for x in unique_times if x not in found]
                if times_missing:
                    validation_errors[
                        f"time_missing_in_deltabell_{deltabell['deltabell']}"
                    ] = ValueError(
                        f"""Time(s) {times_missing} missing in deltabell {deltabell['deltabell']}""",
                    )
                if times_extra:
                    validation_errors[
                        f"time_extra_in_deltabell_{deltabell['deltabell']}"
                    ] = ValueError(
                        f"""Time(s) {times_extra} extra in deltabell {deltabell['deltabell']}""",
                    )
        return validation_errors

    def _check_time_columns(
        self,
        deltabell_name: str,
        variabel: KolonneVariabelType,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        col_num = int(variabel["kolonnenummer"]) - 1
        timeformat_raw = (
            variabel["Kodeliste_text"].split(" format = ")[1].strip().replace("Å", "å")
        )
        # Check length of coloumn matches length of format
        if (
            len(data[deltabell_name].iloc[:, col_num].astype(str).str.len().unique())
            != 1
        ):
            validation_errors[f"time_single_length_format_{col_num}"] = ValueError(
                f"""Column number {col_num} does not have
                a single time format
                in the shape: {timeformat_raw}""",
            )
        if (
            len(timeformat_raw)
            != data[deltabell_name].iloc[:, col_num].astype(str).str.len().unique()[0]
        ):
            validation_errors[f"time_formatlength_{col_num}"] = ValueError(
                f"""Column number {col_num} does not match
                time format in the shape: {timeformat_raw}""",
            )

        validation_errors = self._check_time_nums(
            timeformat_raw,
            deltabell_name,
            col_num,
            data,
            validation_errors,
        )
        validation_errors = self._check_time_chars(
            timeformat_raw,
            deltabell_name,
            col_num,
            data,
            validation_errors,
        )
        return self._check_time_specials(
            timeformat_raw,
            deltabell_name,
            col_num,
            data,
            validation_errors,
        )

    @staticmethod
    def _check_time_nums(
        timeformat_raw: str,
        deltabell_name: str,
        col_num: int,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        nums: list[int] = [i for i, c in enumerate(timeformat_raw) if c.islower()]
        if nums:
            for num in nums:
                if not all(
                    data[deltabell_name].iloc[:, col_num].str[num].str.isdigit(),
                ):
                    validation_errors[f"time_non_digit_column{col_num}"] = ValueError(
                        f"Character number {num} in column {col_num} in DataFrame {deltabell_name}, does not match format {timeformat_raw}",
                    )
        return validation_errors

    @staticmethod
    def _check_time_chars(
        timeformat_raw: str,
        deltabell_name: str,
        col_num: int,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        chars: dict[int, str] = {
            i: c for i, c in enumerate(timeformat_raw) if c.isupper()
        }
        if chars:
            for i, char in chars.items():
                if not all(data[deltabell_name].iloc[:, col_num].str[i] == char):
                    validation_errors[f"character_match_column{col_num}"] = ValueError(
                        f"Should be capitalized character? Character {char}, character number {i} in column {col_num} in DataFrame {deltabell_name}, does not match format {timeformat_raw}",
                    )
        return validation_errors

    @staticmethod
    def _check_time_specials(
        timeformat_raw: str,
        deltabell_name: str,
        col_num: int,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        specials: dict[int, str] = {
            i: c for i, c in enumerate(timeformat_raw) if not c.isalnum()
        }
        if specials:
            for i, special in specials.items():
                if not all(data[deltabell_name].iloc[:, col_num].str[i] == special):
                    validation_errors[f"special_character_match_column{col_num}"] = (
                        ValueError(
                            f"Should be the special character {special}, character number {i} in column {col_num} in DataFrame {deltabell_name}, does not match format {timeformat_raw}",
                        )
                    )
        return validation_errors

    def _check_suppression(
        self,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        if self.suppression:
            prikk_codes = [code["Kode"] for code in self.suppression]
            prikk_codes += [""]
            validation_errors = self._check_suppression_variables(
                data,
                prikk_codes,
                validation_errors,
            )
        for k in validation_errors:
            if "prikke_character_match_column" in k:
                break
        else:
            logger.debug(
                "suppression-codes validation ok / No prikke-columns in use.",
            )

        return validation_errors

    def _check_suppression_variables(
        self,
        data: dict[str, pd.DataFrame],
        prikk_codes: list[str],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        for deltabell in self.variables:
            deltabell_name = deltabell["deltabell"]
            if "null_prikk_missing" in deltabell:
                for prikk_col in deltabell["null_prikk_missing"]:
                    col_num = int(prikk_col["kolonnenummer"]) - 1
                    if not all(
                        data[deltabell_name].iloc[:, col_num].isin(prikk_codes),
                    ):
                        validation_errors[f"prikke_character_match_column{col_num}"] = (
                            ValueError(
                                f"Prikke-code not among allowed prikkecodes: {prikk_codes}, in column {col_num} in DataFrame {deltabell_name}.",
                            )
                        )
        return validation_errors

    def _check_unique_combinations_categories_times(
        self,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        # Get column-numbers containing categorical values per deltabell
        for deltabell in self.variables:
            category_col_nums = [
                int(var["kolonnenummer"]) - 1 for var in deltabell["variabler"]
            ]
            df_colcheck = data[deltabell["deltabell"]].iloc[:, category_col_nums]
            if df_colcheck.duplicated().any():
                validation_errors[
                    f"duplicate_categorical_time_groups_{deltabell['deltabell']}"
                ] = ValueError(
                    f"There seems to be duplicate rows across the categorical values (including time) in deltabell {deltabell['deltabell']}.",
                )
        for k in validation_errors:
            if "duplicate_categorical_time_groups" in k:
                break
        else:
            logger.debug(
                "Found no duplicate combinations of categorical columns",
            )

        return validation_errors

    def _get_check_codes(self) -> dict[str, dict[str, list[str]]]:
        check_codes: dict[str, dict[str, list[str]]] = {}
        for deltabell in self.variables:
            deltabell_navn = deltabell["deltabell"]
            check_codes[deltabell_navn] = {}
            for variabel in deltabell["variabler"]:
                if (
                    "Kodeliste_id" in variabel
                    and variabel.get("Kodeliste_id", "") != "-"
                ):
                    check_codes[deltabell_navn][variabel["kolonnenummer"]] = list(
                        self.codelists[variabel["Kodeliste_id"]]["koder"].keys(),
                    )
        return check_codes

    def _category_columns_are_strings(
        self,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        categorycode_not_string = []
        check_codes = self._get_check_codes()
        for deltabell_name, variabel in check_codes.items():
            for col_num in variabel:
                col = data[deltabell_name].iloc[:, int(col_num) - 1]
                if not pd.api.types.is_string_dtype(col):
                    categorycode_not_string += [
                        f"""{col_num} is a categorical column, but is not a string columns?
                            Convert the column to string before validating / transferring.
                            {col_num}, in deltabell {deltabell_name}""",
                    ]
        if categorycode_not_string:
            logger.warning("Codes in data, outside codelist:")
            logger.warning("\n".join(categorycode_not_string))
            validation_errors["categorycode_outside"] = ValueError(
                categorycode_not_string,
            )
        else:
            logger.debug(
                "All categorical columns are string, thats correct.",
            )
        return validation_errors

    def _category_code_usage(
        self,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:

        validation_errors = self._check_category_code_usage_outside(
            data,
            validation_errors,
        )
        return self._check_category_code_usage_missing(
            data,
            validation_errors,
        )

    def _check_category_code_usage_missing(
        self,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        categorycode_missing = []
        check_codes = self._get_check_codes()
        for deltabell_name, variabel in check_codes.items():
            for col_num, codelist in variabel.items():
                col = data[deltabell_name].iloc[:, int(col_num) - 1]
                col_unique = col.unique()

                for kod in codelist:
                    if kod not in col_unique:
                        categorycode_missing += [
                            f"""Code {kod} missing from column number
                            {col_num}, in deltabell {deltabell_name}""",
                        ]
        # No values outside, warn of missing from codelists on categorical columns
        if categorycode_missing:
            logger.info(
                """Category codes missing from data (This is ok,
            just make sure missing data is intentional):""",
            )
            logger.info("\n".join(categorycode_missing))
        else:
            logger.debug("No codes missing from categorical columns.")
        return validation_errors

    def _check_category_code_usage_outside(
        self,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        check_codes = self._get_check_codes()
        categorycode_outside = []
        for deltabell_name, variabel in check_codes.items():
            for col_num, codelist in variabel.items():
                col = data[deltabell_name].iloc[:, int(col_num) - 1]
                col_unique = col.unique()
                for kod in col_unique:
                    if kod not in codelist and " " in kod:
                        categorycode_outside += [
                            f"""{kod} contains spaces, should it?
                            The exact code "{kod}" (including spaces) is in the data, but not in uttrekksbeskrivelse,
                            add to statbank admin? From column number
                            {col_num}, in deltabell {deltabell_name}""",
                        ]
                    elif kod not in codelist:
                        categorycode_outside += [
                            f"""Code {kod} in data, but not in uttrekksbeskrivelse,
                            add to statbank admin? From column number
                            {col_num}, in deltabell {deltabell_name}""",
                        ]
        if categorycode_outside:
            logger.warning("Codes in data, outside codelist:")
            logger.warning("\n".join(categorycode_outside))
            validation_errors["categorycode_outside"] = ValueError(categorycode_outside)
        else:
            logger.debug(
                "No codes in categorical columns outside codelist.",
            )
        return validation_errors

    def _check_statistikkvar_numerical(
        self,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        for deltabell in self.variables:
            deltabell_name = deltabell["deltabell"]
            for variabel in deltabell["statistikkvariabler"]:
                col_num = int(variabel["kolonnenummer"]) - 1
                col = data[deltabell_name].iloc[:, col_num].copy()
                # Check if can be converted to float and int
                try:
                    if pd.api.types.is_string_dtype(col):
                        col = col.str.replace(",", ".", regex=False)
                    col.astype("Float64")
                except ValueError as e:
                    validation_errors[
                        f"statistikkvar_not_numerical_column{col_num}"
                    ] = ValueError(e)
        return validation_errors

    def _check_rounding(
        self,
        data: dict[str, pd.DataFrame],
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
        """If a column should have a set number of decimals, check if its a string, and how many places are used after the decimal seperator: ","."""
        for deltabell in self.variables:
            deltabell_name = deltabell["deltabell"]
            for variabel in deltabell["statistikkvariabler"]:
                validation_errors = self._check_rounding_per_variable(
                    variabel,
                    data,
                    deltabell_name,
                    validation_errors,
                )
        return validation_errors

    @staticmethod
    def _check_rounding_per_variable(
        variabel: KolonneStatistikkvariabelType,
        data: dict[str, pd.DataFrame],
        deltabell_name: str,
        validation_errors: dict[str, ValueError],
    ) -> dict[str, ValueError]:
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
        # Compensate for na / empty cells
        column = column[column != ""]
        # If there are no rows left after removing empty, skip checking more
        if not len(column):
            return validation_errors
        if decimal_num:
            col_decimals = column.str.split(",").str[-1].str.len()
            if (col_decimals != decimal_num).any():
                error = True
        elif not (
            column.str.replace("-", "", regex=False)
            .str.replace(",", "", regex=False)
            .str.replace(".", "", regex=False)
            .str.isdigit()
            .all()
        ):
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
            validation_errors[f"rounding_error_{deltabell_name}_{col_num}"] = (
                ValueError(error_text)
            )
            logger.warning(error_text)
        return validation_errors
