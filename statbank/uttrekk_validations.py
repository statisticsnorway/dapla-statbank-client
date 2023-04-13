import pandas as pd


class StatbankUttrekkValidators:
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

    def _check_unique_combinations_categories(
        self, data, validation_errors: dict, printing
    ) -> dict:

        # Get column-numbers containing categorical values per deltabell
        for deltabell in self.variables:
            category_col_nums = [
                int(var["kolonnenummer"]) - 1 for var in deltabell["variabler"]
            ]
            df_colcheck = data[deltabell["deltabell"]].iloc[category_col_nums, :]
            if df_colcheck.duplicated().any():
                validation_errors[
                    f"duplicate_categorical_groups_{deltabell['deltabell']}"
                ] = ValueError(
                    f"There seems to be duplicate rows across the categorical values (including time) in deltabell {deltabell['deltabell']}."
                )
        for k in validation_errors.keys():
            if "duplicate_categorical_groups" in k:
                break
        else:
            if printing:
                print("Found no duplicate combinations of categorical columns")

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
                    # Compensate for na / empty cells
                    column = column[column != ""]
                    # If there are no rows left after removing empty, skip checking more
                    if not len(column):
                        break
                    if decimal_num:
                        if any(decimal_num != column.str.split(",").str[-1].str.len()):
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
                        validation_errors[
                            f"rounding_error_{deltabell_name}_{col_num}"
                        ] = ValueError(error_text)
                        if printing:
                            print(error_text)
        return validation_errors
