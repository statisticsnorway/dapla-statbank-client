#!/usr/bin/env python3

from .auth import StatbankAuth

from requests.exceptions import ConnectionError
import pandas as pd
import requests as r
import json

class StatbankUttrekksBeskrivelse(StatbankAuth):
    """
    Class for talking with the "uttrekksbeskrivelses-API", which describes metadata about shape of data to be transferred.
    And metadata about the table itself in Statbankens system, like ID, name of codelists etc.
    ...

    Attributes
    ----------
    loaduser : str
        Username for Statbanken, not the same as "tbf" or "common personal username" in other SSB-systems
    url : str
        Main url for transfer
    lagd : str
        Time of getting the Uttrekksbeskrivelse
    tabellid: str
        Originally the ID of the main table, which to get the Uttrekksbeskrivelse on, 
        but is reset based on the info in the Uttrekksbeskrivelse. 
        To compansate for the possibility of the user sending in "Hovedtabell"-name as tabellid.
        That should work also, probably...
    hovedtabell : str
        The name of the main table in Statbanken, not numbers, like the ID is.
    deltabelltitler : dict
        Names and descriptions of the individual "table-parts" that need to be sent in as different DataFrames.
    variabler : dict
        Metadata about the columns in the different table-parts.
    kodelister : dict
        Metadata about column-contents, like formatting on time, or possible values ("codes").
    prikking : dict
        Details around extra columns which describe main column's "prikking", meaning their suppression-type. 
    headers : dict
        The headers for the request, might be sent in from a StatbankTransfer-object.
    filbeskrivelse : dict
        The "raw" json returned from the API-get-request, loaded into a dict.
    
    Methods
    -------
    validate_dfs(data=pd.DataFrame, raise_errors=bool):
        Checks sent data against UttrekksBeskrivelse, raises errors at end of checking, if raise_errors not set to False.
    _get_uttrekksbeskrivelse():
        Handles the response from the API, prints some status.
    _make_request():
        Makes the actual get-request, split out for easier mocking
    _split_attributes():
        After a successful response, split recieved data into attributes for easier access
    __init__():
    
    """
    def __init__(self, tabellid, loaduser, raise_errors = False, headers=None):
        self.loaduser = loaduser
        self.url = self._build_urls()['uttak']
        self.lagd = ""
        self.tabellid = tabellid
        self.raise_errors = raise_errors
        self.hovedtabell = ""
        self.deltabelltitler = dict()
        self.variabler = dict()
        self.kodelister = dict()
        self.prikking = None
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
        for i, deltabell in enumerate(self.variabler):
            variabel_text += f'\nDeltabell (DataFrame) nummer {i+1}: {deltabell["deltabell"]}\n'
            variabler = [*deltabell["variabler"], *deltabell["statistikkvariabler"]]
            if 'null_prikk_missing' in deltabell.keys():
                variabler += deltabell['null_prikk_missing']
            variabel_text += f'Antall kolonner: {len(variabler)}'
            for i, variabel in enumerate(variabler):
                variabel_text += f'\n\tKolonne {i+1}: '
                if "Kodeliste_text" in variabel.keys():
                    variabel_text += variabel["Kodeliste_text"]
                elif "Text" in variabel.keys():
                    variabel_text += variabel["Text"]
                elif 'gjelder_for_text' in variabel.keys():
                    variabel_text += f'Prikking for kolonne {variabel["gjelder_for__kolonner_nummer"]}: {variabel["gjelder_for_text"]}'
            variabel_text += f'\nEksempellinje: {deltabell["eksempel_linje"]}'
            
        return f'''Uttrekksbeskrivelse for statbanktabell {self.tabellid}. 
        loaduser: {self.loaduser}. 
        
        Hele filbeskrivelsen "rå" ligger under .filbeskrivelse
        Andre attributter: 
        .deltabelltitler, .kodelister, .prikking, .variabler
{variabel_text}
        '''
        
    def __repr__(self):
        return f'StatbankUttrekksBeskrivelse(tabellid="{self.tabellid}", loaduser="{self.loaduser}")'
    
    def to_json(self, path: str = "") -> dict:
        """If path is provided, tries to write to it, 
        otherwise will return a json-string for you to handle like you wish.
        """
        if path:
            print(f'Writing to {path}')
            with open(path, mode="w") as json_file:
                json_file.write(json.dumps(self.__dict__))
        else:
            return json.dumps(self.__dict__)
    
    def validate_dfs(self, data, raise_errors: bool = False) -> dict:
        if not raise_errors:
            raise_errors = self.raise_errors
        
        validation_errors = {}
        print("\nvalidating...")
        ### Number deltabelltitler should match length of data-iterable
        if len(self.deltabelltitler) > 1:
            if not isinstance(data, list) or not isinstance(data, tuple):
                raise TypeError(f"""Please put multiple pandas Dataframes in a list as your data.
                These are your 'deltabeller', and the number & order of DataFrames should match this: 
                {self.deltabelltitler}
                """)
        elif len(self.deltabelltitler) == 1:
            if not isinstance(data, pd.DataFrame):
                raise TypeError("Only one deltabell, expecting one pandas Dataframe in as your data.")
            # For the code below to access the data correctly
            to_validate = [data]
        else:
            validation_errors["deltabell_num"] = ValueError("Deltabeller is shorter than one, weird. Make sure the uttaksbeskrivelse is ok.")
        if "deltabell_num" not in validation_errors.keys():
            print("Correct number of DataFrames passed.")

        ### Number of columns in data must match beskrivelse
        for deltabell_num, deltabell in enumerate(self.variabler):
            deltabell_navn = deltabell['deltabell']
            col_num = len(deltabell['variabler']) + len(deltabell['statistikkvariabler']) # Mangler prikke-kolonner?
            if "null_prikk_missing" in deltabell.keys():
                col_num += len(deltabell["null_prikk_missing"])
            if len(to_validate[deltabell_num].columns) != col_num:
                validation_errors[f"col_count_data_{deltabell_num}"] = ValueError(f"Expecting {col_num} columns in datapart {deltabell_num}: {deltabell_navn}")
        for k in validation_errors.keys():
            if "col_count_data" in k:
                  break
        else:
              print("Correct number of columns...")
        
        ### No values outside, warn of missing from codelists on categorical columns
        categorycode_outside = []
        categorycode_missing = []
        for kodeliste in self.kodelister:
            kodeliste_id = kodeliste['kodeliste']
            for deltabell in self.variabler:
                for i, deltabell2 in enumerate(self.deltabelltitler):
                    if deltabell2["Filnavn"] == deltabell["deltabell"]:
                        deltabell_nr = i + 1
                for variabel in deltabell["variabler"]:
                    if variabel["Kodeliste_id"] == kodeliste_id:
                        break
                else:
                    raise KeyError(f"Can't find {kodeliste_id} among deltabells variables.")
            #if 'SumIALtTotalKode' in kodeliste.keys():
                #print(kodeliste["SumIALtTotalKode"])
            col_unique = to_validate[deltabell_nr-1].iloc[:,int(variabel["kolonnenummer"])-1].unique()
            kod_unique = [i["kode"] for i in kodeliste['koder']]
            for kod in col_unique:
                if kod not in kod_unique:
                    categorycode_outside += [f"Code {kod} in data, but not in uttrekksbeskrivelse, add to statbank admin? From column number {variabel['kolonnenummer']}, in deltabell number {deltabell_nr}, ({deltabell['deltabell']})"]
            for kod in kod_unique:
                if kod not in col_unique:
                    categorycode_missing += [f"Code {kod} missing from column number {variabel['kolonnenummer']}, in deltabell number {deltabell_nr}, ({deltabell['deltabell']})"]
                    
        ### Check rounding on floats? And correct decimal
        
        ### Check formatting on time?
        if categorycode_outside:
            print("Codes in data, outside codelist:")
            print("\n".join(categorycode_outside))
            print()
            validation_errors["categorycode_outside"] = ValueError(categorycode_outside)
        else:
            print("No codes in categorical columns outside codelist.")
        if categorycode_missing:
            print("Category codes missing from data (This is ok, just make sure missing data is intentional):")
            print("\n".join(categorycode_missing))
            print()
        else:
            print("No codes missing from categorical columns.")
        
        
        # Time-columns should follow time format
        for i, deltabell in enumerate(self.variabler):
            for variabel in deltabell['variabler']:
                if 'Kodeliste_text' in variabel.keys():
                    if "format = " in variabel["Kodeliste_text"]:
                        col_num = int(variabel['kolonnenummer']) - 1
                        timeformat_raw = variabel["Kodeliste_text"].split(" format = ")[1].strip().replace("Å", "å")
                        # Check length of coloumn matches length of format
                        if not 1 == len(to_validate[i].iloc[:,col_num].astype(str).str.len().unique()):
                            validation_errors[f'time_single_length_format_{col_num}'] = ValueError(f'Column number {col_num} does not have a single time format in the shape: {timeformat_raw}')
                        if not len(timeformat_raw) == to_validate[i].iloc[:,col_num].astype(str).str.len().unique()[0]:
                            validation_errors[f'time_formatlength_{col_num}'] = ValueError(f'Column number {col_num} does not match time format in the shape: {timeformat_raw}')
                        
                        timeformat = {
                            "nums" : [i for i, c in enumerate(timeformat_raw) if c.islower()],
                            "chars" : {i:c for i, c in enumerate(timeformat_raw) if c.isupper()},
                            "specials": {i:c for i, c in enumerate(timeformat_raw) if not c.isalnum()}
                        }
                        
                        #print(timeformat)
                        if timeformat['nums']:
                            for num in timeformat['nums']:
                                if not all(to_validate[i].iloc[:,col_num].str[num].str.isdigit()):
                                    validation_errors[f'time_non_digit_column{col_num}'] = ValueError(f'Character number {num} in column {col_num} in DataFrame {i}, does not match format {timeformat_raw}')
                        if timeformat['chars']:
                            for i, char in timeformat['chars'].items():
                                if not all(to_validate[i].iloc[:,col_num].str[i] == char):
                                    validation_errors[f'character_match_column{col_num}'] = ValueError(f'Should be capitalized character? Character {char}, character number {num} in column {col_num} in DataFrame {i}, does not match format {timeformat_raw}')
                        if timeformat['specials']:
                            for i, special in timeformat['specials'].items():
                                if not all(to_validate[i].iloc[:,col_num].str[i] == special):
                                    validation_errors[f'special_character_match_column{col_num}'] = ValueError(f'Should be the special character {special}, character number {num} in column {col_num} in DataFrame {i}, does not match format {timeformat_raw}')
        for k in validation_errors.keys():
            if 'time_non_digit_column' in k:
                break
            elif 'character_match_column' in k:
                break
            elif 'special_character_match_column' in k:
                break
            elif 'time_single_length_format' in k:
                break
            elif 'time_formatlength' in k:
                break
        else:
            print("Timeformat validation ok.")

        
        # Check "prikking"-columns for codes outside the codelist, allow empty values ""
        if self.prikking:
            prikk_codes = [code['Kode'] for code in self.prikking]
            prikk_codes += [""]
            for i, deltabell in enumerate(self.variabler):
                if 'null_prikk_missing' in deltabell.keys():
                    for prikk_col in deltabell['null_prikk_missing']:
                        col_num = int(prikk_col['kolonnenummer']) - 1
                        if not all(to_validate[i].iloc[:,col_num].isin(prikk_codes)):
                            validation_errors[f'prikke_character_match_column{col_num}'] = ValueError(f'Prikke-code not among allowed prikkecodes: {prikk_codes}, in column {col_num} in DataFrame {i}.')
        for k in validation_errors.keys():
            if 'prikke_character_match_column' in k:
                break
        else:
            print("Prikking-codes validation ok / No prikke-columns in use.")
        
        
        if raise_errors and validation_errors:
            raise Exception(list(validation_errors.values()))
        print()
        
        return validation_errors
    

        
    def _get_uttrekksbeskrivelse(self) -> dict:
        filbeskrivelse_url = self.url+"tableId="+self.tabellid
        try:
            filbeskrivelse = self._make_request(filbeskrivelse_url, self.headers)
        finally:
            if hasattr(self, "headers"):
                del self.headers
        #print(filbeskrivelse.text)
        if filbeskrivelse.status_code != 200:
            raise ConnectionError(filbeskrivelse, filbeskrivelse.text)
        # Also deletes / overwrites returned Auth-header from get-request
        filbeskrivelse = json.loads(filbeskrivelse.text)
        print(f"""Hentet uttaksbeskrivelsen for {filbeskrivelse['Huvudtabell']}, 
        med tabellid: {self.tabellid}
        den {filbeskrivelse['Uttaksbeskrivelse_lagd']}""")
        
        # reset tabellid and hovedkode after content of request
        self.filbeskrivelse = filbeskrivelse
        
    def _make_request(self, url: str, header: dict):
        return r.get(url, headers=self.headers)
    
    def _split_attributes(self) -> None:
        # Tabellid might have been "hovedkode" up to this point, as both are valid in the URI
        self.lagd = self.filbeskrivelse['Uttaksbeskrivelse_lagd']
        self.tabellid = self.filbeskrivelse['TabellId']
        self.hovedtabell = self.filbeskrivelse['Huvudtabell']
        self.deltabelltitler = self.filbeskrivelse['DeltabellTitler']
        self.variabler = self.filbeskrivelse['deltabller']
        self.kodelister = self.filbeskrivelse['kodelister']
        if 'null_prikk_missing_kodeliste' in self.filbeskrivelse.keys():
            self.prikking = self.filbeskrivelse['null_prikk_missing_kodeliste']
