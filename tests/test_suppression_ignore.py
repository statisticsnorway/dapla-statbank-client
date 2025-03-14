import pandas as pd
import pytest

from statbank import StatbankClient
from statbank.uttrekk_validations import StatbankValidateError


def test_validate_suppressed_cells():
    filbesk = fake_uttrekksbeskrivelse_successful()
    data = fake_data_to_validate()

    filbesk.validate(data, raise_errors=True)


def test_validate_raises_on_literal_nans_in_suppression():
    filbesk = fake_uttrekksbeskrivelse_successful()
    data = fake_data_to_validate()
    data["konflikt1.dat"]["Suppression_TapteArbeidsdagar"] = pd.Series(
        ["01", None, ":", "nan"],
    )
    with pytest.raises(StatbankValidateError):
        filbesk.validate(data, raise_errors=True)


def test_validate_raises_on_literal_nans_data():
    filbesk = fake_uttrekksbeskrivelse_successful()
    data = fake_data_to_validate()
    data["konflikt1.dat"]["Arbeidstakarar"] = pd.Series(
        ["nan", "nan", "", None],
    )  # The last 3 are suppressed, so should raise error because of the first one...
    with pytest.raises(StatbankValidateError):
        filbesk.validate(data, raise_errors=True)


def test_validate_raises_on_literal_empty():
    filbesk = fake_uttrekksbeskrivelse_successful()
    data = fake_data_to_validate()
    data["konflikt1.dat"]["Arbeidstakarar"] = pd.Series(
        ["", "nan", "", None],
    )  # The last 3 are suppressed, so should raise error because of the first one...
    with pytest.raises(StatbankValidateError):
        filbesk.validate(data, raise_errors=True)


def test_validate_raises_on_na():
    filbesk = fake_uttrekksbeskrivelse_successful()
    data = fake_data_to_validate()
    data["konflikt1.dat"]["Arbeidstakarar"] = pd.Series(
        [pd.NA, "nan", "", None],
    )  # The last 3 are suppressed
    # Will not raise an error, because pd.NA is what we "remove suppressed with"... So the question remains if this is a validation error or not...
    # Meaning, if a cell is not suppressed, should it be allowed to be empty? Yes?
    filbesk.validate(data, raise_errors=True)


def test_validate_raises_on_wrong_rounding():
    filbesk = fake_uttrekksbeskrivelse_successful()
    data = fake_data_to_validate()
    data["konflikt1.dat"]["Arbeidstakarar"] = pd.Series(
        [100, 100.22, "", None],
    )  # Should ignore the one with the wrong rounding? Should be 0 decimals
    filbesk.validate(data, raise_errors=True)


def fake_uttrekksbeskrivelse_successful():
    return StatbankClient.read_description_json(
        """{
        "use_db": "PROD",
        "url": "https://i.test.ssb.no/statbank/sos/v1/uttaksbeskrivelse?",
        "time_retrieved": "21.01.2025 klokka 08:42",
        "tableid": "03629",
        "raise_errors": true,
        "tablename": "Konflikt",
        "subtables": {
        "konflikt1.dat": "03629: Arbeidskonfliktar"
        },
        "variables": [
        {
            "deltabell": "konflikt1.dat",
            "variabler": [
            {
                "kolonnenummer": "1",
                "Klassifikasjonsvariabel": "Tid",
                "Variabeltext": "tid",
                "Kodeliste_id": "-",
                "Kodeliste_text": "Tidsperioden for tabelldataene, enhet = år, format = åååå"
            }
            ],
            "statistikkvariabler": [
            {
                "kolonnenummer": "2",
                "Text": "Arbeidskonfliktar",
                "Enhet": "konfliktar",
                "Antall_lagrede_desimaler": "0",
                "Antall_viste_desimaler": "0"
            },
            {
                "kolonnenummer": "3",
                "Text": "Arbeidstakarar i arbeidskonflikt",
                "Enhet": "personar",
                "Antall_lagrede_desimaler": "0",
                "Antall_viste_desimaler": "0"
            },
            {
                "kolonnenummer": "4",
                "Text": "Tapte arbeidsdagar",
                "Enhet": "dagar",
                "Antall_lagrede_desimaler": "0",
                "Antall_viste_desimaler": "0"
            }
            ],
            "null_prikk_missing": [
            {
                "kolonnenummer": "5",
                "gjelder_for_text": "Konflikter",
                "gjelder_for__kolonner_nummer": "2"
            },
            {
                "kolonnenummer": "6",
                "gjelder_for_text": "Arbeidstakarar",
                "gjelder_for__kolonner_nummer": "3"
            },
            {
                "kolonnenummer": "7",
                "gjelder_for_text": "TapteArbeidsdagar",
                "gjelder_for__kolonner_nummer": "4"
            }
            ]
        }
        ],
        "suppression": [
        {
            "Kode": "01",
            "Vises_som": ".",
            "Beskrivelse": "Ikke mulig å oppgi tall. Tall finnes ikke."
        },
        {
            "Kode": "02",
            "Vises_som": "..",
            "Beskrivelse": "Tallgrunnlag mangler."
        },
        {
            "Kode": "04",
            "Vises_som": ":",
            "Beskrivelse": "Vises ikke av konfidensialitetshensyn."
        }
        ]
        }""",
    )


def fake_data_to_validate():
    return {
        "konflikt1.dat": pd.DataFrame(
            {
                "Tid": [2021, 2022, 2023, 2024],  # Year column
                "Arbeidskonfliktar": [10, 20, 30, 40],  # Column 2
                "Arbeidstakarar": [
                    100,
                    "",
                    None,
                    pd.NA,
                ],  # Column 3, values that are suppressed have been deleted for safety (should be possible and ignored)
                "Tapte arbeidsdagar": [1000, 2000, 3000, 4000],  # Column 4
                "Suppression_Arbeidskonfliktar": [
                    "01",
                    "02",
                    "04",
                    "",
                ],  # Column 5 (Suppression for col 2)
                "Suppression_Arbeidstakarar": [
                    "",
                    "02",
                    "04",
                    "01",
                ],  # Column 6 (Suppression for col 3)
                "Suppression_TapteArbeidsdagar": [
                    "01",
                    "",
                    "",
                    "",
                ],  # Column 7 (Suppression for col 4)
            },
        ),
    }
