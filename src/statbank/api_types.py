from __future__ import annotations

from typing import TypedDict

from typing_extensions import NotRequired

#################################
# Statbank get public data type #
#################################


class QueryWholeType(TypedDict):
    """The whole query-type is used to get the data from Statbank."""

    query: list[QueryPartType]
    response: dict[str, str]


class QueryPartType(TypedDict):
    """This represents each part of the query sent to statbanken."""

    code: str
    selection: SelectionPartType


class SelectionPartType(TypedDict):
    """The selection-type is used to get the data from Statbank."""

    filter: str
    values: list[str]


####################
# TRANSFER RESULTS #
####################


class TransferPartType(TypedDict):
    """There are multiple copies of this for every json."""

    GeneratedId: None | str
    Status: str
    Message: str
    Exception: None | str
    ValidationInfoItems: None | str


class TransferResultType(TypedDict):
    """The types of the main result-json from a transfer."""

    TotalResult: TransferPartType
    ItemResults: list[TransferPartType]


#######################
# UTTREKKSBESKRIVELSE #
#######################


class SuppressionCodeListType(TypedDict):
    """The suppression-codes are used to filter out rows in the Uttrekksbeskrivelse, lower-level."""

    Kode: str
    Vises_som: str
    Beskrivelse: str


class SuppressionDeltabellCodeListType(TypedDict):
    """The suppression-codes are used to filter out rows in the Uttrekksbeskrivelse, top-level."""

    kolonnenummer: str
    gjelder_for_text: str
    gjelder_for__kolonner_nummer: str


class KodeType(TypedDict):
    """The code -> labels for the list in the Codelists."""

    kode: str
    text: str


class KodelisteType(TypedDict):
    """The codelists have their own name, which links from the column-metadata, and a list of code -> labels."""

    kodeliste: str
    SumIALtTotalKode: NotRequired[str]
    koder: list[KodeType]


class KodelisteTypeParsed(TypedDict):
    """Reorganizing the kodelists to this is done under init of Uttrekk."""

    SumIALtTotalKode: NotRequired[str]
    koder: dict[str, str]


class KolonneInternasjonalRapporteringType(TypedDict):
    """The international-report-variables are rarely used, but has other metadata than the other columns.."""

    kolonnenummer: str
    egenskap: str
    beskrivning: str
    Kodeliste_id: str
    Kodeliste_text: str


class KolonneStatistikkvariabelType(TypedDict):
    """The statistic-variables are mostly the numerical variables in statbanken.

    This class describes the types of the metadata about these columns in the Uttrekksbeskrivelse.
    """

    kolonnenummer: str
    Text: str
    Enhet: str
    Antall_lagrede_desimaler: str
    Antall_viste_desimaler: str


class KolonneVariabelType(TypedDict):
    """The Variables are mostly the classification variables in statbanken.

    This class describes the types of the metadata about these columns in the Uttrekksbeskrivelse.
    """

    kolonnenummer: str
    Klassifikasjonsvariabel: str
    Variabeltext: str
    Kodeliste_id: str
    Kodeliste_text: str


class DelTabellType(TypedDict):
    """A typed subpart of the returned Filbeskrivelse."""

    deltabell: str
    variabler: list[KolonneVariabelType]
    statistikkvariabler: list[KolonneStatistikkvariabelType]
    internasjonal_rapportering: NotRequired[list[KolonneInternasjonalRapporteringType]]
    null_prikk_missing: NotRequired[list[SuppressionDeltabellCodeListType]]
    eksempel_linje: str


class FilBeskrivelseType(TypedDict):
    """The content of the json returned by the uttrekssbeskrivelse endpoint, takes this shape.

    Parts of it are later assigned to attributes on class-instances.
    """

    Uttaksbeskrivelse_lagd: str
    base: str
    TabellId: str
    Huvudtabell: str
    DeltabellTitler: list[dict[str, str]]
    deltabller: list[DelTabellType]
    null_prikk_missing_kodeliste: NotRequired[list[SuppressionCodeListType]]
    kodelister: list[KodelisteType]
    IRkodelister: NotRequired[list[KodelisteType]]
