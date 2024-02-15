from typing import TypedDict

from typing_extensions import NotRequired

####################
# TRANSFER RESULTS #
####################


#######################
# UTTREKKSBESKRIVELSE #
#######################


class KodeType(TypedDict):
    """The code -> labels for the list in the Codelists."""

    kode: str
    text: str


class KodelisteType(TypedDict):
    """The codelists have their own name, which links from the column-metadata, and a list of code -> labels."""

    kodeliste: str
    koder: list[KodeType]


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
    null_prikk_missing: NotRequired[list[dict[str, str]]]
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
    null_prikk_missing_kodeliste: NotRequired[list[dict[str, str]]]
    kodelister: list[KodelisteType]
    IRkodelister: NotRequired[list[KodelisteType]]
