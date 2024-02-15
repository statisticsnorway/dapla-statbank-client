from typing import TypedDict

from typing_extensions import NotRequired


class DelTabellType(TypedDict):
    """A typed subpart of the returned Filbeskrivelse."""

    deltabell: str
    variabler: list[dict[str, str]]
    statistikkvariabler: list[dict[str, str]]
    null_prikk_missing: list[dict[str, str]]
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
    null_prikk_missing_kodeliste: list[dict[str, str]]
    kodelister: list[dict[str, str]]
    IRkodelister: NotRequired[list[dict[str, str]]]
