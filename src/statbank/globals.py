from __future__ import annotations

import datetime as dt
import enum
import zoneinfo


class Approve(enum.IntEnum):
    """Enum for approval codes."""

    MANUAL = 0
    """Manual approval."""
    AUTOMATIC = 1
    """Automatic approval at transfer-time (immediately)."""
    JIT = 2
    """Just in time approval right before publishing time."""


class DaplaRegion(enum.Enum):
    """Environment variable for what Dapla region your running on."""

    ON_PREM = "ON_PREM"
    DAPLA_LAB = "DAPLA_LAB"


class DaplaEnvironment(enum.Enum):
    """Environment variable for what Dapla environment your running on."""

    PROD = "PROD"
    TEST = "TEST"
    DEV = "DEV"


class UseDb(enum.Enum):
    """Hold options for database choices, targeted at Statbanken."""

    PROD = "PROD"
    TEST = "TEST"


def _approve_type_check(approve: Approve | int | str) -> Approve:
    result: Approve

    match approve:
        case Approve():
            result = approve
        case int():
            result = Approve(approve)
        case str() if approve.isdigit():
            result = Approve(int(approve))
        case str():
            result = getattr(Approve, approve.upper())
        case _:
            error_msg = f"Dont know how to handle approve of type {type(approve)}"  # type: ignore[unreachable]
            raise TypeError(error_msg)

    return result


OSLO_TIMEZONE = zoneinfo.ZoneInfo("Europe/Oslo")
TOMORROW = dt.date.today() + dt.timedelta(days=1)  # noqa: DTZ011
APPROVE_DEFAULT_JIT = Approve.JIT
STATBANK_TABLE_ID_LEN = 5
REQUEST_OK = 200
SSB_TBF_LEN = 3
DATETIME_FORMAT: str = "%d.%m.%Y klokka %H:%M"
