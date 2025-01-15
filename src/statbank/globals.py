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
