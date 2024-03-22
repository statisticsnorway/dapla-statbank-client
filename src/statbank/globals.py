from __future__ import annotations

import datetime as dt
import enum


class Approve(enum.IntEnum):
    """Enum for approval codes."""

    MANUAL = 0
    """Manual approval."""
    AUTOMATIC = 1
    """Automatic approval at transfer-time (immediately)."""
    JIT = 2
    """Just in time approval right before publishing time."""


def _approve_type_check(approve: Approve | int | str) -> Approve:
    if isinstance(approve, int) and not isinstance(approve, Approve):
        result: Approve = Approve(approve)
    elif isinstance(approve, str) and approve.isdigit():
        result = Approve(int(approve))
    elif isinstance(approve, str):
        result = getattr(Approve, approve)
    elif isinstance(approve, Approve):
        result = approve
    else:
        error_msg = f"Dont know how to handle approve of type {type(approve)}"  # type: ignore[unreachable]
        raise TypeError(error_msg)
    return result


OSLO_TIMEZONE = dt.timezone(dt.timedelta(hours=1))
TOMORROW = dt.datetime.now(tz=OSLO_TIMEZONE) + dt.timedelta(days=1)
APPROVE_DEFAULT_JIT = Approve.JIT
STATBANK_TABLE_ID_LEN = 5
REQUEST_OK = 200
SSB_TBF_LEN = 3
