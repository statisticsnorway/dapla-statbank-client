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


OSLO_TIMEZONE = dt.timezone(dt.timedelta(hours=1))
TOMORROW = dt.datetime.now(tz=OSLO_TIMEZONE) + dt.timedelta(days=1)
APPROVE_DEFAULT_JIT = Approve.JIT
STATBANK_TABLE_ID_LEN = 5
REQUEST_OK = 200
SSB_TBF_LEN = 3
