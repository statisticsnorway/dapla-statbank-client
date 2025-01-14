from __future__ import annotations

import datetime as dt
import enum
import time


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


def is_dst_active(date_str: str, date_format: str = r"%Y-%m-%d") -> bool:
    """Checks if DST is active for a given date.

    Args:
        date_str (str): The date to check, as a string.
        date_format (str): The format of the date string (default: "%Y-%m-%d").

    Returns:
        bool: True if DST is active, False otherwise.

    Raises:
        ValueError: If the date string does not match the specified format.
    """
    try:
        # Convert the date string to a struct_time object
        date_struct = time.strptime(date_str, date_format)
        # Convert struct_time to epoch seconds
        epoch_seconds = int(time.mktime(date_struct))
        # Check the DST flag
        return time.localtime(epoch_seconds).tm_isdst > 0
    except ValueError as err:
        # Raise a new error with context of the original exception
        err_msg = f"Invalid date or format: {date_str} does not match {date_format}"
        raise ValueError(err_msg) from err


def add_dst_hour(
    date: str | dt.datetime,
    date_format: str = r"%Y-%m-%d",
) -> dt.datetime:
    """Adds one hour if the given date is within daylight saving time (DST).

    This function checks whether the given date falls within DST and returns a
    timedelta object representing an additional hour if DST is active. If DST
    is not active, it returns a timedelta of zero hours.

    Args:
        date: The date to check, as a datetime or a string (e.g., "2024-06-01").
        date_format: The format of the date string, following `strftime`
            conventions. Defaults to "%Y-%m-%d".

    Returns:
        datetime.datetime: The datetime sent in adding an hour if .
    """
    if not isinstance(date, str):
        date_dt: dt.datetime = date
        date_str: str = date.strftime(r"%Y-%m-%d")
    if isinstance(date, str):
        date_dt = dt.datetime.strptime(date, date_format).replace(
            tzinfo=OSLO_TIMEZONE,
        )
        date_str = date_dt.strftime(r"%Y-%m-%d")

    if is_dst_active(date_str):
        return date_dt + dt.timedelta(hours=1)
    return date_dt + dt.timedelta(hours=0)


OSLO_TIMEZONE = dt.timezone(dt.timedelta(hours=1))
TOMORROW = dt.datetime.now(tz=OSLO_TIMEZONE) + dt.timedelta(days=1)
APPROVE_DEFAULT_JIT = Approve.JIT
STATBANK_TABLE_ID_LEN = 5
REQUEST_OK = 200
SSB_TBF_LEN = 3
