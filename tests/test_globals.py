import datetime as dt
import time
from collections.abc import Callable
from unittest.mock import patch

import pytest

from statbank.globals import OSLO_TIMEZONE
from statbank.globals import add_dst_hour
from statbank.globals import is_dst_active


# Tests for is_dst_active
@pytest.mark.parametrize(
    ("date_str", "expected", "mock_dst_flag"),  # Changed to a tuple
    [
        ("2024-06-01", True, 1),  # Summer (DST active)
        ("2024-12-01", False, 0),  # Winter (DST inactive)
        ("2024-03-10", True, 1),  # Transition into DST
        ("2024-11-01", False, 0),  # Transition out of DST
    ],
)
@patch("time.localtime")
@patch("time.mktime")
def test_is_dst_active(
    mock_mktime: patch,
    mock_localtime: patch,
    date_str: str,
    expected: bool,
    mock_dst_flag: int,
):
    """Test is_dst_active to verify it correctly detects DST status."""
    # Mock mktime and localtime to return desired DST flag
    mock_mktime.return_value = 0  # Simulate epoch seconds
    mock_localtime.return_value = time.struct_time(
        (2024, 6, 1, 0, 0, 0, 0, 152, mock_dst_flag),
    )

    assert is_dst_active(date_str) == expected


# Tests for add_dst_hour
@pytest.mark.parametrize(
    ("input_date", "expected_result", "is_dst_mock"),  # Changed to a tuple
    [
        (
            "2024-06-01",
            dt.datetime(2024, 6, 1, 1, 0, 0, tzinfo=OSLO_TIMEZONE),
            True,
        ),  # Summer (add 1 hour)
        (
            "2024-12-01",
            dt.datetime(2024, 12, 1, 0, 0, 0, tzinfo=OSLO_TIMEZONE),
            False,
        ),  # Winter (no extra hour)
    ],
)
@patch("statbank.globals.is_dst_active")
def test_add_dst_hour(
    mock_is_dst_active: Callable,
    input_date: str,
    expected_result: dt.datetime,
    is_dst_mock: bool,
):
    """Test add_dst_hour to ensure correct addition of an hour for DST-active dates."""
    # Mock DST detection
    mock_is_dst_active.return_value = is_dst_mock

    result = add_dst_hour(input_date, date_format="%Y-%m-%d")
    assert result == expected_result
