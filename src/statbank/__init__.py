"""Validates and transfers data from Dapla to Statbank. Gets data from public and internal statbank.

Used internally by SSB (Statistics Norway).
"""

from __future__ import annotations

import datetime as dt
import importlib
import importlib.metadata  # Needed even with whole import over

import toml

from statbank.apidata import apidata
from statbank.apidata import apidata_all
from statbank.apidata import apidata_rotate
from statbank.client import StatbankClient
from statbank.statbank_logger import logger

__all__ = ["StatbankClient", "apidata", "apidata_all", "apidata_rotate"]


# Split into function for testing
def _try_getting_pyproject_toml(e: Exception | None = None) -> str:
    if e is None:
        passed_excep: Exception = Exception("")
    else:
        passed_excep = e
    try:
        try:
            version: str = toml.load("../pyproject.toml")["tool"]["poetry"]["version"]
        except FileNotFoundError:
            version = toml.load("./pyproject.toml")["tool"]["poetry"]["version"]
    except toml.TomlDecodeError as e:
        version = "0.0.0"
        logger.exception(
            "Error from dapla-statbank-clients __init__, not able to get version-number, setting it to %s. Exception: %s",
            version,
            str(passed_excep),
        )
    return version


# Gets the installed version from pyproject.toml, then there is no need to update this file
try:
    __version__ = importlib.metadata.version("dapla-statbank-client")
except importlib.metadata.PackageNotFoundError as e:
    __version__ = _try_getting_pyproject_toml(e)
