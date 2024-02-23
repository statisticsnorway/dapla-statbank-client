"""The statbank package facilitates publishing data to, and retriving data from Statistics Norway's, public facing 'statistical bank': statbanken."""

__version__ = "1.0.10"

__all__ = ["StatbankClient", "apidata", "apidata_all", "apidata_rotate"]

from statbank.apidata import apidata
from statbank.apidata import apidata_all
from statbank.apidata import apidata_rotate
from statbank.client import StatbankClient
