class StatbankApiError(Exception):
    """Base class for Statbank Api Errors."""


class TooBigRequestError(StatbankApiError):
    """Exception for request that is to big to handle for the Statbank Api.

    The API is limited to 800,000 cells (incl. empty cells)
    """


class StatbankParameterError(StatbankApiError):
    """Exception for invalid request."""


class StatbankVariableSelectionError(StatbankApiError):
    """Exception for invalid variable selection."""
