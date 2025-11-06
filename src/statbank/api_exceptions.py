from typing import Any

import requests


class StatbankAuthError(requests.HTTPError):
    """We pass up the error-response with this error up through try-excepts."""

    def __init__(self, other_error: str | Exception) -> None:
        """Initializing the error with a possibility of wrapping around an existing error.

        Args:
            other_error: The error-string, or a different exception that we want to re-wrap in this exception.
        """
        error_text: str = str(other_error)
        super().__init__(error_text)

        self.response_content: dict[str, Any] | None = None
        if hasattr(
            other_error,
            "response_content",
        ):  # If we double-wrap the error, lets try to save this attribute
            self.response_content = other_error.response_content


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
