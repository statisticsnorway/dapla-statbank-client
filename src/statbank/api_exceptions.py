from typing import Any

import requests


class StatbankAuthError(requests.HTTPError):
    """Raised when authentication with Statbank fails.

    Extends ``requests.HTTPError`` with an optional ``response_content`` attribute
    that can hold parsed JSON from the response.

    Args:
        *args: Positional arguments forwarded to ``requests.HTTPError``.
        response_content: Optional JSON-decoded payload to attach.
        **kwargs: Keyword arguments forwarded to ``requests.HTTPError``.
    """

    def __init__(
        self,
        *args: Any,
        response_content: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the Statbank authentication error.

        Args:
            *args: Positional arguments forwarded to ``requests.HTTPError``.
            response_content: Optional JSON-decoded payload to attach.
            **kwargs: Keyword arguments forwarded to ``requests.HTTPError``.
        """
        super().__init__(*args, **kwargs)
        self.response_content: dict[str, Any] | None = response_content

    def __str__(self) -> str:
        """Create a string representation of the error, including the response_content, if any.
        
        Returns:
            str: The string representation of the error.
        """
        base = super().__str__()
        if self.response_content:
            return f"{base}\nResponse content: {self.response_content}"
        return base


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
