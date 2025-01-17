from __future__ import annotations

import logging
import sys
from typing import Any

from colorama import Back
from colorama import Fore
from colorama import Style


class ColoredFormatter(logging.Formatter):
    """Colored log formatter."""

    def __init__(
        self,
        *args: Any,
        colors: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the formatter with specified format strings."""
        super().__init__(*args, **kwargs)

        self.colors = colors if colors else {}

    def format(self, record: logging.LogRecord) -> str:
        """Format the specified record as text."""
        record.color = self.colors.get(record.levelname, "")
        record.reset = Style.RESET_ALL

        return super().format(record)


formatter = ColoredFormatter(
    "{color} {levelname:8} {reset}| {message}",
    style="{",
    colors={
        "DEBUG": Fore.CYAN,
        "INFO": Fore.GREEN,
        "WARNING": Fore.MAGENTA,
        "ERROR": Fore.RED,
        "CRITICAL": Fore.RED + Back.WHITE + Style.BRIGHT,
    },
)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.handlers[:] = []
logger.addHandler(handler)
logger.setLevel(logging.INFO)
