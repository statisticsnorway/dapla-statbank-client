from __future__ import annotations

import getpass
import os
import sys
from importlib.metadata import version
from typing import TYPE_CHECKING
from typing import Literal
from typing import cast

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

import requests as r
import requests.auth
from dapla_auth_client import AuthClient
from furl import furl

from .globals import DaplaEnvironment
from .globals import DaplaRegion
from .globals import UseDb
from .statbank_logger import logger
from .writable_netrc import Netrc

if TYPE_CHECKING:
    from pathlib import Path


class TokenAuth(requests.auth.AuthBase):
    """Token based authorization."""

    def __init__(  # noqa: D107
        self: Self,
        token: str,
        auth_scheme: str = "Bearer",
    ) -> None:
        self.token = token
        self.auth_scheme = auth_scheme

    def __call__(  # noqa: D102
        self: Self,
        request: r.PreparedRequest,
    ) -> r.PreparedRequest:
        request.headers["Authorization"] = f"{self.auth_scheme} {self.token}"
        return request


class StatbankConfig:
    """Holds config for Transfer-API" and "Uttaksbeskrivelse-API."""

    def __init__(  # noqa: D107
        self: Self,
        *,
        endpoint_base: furl,
        encrypt_url: furl,
        useragent: str,
        environment: DaplaEnvironment,
        region: DaplaRegion,
        netrc_path: Path | None = None,
    ) -> None:
        self.endpoint_base: furl = endpoint_base
        self.encrypt_url: furl = encrypt_url
        self.useragent: str = useragent
        self.environment: DaplaEnvironment = environment
        self.region: DaplaRegion = region
        self.netrc_path: Path | None = netrc_path

    @classmethod
    def from_environ(cls: type[Self], use_db: UseDb | None) -> Self:
        """Load config from environment variables."""
        environment = DaplaEnvironment(os.environ.get("DAPLA_ENVIRONMENT", "TEST"))
        region = DaplaRegion(os.environ.get("DAPLA_REGION", "ON_PREM"))
        service = os.environ.get("DAPLA_SERVICE", "JUPYTERLAB")

        if use_db == UseDb.TEST and environment == DaplaEnvironment.PROD:
            env_key_endpoint_base = "STATBANK_TEST_BASE_URL"
            env_key_encrypt_url = "STATBANK_TEST_ENCRYPT_URL"

        else:
            env_key_endpoint_base = "STATBANK_BASE_URL"
            env_key_encrypt_url = "STATBANK_ENCRYPT_URL"

        try:
            endpoint_base = furl(os.environ[env_key_endpoint_base])
            encrypt_url = furl(os.environ[env_key_encrypt_url])
        except KeyError as e:
            error_message = "Kunne ikke finne miljøvariabel"
            raise ValueError(error_message) from e
        except ValueError as e:
            error_message = "Miljøvariabelene innholder en url som ikke er gyldig"
            raise ValueError(error_message) from e

        useragent = f"dapla-statbank-client:{version('dapla-statbank-client')}:{environment.value.lower()}-{region.value.lower()}-{service.lower()}"

        return cls(
            endpoint_base=endpoint_base,
            encrypt_url=encrypt_url,
            useragent=useragent,
            environment=environment,
            region=region,
        )


class StatbankAuth:
    """Parent class for shared behavior between Statbankens "Transfer-API" and "Uttaksbeskrivelse-API"."""

    def __init__(
        self: Self,
        use_db: UseDb | Literal["TEST", "PROD"] | None = None,
        *,
        config: StatbankConfig | None = None,
        auth: requests.auth.AuthBase | None = None,
    ) -> None:
        """Initialize auth and config."""
        if isinstance(use_db, str):
            use_db = UseDb(use_db)

        if config is None:
            config = StatbankConfig.from_environ(use_db)

        if use_db is None:
            use_db = (
                UseDb.PROD
                if config.environment is DaplaEnvironment.PROD
                else UseDb.TEST
            )

        self._config: StatbankConfig = config
        self.use_db: UseDb = use_db

        if auth is None:
            auth = self._get_auth()

        self._auth: requests.auth.AuthBase = auth

    def check_env(self) -> str:
        """Check if you are on Dapla or in prodsone.

        Simplified terribly by the addition of env vars for this, keeping this method for legacy reasons.

        Returns:
            "DAPLA" if on dapla, "PROD" if you are in prodsone.
        """
        return "DAPLA" if self._config.region == DaplaRegion.DAPLA_LAB else "PROD"

    def check_database(self: Self) -> str:
        """Checks if we are in prod environment. And which statbank-database we are sending to."""
        return self.use_db.value

    def _build_headers(self: Self) -> dict[str, str]:
        return {
            "Content-Type": "multipart/form-data; boundary=12345",
            "Connection": "keep-alive",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept": r"*/*",
            "User-Agent": self._config.useragent,
        }

    def _get_auth(self) -> requests.auth.AuthBase:
        host = cast(str, self._config.endpoint_base.host)  # noqa: TC006

        with Netrc(self._config.netrc_path) as authfile:
            auth_record = authfile[host]

            if not auth_record:
                db = self.use_db.value
                username = getpass.getpass("Lastebruker:")
                password = getpass.getpass(f"Lastepassord ({db}):")
                token = self._encrypt_password(password)

                auth_record.login = username
                auth_record.password = token

        return requests.auth.HTTPBasicAuth(
            auth_record.login,
            (
                auth_record.password if auth_record.password is not None else ""
            ),  # Can be None in Python 3.10
        )

    def _encrypt_password(self: Self, password: str) -> str:
        pat = None

        if self._config.region != DaplaRegion.ON_PREM:
            try:
                pat = AuthClient.fetch_personal_token()
            except RuntimeError as err:
                logger.warning(str(err))

        auth = TokenAuth(pat) if pat is not None else None

        repsonse = r.post(
            self._config.encrypt_url.url,
            auth=auth,
            json={"message": password},
            timeout=30,
        )

        repsonse.raise_for_status()

        data = cast(dict[Literal["message"], str], repsonse.json())  # noqa: TC006

        return data["message"]

    def _build_urls(self: Self) -> dict[str, furl]:
        end_urls = {
            "loader": "statbank/sos/v1/DataLoader",
            "uttak": "statbank/sos/v1/uttaksbeskrivelse",
            "gui": "lastelogg/gui",
            "api": "lastelogg/api",
        }
        return {k: self._config.endpoint_base / v for k, v in end_urls.items()}
