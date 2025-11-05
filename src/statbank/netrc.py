"""Modified version of tinynetrc."""

from __future__ import annotations

import netrc
import sys
from collections import defaultdict
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import MutableMapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING
from typing import TypeAlias

if TYPE_CHECKING:
    from types import TracebackType

if sys.version_info >= (3, 11):
    from typing import Self

    @dataclass
    class NetrcRecord:
        """Dataclass store login info."""

        login: str
        account: str
        password: str

        def __bool__(self: Self) -> bool:  # noqa: D105
            return self.login != ""

    if TYPE_CHECKING:
        _NetrcTuple: TypeAlias = tuple[str, str, str]

else:
    from typing_extensions import Self

    @dataclass
    class NetrcRecord:
        """Dataclass to store login info."""

        login: str
        account: str | None
        password: str | None

        def __bool__(self: Self) -> bool:  # noqa: D105
            return self.login != ""

    if TYPE_CHECKING:
        _NetrcTuple: TypeAlias = tuple[str, str | None, str | None]


def _to_record(
    hosts: Mapping[str, _NetrcTuple],
) -> defaultdict[str, NetrcRecord]:
    ret: defaultdict[str, NetrcRecord] = defaultdict(
        lambda: NetrcRecord(login="", account="", password=""),
    )
    for machine, info in hosts.items():
        ret[machine] = NetrcRecord(*info)
    return ret


def _to_tuple(machines: Mapping[str, NetrcRecord]) -> dict[str, _NetrcTuple]:
    return {
        machine: (info.login, info.account, info.password)
        for machine, info in machines.items()
    }


class Netrc(MutableMapping[str, NetrcRecord]):
    """A mapping of a .netrc file, that can be used as a context manager, and save changes to disk."""

    def __init__(self: Self, filepath: Path | None = None) -> None:  # noqa: D107
        if filepath is None:
            filepath = Path.home() / ".netrc"
        if not filepath.exists():
            filepath.touch()
        self.filepath: Path = filepath
        self._netrc = netrc.netrc(filepath)
        self.machines: defaultdict[str, NetrcRecord] = _to_record(self._netrc.hosts)

    def authenticators(self: Self, host: str) -> _NetrcTuple | None:
        """Return a (user, account, password) tuple for given host."""
        return self._netrc.authenticators(host)

    @property
    def hosts(self: Self) -> dict[str, _NetrcTuple]:
        """Return a dict with hosts as keys and a (user, account, password) tuple as value."""
        return self._netrc.hosts

    def __getitem__(self: Self, key: str) -> NetrcRecord:  # noqa: D105
        return self.machines[key]

    def __setitem__(self: Self, key: str, value: NetrcRecord) -> None:  # noqa: D105
        self.machines[key] = value

    def __delitem__(self: Self, key: str) -> None:  # noqa: D105
        del self.machines[key]

    def __iter__(self: Self) -> Iterator[str]:
        """Iter over hosts."""
        return iter(self.machines)

    def __len__(self: Self) -> int:
        """Number of auth records."""
        return len(self.machines)

    #### end dict-like interface implementation #####

    def __enter__(self: Self) -> Self:  # noqa: D105
        return self

    def __exit__(  # noqa: D105
        self: Self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        if not exc_type and self.is_changed:
            self.save()

    def __repr__(self: Self) -> str:  # noqa: D105
        return repr(dict(self.machines))

    @property
    def is_changed(self: Self) -> bool:
        """Check if changed from last save."""
        return self._netrc.hosts != _to_tuple(self.machines)

    def _dump(self: Self) -> str:
        """Dump the class data in the format of a .netrc file."""
        self._netrc.hosts = _to_tuple(self.machines)

        rep = ""
        for host in self._netrc.hosts.keys():
            attrs = self._netrc.hosts[host]
            rep += f"machine {host}\n\tlogin {attrs[0]}\n"
            if attrs[1]:
                rep += f"\taccount {attrs[1]}\n"
            if attrs[2]:
                rep += f"\tpassword {attrs[2]}\n"
        for macro in self._netrc.macros.keys():
            rep += f"macdef {macro}\n"
            for line in self._netrc.macros[macro]:
                rep += line
            rep += "\n"
        return rep

    def save(self: Self) -> None:
        """Save changes to disk."""
        with self.filepath.open("w") as fp:
            fp.write(self._dump())

        self._netrc.hosts = _to_tuple(self.machines)
