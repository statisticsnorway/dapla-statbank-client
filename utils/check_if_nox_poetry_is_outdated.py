import re
import shutil
import subprocess
import sys
from pathlib import Path

import nox


def _read_constraints() -> dict[str, str]:
    """Read pinned tool versions from constraints file.

    Only `poetry` and `nox` are considered for the warning.
    Returns a mapping like {"poetry": "2.2.1", "nox": "2025.10.16"}.
    """
    constraints: dict[str, str] = {}
    path = Path(".github/workflows/constraints.txt")
    try:
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "==" not in line:
                continue
            name, version = line.split("==", 1)
            name = name.strip().lower()
            version = version.strip()
            if name in {"poetry", "nox"}:
                constraints[name] = version
    except FileNotFoundError:
        pass
    return constraints


def _version_tuple(v: str) -> tuple[int, ...]:
    """Parse a version string into a numeric tuple for comparison.

    Extracts all integer groups in order, e.g. "2.2.1" -> (2, 2, 1),
    "2025.10.16" -> (2025, 10, 16), "2.2.1b1" -> (2, 2, 1, 1).
    """
    nums = re.findall(r"\d+", v)
    return tuple(int(n) for n in nums) if nums else (0,)


def _poetry_version_output() -> str | None:
    """Return stdout of `poetry --version`, or None on failure."""
    try:
        poetry_path = shutil.which("poetry")
        if not poetry_path:
            return None
        result = subprocess.run(  # noqa: S603
            [poetry_path, "--version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        return result.stdout.strip()
    except (OSError, FileNotFoundError, subprocess.SubprocessError):
        return None


def _extract_poetry_version(s: str | None) -> str | None:
    """Extract version from `poetry --version` output."""
    if not s:
        return None
    m = re.search(r"\bversion\s*([0-9][^\s\)]*)", s, flags=re.IGNORECASE)
    if m:
        return m.group(1)
    # Fallback: first version-like pattern
    m = re.search(r"\d+(?:[\.\-]\d+)*[a-z0-9]*", s, flags=re.IGNORECASE)
    return m.group(0) if m else None


def _extract_nox_version_from_cmd(s: str | None) -> str | None:
    """Extract version from `nox --version` output."""
    if not s:
        return None
    # Newer nox prints like: "2025.10.16"; older: "nox, version 2023.04.22"
    m = re.search(r"(\d{4}\.\d{2}\.\d{2}|\d+(?:\.\d+)+)", s)
    return m.group(1) if m else None


def check_tool_versions_against_constraints() -> None:
    """Compare local poetry/nox versions to pinned constraints and warn if older."""
    constraints = _read_constraints()

    # Gather installed versions
    poetry_out = _poetry_version_output()  # relies on Poetry in PATH
    poetry_ver = _extract_poetry_version(poetry_out)

    # Prefer library version for the running nox, fallback to CLI
    nox_ver = getattr(nox, "__version__", None)
    if not nox_ver:
        nox_path = shutil.which("nox")
        if nox_path:
            try:
                nox_result = subprocess.run(  # noqa: S603
                    [nox_path, "--version"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    check=False,
                )
                nox_ver = _extract_nox_version_from_cmd(nox_result.stdout)
            except (OSError, FileNotFoundError, subprocess.SubprocessError):
                nox_ver = None
        else:
            nox_ver = None

    warnings: list[str] = []

    # Compare poetry
    required_poetry = constraints.get("poetry")
    if (
        required_poetry
        and poetry_ver
        and _version_tuple(poetry_ver) < _version_tuple(required_poetry)
    ):
        warnings.append(
            f"poetry: installed {poetry_ver}, required {required_poetry}",
        )

    # Compare nox
    required_nox = constraints.get("nox")
    if (
        required_nox
        and nox_ver
        and _version_tuple(nox_ver) < _version_tuple(required_nox)
    ):
        warnings.append(f"nox: installed {nox_ver}, required {required_nox}")

    if warnings:
        msg = (
            "\n"  # start on a new line to stand out a bit
            "WARNING: Local tool versions are older than project constraints:\n"
            + "\n".join(f"  - {w}" for w in warnings)
            + "\nConsider upgrading your tools with: pipx upgrade-all\n"
        )
        sys.stderr.write(msg)
