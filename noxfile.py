"""Nox sessions.

Adds a light pre-run check that compares locally installed
`poetry` and `nox` versions to the pinned tool versions in
`.github/workflows/constraints.txt`. If either local tool is
older than the constraint, a warning is printed recommending
`pipx upgrade-all`.
"""

from collections.abc import Iterable
import os
import shlex
import shutil
import sys
from pathlib import Path
from textwrap import dedent

import nox

# Ensure project root is importable when Nox loads this file from elsewhere
_ROOT = Path(__file__).parent.resolve()
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.check_if_nox_poetry_is_outdated import (
    check_tool_versions_against_constraints,
)


try:
    from nox_poetry import Session
    from nox_poetry import session
except ImportError:
    message = f"""\
    Nox failed to import the 'nox-poetry' package.

    Please install it using the following command:

    {sys.executable} -m pip install nox-poetry"""
    raise SystemExit(dedent(message)) from None


# Run the check as soon as the noxfile is imported (i.e., on `poetry run nox`).
check_tool_versions_against_constraints()

package = "statbank"
python_versions = [
    "3.10",
    "3.11",
    "3.12",
    "3.13",
]
python_versions_for_test = python_versions
nox.needs_version = ">= 2021.6.6"
nox.options.sessions = (
    "pre-commit",
    "mypy",
    "tests",
    "typeguard",
    "xdoctest",
    "docs-build",
)


def install_poetry_groups(session: Session, groups: Iterable[str]) -> None:
    """Manually parse the pyproject file to find group(s) of dependencies, then install."""
    pyproject_path = Path("pyproject.toml")
    data = nox.project.load_toml(pyproject_path)
    group_data = data["tool"]["poetry"]["group"]
    all_dependencies = []
    for group in groups:
        dependencies = group_data[group]["dependencies"]
        for dependency, spec in dependencies.items():
            if isinstance(spec, dict) and "extras" in spec:
                dependency += "[{}]".format(",".join(spec["extras"]))
            all_dependencies.append(dependency)
    all_dependencies = list(set(all_dependencies))
    session.install(*all_dependencies)


def activate_virtualenv_in_precommit_hooks(session: Session) -> None:
    """Activate virtualenv in hooks installed by pre-commit.

    This function patches git hooks installed by pre-commit to activate the
    session's virtual environment. This allows pre-commit to locate hooks in
    that environment when invoked from git.

    Args:
        session: The Session object.
    """
    assert session.bin is not None  # nosec

    # Only patch hooks containing a reference to this session's bindir. Support
    # quoting rules for Python and bash, but strip the outermost quotes so we
    # can detect paths within the bindir, like <bindir>/python.
    bindirs = [
        bindir[1:-1] if bindir[0] in "'\"" else bindir
        for bindir in (repr(session.bin), shlex.quote(session.bin))
    ]

    virtualenv = session.env.get("VIRTUAL_ENV")
    if virtualenv is None:
        return

    headers = {
        # pre-commit < 2.16.0
        "python": f"""\
            import os
            os.environ["VIRTUAL_ENV"] = {virtualenv!r}
            os.environ["PATH"] = os.pathsep.join((
                {session.bin!r},
                os.environ.get("PATH", ""),
            ))
            """,
        # pre-commit >= 2.16.0
        "bash": f"""\
            VIRTUAL_ENV={shlex.quote(virtualenv)}
            PATH={shlex.quote(session.bin)}"{os.pathsep}$PATH"
            """,
        # pre-commit >= 2.17.0 on Windows forces sh shebang
        "/bin/sh": f"""\
            VIRTUAL_ENV={shlex.quote(virtualenv)}
            PATH={shlex.quote(session.bin)}"{os.pathsep}$PATH"
            """,
    }

    hookdir = Path(".git") / "hooks"
    if not hookdir.is_dir():
        return

    for hook in hookdir.iterdir():
        if hook.name.endswith(".sample") or not hook.is_file():
            continue

        if not hook.read_bytes().startswith(b"#!"):
            continue

        text = hook.read_text()

        if not is_bindir_in_text(bindirs, text):
            continue

        lines = text.splitlines()
        hook.write_text(insert_header_in_hook(headers, lines))


def is_bindir_in_text(bindirs: list[str], text: str) -> bool:
    """Helper function to check if bindir is in text."""
    return any(
        Path("A") == Path("a") and bindir.lower() in text.lower() or bindir in text
        for bindir in bindirs
    )


def insert_header_in_hook(header: dict[str, str], lines: list[str]) -> str:
    """Helper function to insert headers in hook's text."""
    for executable, header_text in header.items():
        if executable in lines[0].lower():
            lines.insert(1, dedent(header_text))
            return "\n".join(lines)
    return "\n".join(lines)


@session(name="pre-commit", python="3.11")
def precommit(session: Session) -> None:
    """Lint using pre-commit."""
    args = session.posargs or [
        "run",
        "--all-files",
        "--hook-stage=manual",
        "--show-diff-on-failure",
    ]
    session.install(
        "pre-commit",
        "pre-commit-hooks",
        "darglint",
        "ruff",
        "black",
    )
    session.run("pre-commit", *args)
    if args and args[0] == "install":
        activate_virtualenv_in_precommit_hooks(session)


@session(python=python_versions)
def mypy(session: Session) -> None:
    """Type-check using mypy."""
    args = session.posargs or ["src"]
    session.poetry.installroot()
    install_poetry_groups(session, ["dev", "typing"])
    session.run("mypy", *args)
    if not session.posargs:
        session.run("mypy", f"--python-executable={sys.executable}", "noxfile.py")


@session(python=python_versions)
def tests(session: Session) -> None:
    """Run the test suite."""
    session.poetry.installroot()
    session.install("coverage[toml]", "pytest", "pygments", "typeguard")
    try:
        session.run(
            "coverage",
            "run",
            "--parallel",
            "-m",
            "pytest",
            "-o",
            "pythonpath=",
            *session.posargs,
            "-m",
            "not integration_dapla",
        )
    finally:
        if session.interactive:
            session.notify("coverage", posargs=[])


@session(python=python_versions[-1])
def coverage(session: Session) -> None:
    """Produce the coverage report."""
    args = session.posargs or ["report", "--skip-empty"]

    session.install("coverage[toml]")

    if not session.posargs and any(Path().glob(".coverage.*")):
        session.run("coverage", "combine")

    session.run("coverage", *args)


@session(python="3.11")
def typeguard(session: Session) -> None:
    """Runtime type checking using Typeguard."""
    session.poetry.installroot()
    session.install("pytest", "typeguard", "pygments")
    session.run(
        "pytest",
        "-m",
        "not integration_dapla",
        f"--typeguard-packages={package}",
        *session.posargs,
    )


@session(python=python_versions)
def xdoctest(session: Session) -> None:
    """Run examples with xdoctest."""
    if session.posargs:
        args = [package, *session.posargs]
    else:
        args = [f"--modname={package}", "--command=all"]
        if "FORCE_COLOR" in os.environ:
            args.append("--colored=1")

    session.poetry.installroot()
    session.install("xdoctest[colors]")
    session.run("python", "-m", "xdoctest", *args)


@session(name="docs-build", python="3.11")
def docs_build(session: Session) -> None:
    """Build the documentation."""
    args = session.posargs or ["docs", "docs/_build"]
    if not session.posargs and "FORCE_COLOR" in os.environ:
        args.insert(0, "--color")

    session.poetry.installroot()
    install_poetry_groups(session, ["docs"])

    build_dir = Path("docs", "_build")
    if build_dir.exists():
        shutil.rmtree(build_dir)

    session.run("sphinx-build", *args)


@session(python="3.11")
def docs(session: Session) -> None:
    """Build and serve the documentation with live reloading on file changes."""
    args = session.posargs or ["--open-browser", "docs", "docs/_build"]
    session.poetry.installroot()
    install_poetry_groups(session, ["docs"])

    build_dir = Path("docs", "_build")
    if build_dir.exists():
        shutil.rmtree(build_dir)

    session.run("sphinx-autobuild", *args)
