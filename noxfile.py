"""Nox sessions."""

from collections.abc import Iterable
import os
import shlex
import shutil
import sys
from pathlib import Path
from textwrap import dedent

import nox
from nox import Session

package = "statbank"
python_versions = [
    "3.12",
    "3.13",
    "3.14",
]
python_versions_for_test = python_versions
nox.needs_version = ">= 2025.2.9"
nox.options.sessions = (
    "pre-commit",
    "mypy",
    "tests",
    "typeguard",
    "xdoctest",
    "docs-build",
)
nox.options.default_venv_backend = "uv"
session = nox.session


def install_with_uv(
    session: Session,
    *,
    only_groups: list[str] | None = None,
    all_extras: bool = False,
    locked: bool = True,
    resolution: str | None = None,
) -> None:
    """Install packages using uv."""
    if resolution is None:
        cmd = ["uv", "sync"]
        if locked:
            cmd.append("--locked")
    else:
        cmd = [
            "uv",
            "pip",
            "install",
            "--resolution",
            resolution,
            "--editable",
            ".",
        ]
        only_groups = only_groups or ["dev"]
    if only_groups:
        group_flag = "--group" if resolution is not None else "--only-group"
        for group in only_groups:
            cmd.extend([group_flag, group])
    if all_extras:
        cmd.append("--all-extras")
    cmd.append(
        f"--python={session.virtualenv.location}"
    )  # Target the nox venv's Python interpreter
    if resolution is not None:
        session.run_install(*cmd)
    else:
        session.run_install(
            *cmd, env={"UV_PROJECT_ENVIRONMENT": session.virtualenv.location}
        )


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
        hook.write_text(
            insert_header_in_hook(headers, lines)
        )  # NOSONAR - Safe because hardcoded local directory, and not user-controlled input.


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


@session(name="pre-commit", python="3.13")
def precommit(session: Session) -> None:
    """Lint using pre-commit."""
    args = session.posargs or [
        "run",
        "--all-files",
        "--hook-stage=manual",
        "--show-diff-on-failure",
    ]
    install_with_uv(session, only_groups=["lint"])
    session.run("pre-commit", *args)
    if args and args[0] == "install":
        activate_virtualenv_in_precommit_hooks(session)


@session(python=python_versions)
def mypy(session: Session) -> None:
    """Type-check using mypy."""
    args = session.posargs or ["src"]
    install_with_uv(session)
    session.run("mypy", *args)
    if not session.posargs:
        session.run("mypy", f"--python-executable={sys.executable}", "noxfile.py")


@session(python=python_versions)
def tests(session: Session) -> None:
    """Run the test suite."""
    install_with_uv(session)
    try:
        session.run(
            "coverage",
            "run",
            "--parallel",
            "-m",
            "pytest",
            "-m",
            "not integration_dapla",
        )
    finally:
        if session.interactive:
            session.notify("coverage", posargs=[])


@session(name="dependency-bounds", python="3.13")
@nox.parametrize("resolution", ["lowest-direct", "highest"])
def dependency_bounds(session: Session, resolution: str) -> None:
    """Run the test suite with lowest-direct and highest dependency versions."""
    install_with_uv(session, resolution=resolution)
    session.run(
        "pytest",
        "-m",
        "not integration_dapla",
    )


@session(python="3.13")
def coverage(session: Session) -> None:
    """Produce the coverage report."""
    args = session.posargs or ["report", "--skip-empty"]
    install_with_uv(session)
    if not session.posargs and any(Path().glob(".coverage.*")):
        session.run("coverage", "combine")

    session.run("coverage", *args)


@session(python="3.13")
def typeguard(session: Session) -> None:
    """Runtime type checking using Typeguard."""
    install_with_uv(session)
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

    install_with_uv(session)
    session.run("python", "-m", "xdoctest", *args)


@session(name="docs-build", python="3.13")
def docs_build(session: Session) -> None:
    """Build the documentation."""
    args = session.posargs or ["docs", "docs/_build"]
    if not session.posargs and "FORCE_COLOR" in os.environ:
        args.insert(0, "--color")

    install_with_uv(session, only_groups=["docs"])

    build_dir = Path("docs", "_build")
    if build_dir.exists():
        shutil.rmtree(build_dir)

    session.run("sphinx-build", *args)


@session(python="3.13")
def docs(session: Session) -> None:
    """Build and serve the documentation with live reloading on file changes."""
    args = session.posargs or ["--open-browser", "docs", "docs/_build"]
    install_with_uv(session, only_groups=["docs"])

    build_dir = Path("docs", "_build")
    if build_dir.exists():
        shutil.rmtree(build_dir)

    session.run("sphinx-autobuild", *args)
