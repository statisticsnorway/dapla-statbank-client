from pathlib import Path

from statbank.netrc import Netrc


def test_write_empty_netrc(empty_netrc_file: Path):
    with Netrc(empty_netrc_file) as authfile:
        authfile["test.com"].login = "foo"
        authfile["test.com"].password = "bar"  # noqa: S105

    text = empty_netrc_file.read_text()
    result = " ".join(line.lstrip() for line in text.split("\n") if line)
    expected = "machine test.com login foo password bar"

    assert result == expected


def test_write_existing_netrc(existing_netrc_file: Path, fake_auth:str):
    with Netrc(existing_netrc_file) as authfile:
        authfile["test.com"].login = "foo"
        authfile["test.com"].password = "bar"  # noqa: S105

    text = existing_netrc_file.read_text()
    result = " ".join(line.lstrip() for line in text.split("\n") if line)
    expected = f'machine fakeurl.com login ola password {fake_auth} machine test.com login foo password bar macdef init echo "foobar"'
    assert result == expected


def test_not_changed(existing_netrc_file: Path):
    with Netrc(existing_netrc_file) as authfile:
        assert not authfile.is_changed
