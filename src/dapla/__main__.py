"""Command-line interface."""
import click


@click.command()
@click.version_option()
def main() -> None:
    """Dapla Statbank Client."""


if __name__ == "__main__":
    main(prog_name="dapla-statbank-client")  # pragma: no cover
