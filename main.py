import json
from pathlib import Path

import click

from filemanager.scan import scan_directory
from filemanager.storage import Database


def echo_colored_text(text: str, color: str = "red") -> None:
    """
    red: error
    yellow: warning
    blue: info
    green: success
    """
    match color:
        case "red":
            click.echo("\033[31m" + text + "\033[0m")
        case "green":
            click.echo("\033[32m" + text + "\033[0m")
        case "yellow":
            click.echo("\033[33m" + text + "\033[0m")
        case "blue":
            click.echo("\033[34m" + text + "\033[0m")
        case _:
            click.echo(text)


def confirm(text: str, *args) -> bool:
    yellow_text = lambda text: "\033[33m" + text + "\033[0m"
    args = [yellow_text(arg) for arg in args]
    text = text.format(*args)
    return click.confirm(text, default=False, show_default=True)


@click.command()
def debug():
    echo_colored_text("Welcome! ", "red")
    echo_colored_text("Welcome! ", "green")
    echo_colored_text("Welcome! ", "yellow")
    echo_colored_text("Welcome! ", "blue")


@click.group(help="scan file information")
def cli():
    "a tool to scan file information and store to database"
    pass


@click.command()
@click.option(
    "-d",
    "--directory",
    default=".",
    type=click.Path(exists=True, file_okay=False),
    help="The folder to scan",
)
@click.option(
    "-b",
    "--database",
    default="index.sqlite",
    type=click.Path(exists=True, dir_okay=False),
    help="sqlite file, used to filter already recorded",
)
def scan(directory, database):
    "scan files inside given directory"
    if not Path(directory).exists():
        echo_colored_text("directory does not exist!")
        return

    if not confirm("Scan {}, use database at {}? ", directory, database):
        return

    echo_colored_text(f"Scanning {directory} ...", "blue")
    scan_directory(directory, save=True, db_path=database)
    echo_colored_text(f"Scan {directory} finished", "green")


@click.command()
@click.argument("database", type=click.Path())
def init_database(database):
    "create and initialize a new database"
    if not confirm("Initialize the database at {}?", database):
        return
    if Path(database).exists():
        echo_colored_text("WARNING: database exists, but still continue!", "yellow")

    db = Database(database)
    echo_colored_text("Initialized the database", "green")


@click.command()
@click.option(
    "-d",
    "--database",
    default="index.sqlite",
    type=click.Path(exists=True, dir_okay=False),
    help="sqlite file",
)
@click.argument("source", type=click.Path(exists=True))
def submit(database, source):
    "submit scan result ( filescan_*.json you got from scan ) to database"
    if not confirm(
        "Submit filescan_*.json inside {}, use database at {}? ", source, database
    ):
        return
    source = Path(source)
    # get scan result files
    if source.is_dir():
        files = list(source.rglob("filescan_*.json"))
        if not len(files):
            echo_colored_text("no scan file found!")
            return
    else:
        files = [source]

    db = Database(database)

    echo_colored_text(f"{len(files)} files to submit", "blue")
    for file in files:
        try:
            data = json.load(open(file, "r", encoding="utf-8"))
            db.update_from_dicts(data["files"])
            echo_colored_text(f"Submit {str(file)} finished", "green")
        except Exception as e:
            echo_colored_text(f"Submit {str(file)} failed! Exception: {e}")
    echo_colored_text("Finished", "green")


cli.add_command(scan)
cli.add_command(init_database)
cli.add_command(submit)

if __name__ == "__main__":
    cli()
    # debug()
