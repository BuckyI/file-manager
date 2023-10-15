import argparse
import json
from pathlib import Path

import click

from filemanager.scan import scan_directory
from filemanager.storage import Database


@click.group(help="scan file information")
def cli():
    pass


@click.command()
@click.option(
    "-d",
    "--directory",
    default=".",
    type=click.Path(exists=True, file_okay=False),
    help="The folder to scan",
)
def scan(directory):
    if not Path(directory).exists():
        click.echo("directory does not exist!")
        return
    scan_directory(directory, save=True)
    click.echo(f"Scan {directory} finished")


@click.command()
@click.argument("database", type=click.Path())
def init_database(database):
    db = Database(database)
    click.echo("Initialized the database")


@click.command()
@click.option(
    "-d",
    "--database",
    default="data.sqlite",
    type=click.Path(exists=True, dir_okay=False),
    help="sqlite file",
)
@click.argument("source", type=click.Path(exists=True))
def submit(database, source):
    source = Path(source)
    # get scan result files
    if source.is_dir():
        files = list(source.rglob("filescan_*.json"))
        if not len(files):
            click.echo("no scan file found!")
            return
    else:
        files = [source]

    db = Database(database)
    for file in files:
        data = json.load(open(file, "r", encoding="utf-8"))
        db.update(data["files"])
        click.echo(f"Submit {str(file)} finished")
    click.echo("Finished")


cli.add_command(scan)
cli.add_command(init_database)
cli.add_command(submit)

if __name__ == "__main__":
    cli()
