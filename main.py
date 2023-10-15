import argparse
from pathlib import Path

import click

from filemanager.scan import scan_directory


@click.group(help="scan file information")
def cli():
    pass


@click.command()
@click.option("-d", "--directory", default=".", help="The folder to scan")
def scan(directory):
    if not Path(directory).exists():
        click.echo("directory does not exist!")
        return
    scan_directory(directory, save=True)
    click.echo(f"Scan {directory} finished")


cli.add_command(scan)

if __name__ == "__main__":
    cli()
