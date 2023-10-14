import argparse
from pathlib import Path

from filemanager.scan import scan_directory


def init_argsparse():
    parser = argparse.ArgumentParser(description="scan file information")
    parser.add_argument("-d", "--directory", type=str, default=".")
    args = parser.parse_args()

    assert Path(args.directory).exists(), "directory does not exist!"
    return args


if __name__ == "__main__":
    args = init_argsparse()
    scan_directory(directory=args.directory, save=True)
