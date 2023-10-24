import datetime
import fnmatch
import json
import platform
from pathlib import Path
from typing import Set

from tqdm import tqdm

from .datatype import File
from .storage import Database


class ScanFilter:
    EXCLUDE = [
        ".*",  # hidden folders
        "__*__",  # __pycache__
        "*.exe",  # executables
    ]

    def __init__(self, *, db_path: str | None = None) -> None:
        self.db = None  # refer to database to filter items
        if db_path and Path(db_path).exists() and Database.validate_database(db_path):
            self.db = Database(db_path)

        # there exists many same string that is allowed
        self._cache_permit: Set[str] = set()

    def is_valid(self, path: Path) -> bool:
        if not path.is_file():  # not an existing file
            return False
        for part in path.parts:  # filter by path
            if part in self._cache_permit:
                continue
            for pattern in self.EXCLUDE:
                if fnmatch.fnmatch(part, pattern):
                    return False
            # skip items already in database
            if self.db and self.db.check_file_exist(File(path)):
                return False

            # finally, pass
            self._cache_permit.add(part)
        return True


def scan_directory(
    directory: str, *, save: bool = True, db_path: str | None = None
) -> dict:
    directory: Path = Path(directory)
    sf = ScanFilter(db_path=db_path)
    filedata = [
        File(path).to_dict()
        for path in tqdm(directory.rglob("*"), desc="Scanning", unit="files")
        if sf.is_valid(path)
    ]
    data = {
        "system": platform.platform(),
        "scan_directory": str(directory.absolute()),
        "files": filedata,
    }
    if save:
        json_data = json.dumps(data, ensure_ascii=False, indent=4)
        savepath = directory / "filescan_{}.json".format(
            datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        )
        savepath.write_text(json_data, encoding="utf8")
    return data
