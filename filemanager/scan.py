import datetime
import fnmatch
import json
import platform
from pathlib import Path

from .datatype import File


class ScanFilter:
    EXCLUDE = [
        ".*",  # hidden folders
        "__*__",  # __pycache__
        "*.exe",  # executables
    ]

    def __init__(self) -> None:
        # there exists many same string that is allowed
        self._cache_permit = set()

    def is_valid(self, path: Path) -> bool:
        if not path.is_file():  # not an existing file
            return False
        for part in path.parts:
            if part in self._cache_permit:
                continue
            for pattern in self.EXCLUDE:
                if fnmatch.fnmatch(part, pattern):
                    return False
            else:
                self._cache_permit.add(part)
        return True


def scan_directory(directory: str, *, save: bool = True):
    directory: Path = Path(directory)
    sf = ScanFilter()
    filedata = [
        File(path).to_dict() for path in directory.rglob("*") if sf.is_valid(path)
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
