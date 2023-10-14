import datetime
import fnmatch
import json
import platform
from pathlib import Path

from .datatype import File

EXCLUDE = [
    ".*",  # hidden folders
    "__*__",  # __pycache__
    "*.exe",  # executables
]


def is_valid(path: Path) -> bool:
    if not path.is_file():
        return False
    path = path.absolute()
    for pattern in EXCLUDE:
        for part in path.parts:
            if fnmatch.fnmatch(part, pattern):
                return False
    return True


def scan_directory(directory: str, *, save: bool = True):
    directory: Path = Path(directory)
    filedata = [File(path).to_dict() for path in directory.rglob("*") if is_valid(path)]
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
