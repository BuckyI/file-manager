import hashlib
import os
from datetime import datetime
from pathlib import Path


class File:
    "read local files and extract essential infos"

    def __init__(self, file_path: str | Path):
        self.path = Path(file_path)
        assert self.path.exists(), f"File at {file_path} not exist!"

    def hash(self) -> str:
        hash = hashlib.md5()
        with open(self.path, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                hash.update(chunk)
        return hash.hexdigest()

    def earliest_timestamp(self) -> float:
        stat = self.path.stat()
        return min(stat.st_atime, stat.st_mtime, stat.st_ctime)

    @staticmethod
    def filesize2str(size: int) -> str:
        "size: size of file in bytes"
        units = ["B", "KB", "MB", "GB", "TB"]
        index = 0
        while size > 1024:
            size /= 1024
            index += 1
        return f"{size:.2f} {units[index]}"

    @staticmethod
    def timestamp2str(timestamp: int | float) -> str:
        return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def stat2dict(stat: os.stat_result) -> dict:
        return {k: getattr(stat, k) for k in dir(stat) if k.startswith("st_")}

    def to_dict(self) -> dict:
        info = {
            "md5": self.hash(),
            "path": str(self.path.absolute()),
            "earliest_timestamp": self.earliest_timestamp(),
            "stat": self.stat2dict(self.path.stat()),
        }
        return info

    def to_str(self) -> str:
        "human readable description of a file"
        info = self.to_dict()
        infos = (
            "File:",
            "Hash: {}".format(info["md5"]),
            "Location: {}".format(info["path"]),
            "Time: {}".format(self.timestamp2str(info["earliest_timestamp"])),
            "Size: {}".format(self.filesize2str(info["stat"]["st_size"])),
        )
        return "\n".join(infos)
