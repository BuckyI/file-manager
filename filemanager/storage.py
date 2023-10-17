import os
import sqlite3
from contextlib import closing
from typing import Dict, List, Tuple

from .datatype import File


class Database:
    TABLENAME = "files"
    FIELDS = {
        "id": "INTEGER",
        "md5": "TEXT",
        "path": "TEXT",
        "ctime": "REAL",
        "st_atime_ns": "INTEGER",
        "st_mtime_ns": "INTEGER",
        "st_ctime_ns": "INTEGER",
        "st_size": "INTEGER",
    }
    field_sql = "md5, path, ctime, st_atime_ns, st_mtime_ns, st_ctime_ns, st_size"

    def __init__(self, db_path="data.sqlite"):
        self.db_path = db_path
        if os.path.exists(db_path):
            assert self.validate_database(
                db_path
            ), "you might connected the wrong database"
        self.connection = sqlite3.connect(db_path)
        self.init_table()

    def __del__(self):
        self.connection.close()

    @classmethod
    def validate_database(cls, db_path: str):
        "validate if an existing database suits this class"
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            if (cls.TABLENAME,) not in cursor.fetchall():
                return False

            cursor.execute("PRAGMA table_info({})".format(cls.TABLENAME))
            fields = {col[1]: col[2] for col in cursor.fetchall()}
            if fields != cls.FIELDS:
                return False

        return True

    def init_table(self):
        cursor = self.connection.cursor()
        # init
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                md5 TEXT,
                path TEXT,
                ctime REAL,
                st_atime_ns INTEGER,
                st_mtime_ns INTEGER,
                st_ctime_ns INTEGER,
                st_size INTEGER
            )
            """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_files_md5 ON files (md5)")
        self.connection.commit()
        cursor.close()

    def count_items(self):
        with closing(self.connection.cursor()) as cursor:
            cursor.execute("SELECT COUNT(*) FROM files")
            total_rows = cursor.fetchone()[0]
            return total_rows

    def update_from_dicts(self, data: List[Dict]):
        required_keys = {"md5", "path", "earliest_timestamp", "stat"}
        assert len(data) and required_keys.issubset(data[0]), "invalid source"
        item_num = self.count_items()

        cursor = self.connection.cursor()
        for item in data:
            insert_row = (
                item["md5"],
                item["path"],
                item["earliest_timestamp"],
                item["stat"]["st_atime_ns"],
                item["stat"]["st_mtime_ns"],
                item["stat"]["st_ctime_ns"],
                item["stat"]["st_size"],
            )

            # prevent duplicate
            lookup_sql = f"SELECT {self.field_sql} FROM files WHERE md5 = ?"
            cursor.execute(lookup_sql, (item["md5"],))
            if any(row == insert_row for row in cursor.fetchall()):
                continue

            insert_sql = (
                f"INSERT INTO files ({self.field_sql}) VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            cursor.execute(insert_sql, insert_row)

        self.connection.commit()
        cursor.close()

        item_num_new = self.count_items()
        print("Update {} items (now: {})".format(item_num_new - item_num, item_num_new))

    def update_from_database(self, db_path: str):
        "merge an existing database into this one"
        update_count = 0
        if not self.validate_database(db_path):
            return update_count

        self_cursor = self.connection.cursor()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT {} FROM files".format(self.field_sql))
        field = self.field_sql.split(", ")
        for insert_row in cursor.fetchall():
            # prevent duplicate
            existed_rows = self.select(dict(zip(field, insert_row)))
            if len(existed_rows):
                continue

            insert_sql = (
                f"INSERT INTO files ({self.field_sql}) VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            self_cursor.execute(insert_sql, insert_row)
            update_count += 1

        self.connection.commit()
        self_cursor.close()
        conn.close()
        return update_count

    def get_duplicated_items(self):
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT md5, COUNT(*) FROM files GROUP BY md5 HAVING COUNT(*) > 1"
        )
        rows = cursor.fetchall()
        cursor.close()
        return rows

    def delete(self, ids: List[int]):
        with closing(self.connection.cursor()) as cursor:
            for id in ids:
                cursor.execute("DELETE FROM files WHERE id = ?", (id,))
            self.connection.commit()

    def select(self, conditions: dict) -> List[Tuple]:
        conditions = {f"{k} = ?": v for k, v in conditions.items() if k in self.FIELDS}
        if not conditions:
            print("no conditions specified")
            return []
        where_query = "SELECT * FROM files WHERE " + " AND ".join(conditions.keys())
        where_param = tuple(conditions.values())

        with closing(self.connection.cursor()) as cursor:
            cursor.execute(where_query, where_param)
            return cursor.fetchall()

    def check_file_exist(self, file: File, fast: bool = True) -> bool:
        """
        fast: roughly check if `file` has been recorded by time info and path
        no fast: check by md5
        """

        filestat = file.path.stat()
        if fast:
            info = {
                "st_size": filestat.st_size,
                "st_mtime_ns": filestat.st_mtime_ns,
                "st_ctime_ns": filestat.st_ctime_ns,
            }
        else:
            info = {"md5": file.hash()}
        return bool(len(self.select(info)))
