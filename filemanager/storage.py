import os
import sqlite3
from typing import Dict, List


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
            assert self.validate(db_path), "you might connected the wrong database"
        self.init_table()

    @classmethod
    def validate(cls, db_path: str):
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
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
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
        conn.commit()
        conn.close()

    def count_all(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM files")
        total_rows = cursor.fetchone()[0]
        conn.close()
        return total_rows

    def update(self, data: List[Dict]):
        required_keys = {"md5", "path", "earliest_timestamp", "stat"}
        assert len(data) and required_keys.issubset(data[0]), "invalid source"
        item_num = self.count_all()

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
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

        conn.commit()
        conn.close()
        item_num_new = self.count_all()
        print("Update {} items (now: {})".format(item_num_new - item_num, item_num_new))

    def merge(self, db_path: str):
        "merge an existing database into this one"
        update_count = 0
        if not self.validate(db_path):
            return update_count
        main_conn = sqlite3.connect(self.db_path)
        main_cursor = main_conn.cursor()
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT {} FROM files".format(self.field_sql))
        for insert_row in cursor.fetchall():
            # prevent duplicate
            lookup_sql = f"SELECT {self.field_sql} FROM files WHERE md5 = ?"
            main_cursor.execute(lookup_sql, (insert_row[0],))
            if any(row == insert_row for row in main_cursor.fetchall()):
                continue

            insert_sql = (
                f"INSERT INTO files ({self.field_sql}) VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            main_cursor.execute(insert_sql, insert_row)
            update_count += 1
        return update_count

    def show_all(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM files")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        conn.close()
        return rows

    def show_duplicate(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT md5, COUNT(*) FROM files GROUP BY md5 HAVING COUNT(*) > 1"
        )
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        conn.close()
        return rows

    def delete(self, ids: List[int]):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        for id in ids:
            cursor.execute("DELETE FROM files WHERE id = ?", (id,))
        conn.commit()
        conn.close()

    def lookup(self, md5: str):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM files WHERE md5 = ?", (md5,))
        rows = cursor.fetchall()
        conn.close()
        return rows
