import sqlite3
from typing import Dict, List


class Database:
    TABLENAME = "files"
    FIELDS = (
        "id",
        "md5",
        "path",
        "ctime",
        "st_atime_ns",
        "st_mtime_ns",
        "st_ctime_ns",
        "st_size",
    )

    def __init__(self, db_path="data.sqlite"):
        self.db_path = db_path
        self.init_table()

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
            insert_sql = (
                "INSERT INTO files (md5, path, ctime, st_atime_ns, st_mtime_ns, st_ctime_ns, st_size) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)"
            )
            insert_row = (
                item["md5"],
                item["path"],
                item["earliest_timestamp"],
                item["stat"]["st_atime_ns"],
                item["stat"]["st_mtime_ns"],
                item["stat"]["st_ctime_ns"],
                item["stat"]["st_size"],
            )
            cursor.execute("SELECT * FROM files WHERE md5 = ?", (item["md5"],))
            # [1:] to remove id
            exist = any(row[1:] == insert_row for row in cursor.fetchall())
            if not exist:  # insert if no duplicate
                cursor.execute(insert_sql, insert_row)

        conn.commit()
        conn.close()
        item_num_new = self.count_all()
        print("Update {} items (now: {})".format(item_num_new - item_num, item_num_new))

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