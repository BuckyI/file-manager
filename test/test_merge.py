import sqlite3

from filemanager.storage import Database

db = Database("test/data.sqlite")
db_path = "test/data.sqlite"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute("SELECT {} FROM files".format(Database.field_sql))
ls = cursor.fetchall()
insert_row = ls[0]
print(insert_row)

rows = db.lookup(insert_row[0])
print(rows)
any(row[1:] == insert_row for row in rows)
