from filemanager.storage import Database

db_path = "test/data.sqlite"
db = Database(db_path)
count = db.update_from_database(db_path)
print(count)
