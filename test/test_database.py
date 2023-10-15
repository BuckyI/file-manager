from filemanager.storage import Database
import json
db = Database("test/data.sqlite")

with open("test/data.json", "r", encoding="utf-8") as file:
    data = json.load(file)

print(db.count_all())

db.update(data['files'])
