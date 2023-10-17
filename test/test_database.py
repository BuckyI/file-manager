import json

from filemanager.storage import Database

db = Database("test/data.sqlite")

with open("test/data.json", "r", encoding="utf-8") as file:
    data = json.load(file)

print(db.count_items())

db.update_from_dicts(data["files"])
