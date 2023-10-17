import json

from filemanager.scan import scan_directory
from filemanager.storage import Database

# get a record
scan_directory("test", db_path="test/data.sqlite")

# update it into database
db = Database("test/data.sqlite")
data = json.load(open(r"test\filescan_20231017220058.json", "r", encoding="utf-8"))
db.update_from_dicts(data["files"])  # Update 5 items (now: 87)

# get another record
scan_directory("test", db_path="test/data.sqlite")  # no duplicate from database
