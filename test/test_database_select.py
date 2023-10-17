from filemanager.datatype import File
from  filemanager.storage import Database

db  = Database('test/data.sqlite')
print(db.select({'md5': 'eac9bcdb93d8cbe0cfad5d42f6919273'}))

file = File('test/data.json')
filestat = file.path.stat()
info = {
    "st_size": filestat.st_size,
    "st_mtime_ns": filestat.st_mtime_ns,
    "st_ctime_ns": filestat.st_ctime_ns,
}

print(info)
print(db.select(info))
