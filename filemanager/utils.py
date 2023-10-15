import hashlib

import psutil


def get_optimal_chunk_size():
    mem = psutil.virtual_memory()
    available_memory = mem.available
    return int(available_memory * 0.5)


def hash(path):
    hash = hashlib.md5()
    # consider available memory to decide chunk size
    # this is useful for big files
    chunk_size = get_optimal_chunk_size()
    with open(path, "rb") as file:
        for chunk in iter(lambda: file.read(chunk_size), b""):
            hash.update(chunk)
    return hash.hexdigest()
