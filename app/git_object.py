import os
import zlib
from hashlib import sha1


def create_git_object(contents: bytes, obj_type: str, should_write: bool = True) -> str:
    content_length = len(contents)
    blob_object = (
        obj_type.encode() + b" " + str(content_length).encode() + b"\0" + contents
    )
    object_hash = sha1(blob_object).hexdigest()

    if should_write:
        folder = object_hash[:2]
        filename = object_hash[2:]
        os.makedirs(f".git/objects/{folder}", exist_ok=True)

        compressed_object = zlib.compress(blob_object)
        with open(f".git/objects/{folder}/{filename}", "wb") as file:
            file.write(compressed_object)

    return object_hash
