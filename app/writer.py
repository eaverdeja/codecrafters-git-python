import os
import zlib
from hashlib import sha1

from app.encoder import encode_object


def write_contents_to_disk(contents: bytes, object_type: str) -> str:
    blob_object = encode_object(contents, object_type)
    object_hash = sha1(blob_object).hexdigest()

    folder = object_hash[0:2]
    filename = object_hash[2:]
    if not os.path.isdir(f".git/objects/{folder}"):
        os.mkdir(f".git/objects/{folder}")

    compressed_object = zlib.compress(blob_object)
    with open(f".git/objects/{folder}/{filename}", "wb") as file:
        file.write(compressed_object)

    return object_hash
