def encode_object(contents: bytes, object_type: str) -> bytes:
    return (
        object_type.encode() + b" " + str(len(contents)).encode() + b"\x00" + contents
    )
