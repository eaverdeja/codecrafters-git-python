def hex_dump(data: bytes, offset: int = 0) -> str:
    """Create a hex dump of the data for debugging."""
    result = []
    for i in range(0, len(data), 16):
        chunk = data[i : i + 16]
        hex_vals = " ".join(f"{b:02x}" for b in chunk)
        ascii_vals = "".join(chr(b) if 32 <= b <= 126 else "." for b in chunk)
        result.append(f"{offset+i:08x}  {hex_vals:<48}  |{ascii_vals}|")
    return "\n".join(result)
