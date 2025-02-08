def encode_pkt_line(line: str) -> str:
    # +4 to include the padded length in the count
    length = "{0:x}".format(len(line) + 4)

    return f"{length.zfill(4)}{line}"


def decode_pkt_line(line: bytes) -> tuple[str | None, int]:
    length = int(line[:4], 16)
    if length == 0:
        return None, 0

    return line[4:length].decode(), length
