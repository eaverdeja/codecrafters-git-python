from io import BytesIO
import zlib
import struct
from typing import BinaryIO, Tuple, Generator
from dataclasses import dataclass


@dataclass
class PackObject:
    type: str
    size: int
    data: bytes
    offset: int


class PackfileParser:
    def __init__(
        self,
        stream: BinaryIO,
    ):
        self.stream = stream
        self.offset = 0

    def _read_bytes(self, n: int) -> bytes:
        data = self.stream.read(n)
        if not data and n > 0:
            raise EOFError("Unexpected end of file")
        self.offset += len(data)
        return data

    def _read_pktline(self) -> bytes:
        """Read a packet line according to Git's pkt-line format."""
        length_hex = self._read_bytes(4).decode("ascii")
        try:
            length = int(length_hex, 16)
        except ValueError:
            raise ValueError(f"Invalid packet line length: {length_hex}")

        if length == 0:
            return b""  # Flush packet
        if length < 4:
            raise ValueError(f"Invalid packet line length: {length}")

        content = self._read_bytes(length - 4)

        return content

    def _skip_until_pack(self):
        """Skip through pktlines until we find the PACK signature."""
        while True:
            data = self.stream.read(4)
            if not data:
                raise ValueError("Reached end of file without finding PACK signature")
            if data == b"PACK":
                self.stream.seek(-4, 1)
                return
            self.stream.seek(-3, 1)

    def _parse_header(self) -> Tuple[str, int, int]:
        """Parse the packfile header and return signature and version."""
        pktline = self._read_pktline()
        if not pktline.startswith(b"packfile\n"):
            raise ValueError(f"Expected packfile announcement, got: {pktline!r}")

        self._skip_until_pack()

        signature = self._read_bytes(4)
        if signature != b"PACK":
            raise ValueError(f"Invalid packfile signature: {signature!r}")

        version = struct.unpack(">I", self._read_bytes(4))[0]
        if version not in (2, 3):
            raise ValueError(f"Unsupported packfile version: {version}")

        num_objects = struct.unpack(">I", self._read_bytes(4))[0]

        return signature.decode(), version, num_objects

    def _read_varint(self) -> Tuple[int, str]:
        """Read a variable-length integer and object type."""
        byte = self._read_bytes(1)[0]
        type_id = (byte >> 4) & 7
        size = byte & 0x0F
        shift = 4

        while byte & 0x80:
            byte = self._read_bytes(1)[0]
            size |= (byte & 0x7F) << shift
            shift += 7

        types = {
            1: "commit",
            2: "tree",
            3: "blob",
            4: "tag",
            6: "ofs_delta",
            7: "ref_delta",
        }

        return size, types.get(type_id, f"unknown_{type_id}")

    def _read_compressed_data(self) -> bytes:
        """Read zlib compressed data from the current position."""
        decompressor = zlib.decompressobj()
        data = b""
        while True:
            byte = self.stream.read(1)
            if not byte:
                break
            try:
                data += decompressor.decompress(byte)
                if decompressor.eof:
                    break
            except zlib.error:
                self.stream.seek(-1, 1)  # Go back one byte
                break

        return data

    def _parse_object(self) -> PackObject:
        """Parse a single object from the packfile."""
        start_offset = self.offset
        size, obj_type = self._read_varint()

        # For delta objects, read the base offset/reference
        if obj_type == "ofs_delta":
            offset = 0
            shift = 0
            while True:
                byte = self._read_bytes(1)[0]
                offset |= (byte & 0x7F) << shift
                shift += 7
                if not (byte & 0x80):
                    break
        elif obj_type == "ref_delta":
            self._read_bytes(20)  # Base object SHA-1

        # Read and decompress the object data
        data = self._read_compressed_data()

        return PackObject(type=obj_type, size=size, data=data, offset=start_offset)

    def parse_objects(self) -> Generator[PackObject, None, None]:
        """Parse all objects in the packfile."""
        signature, version, num_objects = self._parse_header()

        for _ in range(num_objects):
            yield self._parse_object()


def parse_packfile(data: bytes) -> list[PackObject]:
    """Parse a packfile and print information about its contents."""
    with BytesIO(data) as stream:
        parser = PackfileParser(stream)
        return list(parser.parse_objects())
