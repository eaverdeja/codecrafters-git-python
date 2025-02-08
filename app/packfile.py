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
    def __init__(self, file: BinaryIO, debug: bool = False):
        self.file = file
        self.offset = 0
        self.debug = debug

    def log(self, message):
        if self.debug:
            print(f"DEBUG: {message}")

    def read_bytes(self, n: int) -> bytes:
        data = self.file.read(n)
        if not data and n > 0:
            raise EOFError("Unexpected end of file")
        self.offset += len(data)
        return data

    def read_pktline(self) -> bytes:
        """Read a packet line according to Git's pkt-line format."""
        length_hex = self.read_bytes(4).decode("ascii")
        try:
            length = int(length_hex, 16)
        except ValueError:
            raise ValueError(f"Invalid packet line length: {length_hex}")

        if length == 0:
            return b""  # Flush packet
        if length < 4:
            raise ValueError(f"Invalid packet line length: {length}")

        content = self.read_bytes(length - 4)
        self.log(f"Read pktline: {length_hex} -> {content!r}")

        return content

    def skip_until_pack(self):
        """Skip through pktlines until we find the PACK signature."""
        while True:
            data = self.file.read(4)
            if not data:
                raise ValueError("Reached end of file without finding PACK signature")
            if data == b"PACK":
                self.file.seek(-4, 1)
                self.log(f"Found PACK signature at offset {self.file.tell()}")
                return
            self.file.seek(-3, 1)

    def parse_header(self) -> Tuple[str, int, int]:
        """Parse the packfile header and return signature and version."""
        pktline = self.read_pktline()
        if not pktline.startswith(b"packfile\n"):
            raise ValueError(f"Expected packfile announcement, got: {pktline!r}")

        self.skip_until_pack()

        signature = self.read_bytes(4)
        if signature != b"PACK":
            raise ValueError(f"Invalid packfile signature: {signature!r}")

        version = struct.unpack(">I", self.read_bytes(4))[0]
        if version not in (2, 3):
            raise ValueError(f"Unsupported packfile version: {version}")

        num_objects = struct.unpack(">I", self.read_bytes(4))[0]

        self.log(f"Header parse complete: version={version}, objects={num_objects}")

        return signature.decode(), version, num_objects

    def read_varint(self) -> Tuple[int, str]:
        """Read a variable-length integer and object type."""
        self.log(f"Starting varint read at offset {self.offset}")
        byte = self.read_bytes(1)[0]
        type_id = (byte >> 4) & 7
        size = byte & 0x0F
        shift = 4

        while byte & 0x80:
            byte = self.read_bytes(1)[0]
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

        obj_type = types.get(type_id, f"unknown_{type_id}")
        self.log(f"Read varint: type={obj_type}, size={size}")

        return size, obj_type

    def read_compressed_data(self) -> bytes:
        """Read zlib compressed data from the current position."""
        self.log(f"Starting compressed data read at offset {self.offset}")
        # Save start position for debugging
        start_pos = self.file.tell()

        # Initialize zlib decompressor
        decompressor = zlib.decompressobj()
        data = b""

        # First, try reading in larger chunks for efficiency
        chunk_size = 1024
        while True:
            chunk = self.file.read(chunk_size)
            if not chunk:
                break

            try:
                data += decompressor.decompress(chunk)
                if decompressor.eof:
                    # Found end of compressed data
                    unused = len(decompressor.unused_data)
                    if unused:
                        # Seek back by the number of unused bytes
                        self.file.seek(-unused, 1)
                    break
            except zlib.error:
                # If we fail with a large chunk, fall back to byte-by-byte
                self.file.seek(-len(chunk), 1)  # Go back
                break

        # If we didn't find the end, try byte by byte
        if not decompressor.eof:
            decompressor = zlib.decompressobj()
            data = b""
            while True:
                byte = self.file.read(1)
                if not byte:
                    break

                try:
                    data += decompressor.decompress(byte)
                    if decompressor.eof:
                        break
                except zlib.error:
                    self.file.seek(-1, 1)  # Go back one byte
                    break

        end_pos = self.file.tell()
        self.log(
            f"Compressed data read from {start_pos} to {end_pos} ({end_pos - start_pos} bytes)"
        )
        self.log(f"Decompressed to {len(data)} bytes")

        self.offset = end_pos
        return data

    def parse_object(self) -> PackObject:
        """Parse a single object from the packfile."""
        self.log(f"\nParsing object at offset {self.offset}")
        start_offset = self.offset
        size, obj_type = self.read_varint()

        # For delta objects, read the base offset/reference
        base_info = None
        if obj_type == "ofs_delta":
            self.log("Reading offset delta base info")
            offset = 0
            shift = 0
            while True:
                byte = self.read_bytes(1)[0]
                offset |= (byte & 0x7F) << shift
                shift += 7
                if not (byte & 0x80):
                    break
            base_info = -offset
            self.log(f"Delta base offset: {base_info}")
        elif obj_type == "ref_delta":
            self.log("Reading ref delta base info")
            base_info = self.read_bytes(20)  # Base object SHA-1
            self.log(f"Delta base ref: {base_info.hex()}")

        # Read and decompress the object data
        data = self.read_compressed_data()

        if len(data) != size:
            self.log(f"Warning: Size mismatch - expected {size}, got {len(data)}")

        return PackObject(type=obj_type, size=size, data=data, offset=start_offset)

    def parse_objects(self) -> Generator[PackObject, None, None]:
        """Parse all objects in the packfile."""
        signature, version, num_objects = self.parse_header()
        self.log(f"Packfile version {version} with {num_objects} objects")

        for _ in range(num_objects):
            yield self.parse_object()


def parse_packfile(data: bytes) -> None:
    """Parse a packfile and print information about its contents."""
    with BytesIO(data) as stream:
        parser = PackfileParser(stream)
        for obj in parser.parse_objects():
            print(f"  Type: {obj.type}")
            print(f"  Data: {obj.data!r}")
            print()
