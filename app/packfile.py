from io import BytesIO
import zlib
import struct
from typing import BinaryIO, Tuple, Generator
from dataclasses import dataclass

from app.git_object import create_git_object
from app.pkt_line import encode_pkt_line
from app.protocol_v2 import v2_protocol_request


@dataclass
class PackObject:
    type: str
    size: int
    data: bytes
    offset: int
    base_sha: str | None = None


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
        base_sha = None
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
            base_sha = self._read_bytes(20).hex()  # Base object SHA-1
            print("base sha1 from ref_delta: ", base_sha)

        # Read and decompress the object data
        data = self._read_compressed_data()

        return PackObject(
            type=obj_type,
            size=size,
            data=data,
            offset=start_offset,
            base_sha=base_sha,
        )

    def _resolve_deltas(self, objects: list[PackObject]) -> list[PackObject]:
        """Resolve all delta objects iteratively."""
        base_objects = {}
        delta_objects: list[PackObject] = []

        # Debug: Print all object types we receive
        for obj in objects:
            if obj.type == "ofs_delta":
                # Skip OFS_DELTA objects
                continue
            elif obj.type == "ref_delta":
                delta_objects.append(obj)
            else:
                # Store the SHA-1 of this object
                obj_sha = create_git_object(obj.data, obj.type, should_write=False)
                base_objects[obj_sha] = obj

        print(f"\nNumber of base objects: {len(base_objects)}")
        print(f"Number of delta objects to resolve: {len(delta_objects)}")

        # Keep processing deltas until we can't resolve any more
        iteration = 0
        while delta_objects:
            iteration += 1
            print(f"\nIteration {iteration}:")
            made_progress = False
            remaining_deltas = []

            for delta in delta_objects:
                print(f"Attempting to resolve delta with base SHA: {delta.base_sha}")
                base_obj = base_objects.get(delta.base_sha or "")
                if base_obj is None:
                    print(f"Base object not found for SHA: {delta.base_sha}")
                    remaining_deltas.append(delta)
                    continue

                # Resolve the delta
                resolved_data = self._apply_delta(base_obj.data, delta.data)
                resolved_obj = PackObject(
                    type=base_obj.type,
                    size=len(resolved_data),
                    data=resolved_data,
                    offset=delta.offset,
                )

                # Store the resolved object as a potential base for other deltas
                obj_sha = create_git_object(
                    resolved_data, base_obj.type, should_write=False
                )
                base_objects[obj_sha] = resolved_obj
                made_progress = True
                print(f"Successfully resolved delta, new object SHA: {obj_sha}")

            print(f"Remaining deltas after iteration: {len(remaining_deltas)}")

            # Update our list of deltas to process
            delta_objects = remaining_deltas

            # If we made no progress in this iteration and there are still deltas,
            # then we have truly unresolvable deltas
            if not made_progress and delta_objects:
                print("\nFailed to resolve these deltas' base SHAs:")
                for delta in delta_objects:
                    print(f"- {delta.base_sha}")
                raise ValueError("Could not resolve all delta objects")

        # Return all resolved objects
        return list(base_objects.values())

    def _apply_delta(self, base_data: bytes, delta_data: bytes) -> bytes:
        source_size = 0
        target_size = 0
        pos = 0

        # source size
        shift = 0
        while True:
            byte = delta_data[pos]
            source_size |= (byte & 0x7F) << shift
            pos += 1
            if not (byte & 0x80):
                break
            shift += 7

        # target size
        shift = 0
        while True:
            byte = delta_data[pos]
            target_size |= (byte & 0x7F) << shift
            pos += 1
            if not (byte & 0x80):
                break
            shift += 7

        if len(base_data) != (source_size):
            raise ValueError(
                f"Base object size mismatch: expected {source_size} got {len(base_data)}"
            )

        result = bytearray()

        while pos < len(delta_data):
            cmd = delta_data[pos]
            pos += 1

            if cmd & 0x80:  # Copy
                copy_offset = 0
                copy_size = 0

                if cmd & 0x01:
                    copy_offset = delta_data[pos]
                    pos += 1
                if cmd & 0x02:
                    copy_offset |= delta_data[pos] << 8
                    pos += 1
                if cmd & 0x04:
                    copy_offset |= delta_data[pos] << 16
                    pos += 1
                if cmd & 0x08:
                    copy_offset |= delta_data[pos] << 24
                    pos += 1

                if cmd & 0x10:
                    copy_size = delta_data[pos]
                    pos += 1
                if cmd & 0x20:
                    copy_size |= delta_data[pos] << 8
                    pos += 1
                if cmd & 0x40:
                    copy_size |= delta_data[pos] << 16
                    pos += 1

                if copy_size == 0:
                    copy_size = 0x10000

                result.extend(base_data[copy_offset : copy_offset + copy_size])
            else:  # Insert
                result.extend(delta_data[pos : pos + cmd])
                pos += cmd

        if len(result) != target_size:
            breakpoint()
            raise ValueError(
                f"Target object size mismatch: expected {target_size} got {len(result)}"
            )

        return result

    def parse_objects(self) -> Generator[PackObject, None, None]:
        """Parse all objects in the packfile."""
        _signature, _version, num_objects = self._parse_header()

        objects = [self._parse_object() for _ in range(num_objects)]
        for obj in self._resolve_deltas(objects):
            yield obj


def parse_packfile(data: bytes) -> list[PackObject]:
    """Parse a packfile and print information about its contents."""
    with BytesIO(data) as stream:
        parser = PackfileParser(stream)
        return list(parser.parse_objects())


def fetch_packfile(url: str, ref: str) -> bytes:
    data = encode_pkt_line("command=fetch")
    data += "0001"  # section marker
    data += encode_pkt_line("no-progress")
    data += encode_pkt_line(f"want {ref}")
    data += "0000"  # end of data

    return v2_protocol_request(
        url=f"{url}/git-upload-pack", method="POST", data=data.encode()
    )
