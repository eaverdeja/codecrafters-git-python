from argparse import ArgumentParser
import os
from hashlib import sha1
from dataclasses import dataclass
import sys
from typing import Any
import zlib
import urllib.request

from app.encoder import encode_object
from app.packfile import parse_packfile
from app.writer import write_contents_to_disk


@dataclass
class TreeEntry:
    mode: str
    name: str
    sha_hash: str

    def to_bytes(self) -> bytes:
        return (
            self.mode.encode()
            + b" "
            + self.name.encode()
            + b"\x00"
            + bytes.fromhex(self.sha_hash)
        )


def init() -> None:
    os.mkdir(".git")
    os.mkdir(".git/objects")
    os.mkdir(".git/refs")
    with open(".git/HEAD", "w") as f:
        f.write("ref: refs/heads/main\n")


def cat_file(object_hash: str) -> str:
    folder = object_hash[0:2]
    filename = object_hash[2:]

    with open(f".git/objects/{folder}/{filename}", "rb") as file:
        compressed_contents = file.read()

    content_bytes = zlib.decompress(compressed_contents)

    # Parse the content
    end_of_type_marker = content_bytes.find(b" ")
    end_of_size_marker = content_bytes.find(b"\x00")
    content_type = content_bytes[:end_of_type_marker].decode()
    assert content_type == "blob"

    content_size = int(
        content_bytes[end_of_type_marker + 1 : end_of_size_marker].decode()
    )
    contents = content_bytes[
        end_of_size_marker + 1 : end_of_size_marker + content_size + 1
    ].decode()

    return contents


def hash_object() -> str:
    parser = ArgumentParser(
        description="Computes the SHA hash of a git object. Optionally writes the object."
    )
    parser.add_argument("file_name")
    parser.add_argument(
        "--write",
        "-w",
        action="store_true",
        help="Specifies that the git object should be written to .git/objects",
    )
    args = parser.parse_args(sys.argv[2:])
    file_name = args.file_name

    with open(file_name, "rb") as file:
        contents = file.read()

    return _create_git_object(contents, "blob", args.write)


def ls_tree(tree_hash: str) -> list[TreeEntry]:
    # Fetch the trees contents from the filesystem
    folder = tree_hash[0:2]
    filename = tree_hash[2:]
    with open(f".git/objects/{folder}/{filename}", "rb") as file:
        compressed_contents = file.read()
    contents = zlib.decompress(compressed_contents)

    # Parse the content type and make sure it's a tree
    end_of_type_marker = contents.find(b" ")
    end_of_size_marker = contents.find(b"\x00")
    content_type = contents[:end_of_type_marker].decode()
    assert content_type == "tree"

    # Parse the entries
    content_size = int(contents[end_of_type_marker + 1 : end_of_size_marker].decode())
    entries = contents[end_of_size_marker + 1 : end_of_size_marker + content_size + 1]
    parsed_entries: list[TreeEntry] = []
    pos = 0
    while entries := entries[pos:]:
        end_of_mode_marker = entries.find(b" ")
        end_of_name_marker = entries.find(b"\x00")

        mode = entries[:end_of_mode_marker].decode()
        name = entries[end_of_mode_marker + 1 : end_of_name_marker].decode()
        # The SHA hash is 20 bytes in length
        sha_hash = entries[end_of_name_marker + 1 : end_of_name_marker + 20 + 1]

        # +2 for the control markers
        pos = len(mode) + len(name) + len(sha_hash) + 2
        parsed_entries.append(TreeEntry(mode=mode, name=name, sha_hash=sha_hash.hex()))

    return parsed_entries


def write_tree(path: str | None = None) -> str:
    tree = _create_tree(path)
    _, contents = _encode_tree(tree)
    object_hash = write_contents_to_disk(contents, "tree")

    return object_hash


def commit_tree(tree_sha: str, parent: str, message: str) -> str:
    contents = (
        b"tree "
        + str(tree_sha).encode()
        + b"\n"
        + b"parent "
        + str(parent).encode()
        + b"\n"
        + b"author eaverdeja <eaverdeja@gmail.com> 1738923862 -0300\n"
        + b"committer eaverdeja <eaverdeja@gmail.com> 1738923862 -0300\n"
        + b"\n"
        + str(message).encode()
        + b"\n"
    )
    object_hash = write_contents_to_disk(contents, "commit")
    return object_hash


def clone(url: str, directory: str):
    """
    Useful docs and links:
    https://git-scm.com/docs/gitprotocol-v2
    https://app.codecrafters.io/courses/git/stages/mg6
    https://forum.codecrafters.io/t/step-for-git-clone-implementing-the-git-protocol/4407/3
    https://stefan.saasen.me/articles/git-clone-in-haskell-from-the-bottom-up
    https://i27ae15.github.io/git-protocol-doc/docs/git-protocol/intro
    https://codewords.recurse.com/issues/three/unpacking-git-packfiles
    """
    # CD to directory and run init
    os.mkdir(directory)
    os.chdir(directory)
    init()

    # Discover references
    head_ref = ls_remote_head(url)

    # Fetch packfile
    packfile = fetch_packfile(url, head_ref)

    # Parse the packfile
    pack_objects = parse_packfile(packfile)

    # Create git objects. These need to be done sequentially
    # in order to ensure proper references from commit -> tree -> blob
    for obj in pack_objects:
        if obj.type == "blob":
            _create_git_object(obj.data, "blob")

    for obj in pack_objects:
        if obj.type == "tree":
            _create_git_object(obj.data, "tree")

    for obj in pack_objects:
        if obj.type == "commit":
            _create_git_object(obj.data, "commit")

    # Update HEAD ref
    ref_path = f".git/refs/heads/main"
    os.makedirs(os.path.dirname(ref_path), exist_ok=True)
    with open(ref_path, "w") as f:
        f.write(head_ref + "\n")


def ls_remote_head(url: str) -> str:
    response = _v2_protocol_request(
        url=f"{url}/info/refs?service=git-upload-pack", method="GET"
    )
    lines = response.split(b"\n")

    first_line, _ = _decode_pkt_line(lines[0])
    assert first_line == "# service=git-upload-pack"
    second_line, _ = _decode_pkt_line(lines[1])
    assert second_line is None

    # Iterate for sanity's sake, but skip capability advertisement as a whole
    for line in lines[2:]:
        parsed_line, _ = _decode_pkt_line(line)
        if parsed_line is None:
            break

    # What we want is *reference advertisement*,
    # which in the v2 protocol is achieved with ls-refs
    data = _encode_pkt_line("command=ls-refs")
    data += "0000"  # end of data

    response = _v2_protocol_request(
        url=f"{url}/git-upload-pack", method="POST", data=data.encode()
    )

    for line in response.split(b"\n"):
        if b"HEAD" in line:
            # SHA hash is 40 bytes in length
            return line[4 : 40 + 4].decode()

    raise Exception("HEAD ref not found!")


def fetch_packfile(url: str, ref: str) -> bytes:
    data = _encode_pkt_line("command=fetch")
    data += "0001"  # section marker
    data += _encode_pkt_line("no-progress")
    data += _encode_pkt_line(f"want {ref}")
    data += "0000"  # end of data

    response = _v2_protocol_request(
        url=f"{url}/git-upload-pack", method="POST", data=data.encode()
    )
    return response


def _create_git_object(
    contents: bytes, obj_type: str, should_write: bool = True
) -> str:
    content_length = len(contents)
    blob_object = (
        obj_type.encode() + b" " + str(content_length).encode() + b"\0" + contents
    )
    object_hash = sha1(blob_object).hexdigest()

    if should_write:
        folder = object_hash[0:2]
        filename = object_hash[2:]
        if not os.path.isdir(f".git/objects/{folder}"):
            os.mkdir(f".git/objects/{folder}")

        compressed_object = zlib.compress(blob_object)
        with open(f".git/objects/{folder}/{filename}", "wb") as file:
            file.write(compressed_object)

    return object_hash


def _encode_pkt_line(line: str) -> str:
    # +4 to include the padded length in the count
    length = "{0:x}".format(len(line) + 4)

    return f"{length.zfill(4)}{line}"


def _decode_pkt_line(line: bytes) -> tuple[str | None, int]:
    length = int(line[:4], 16)
    if length == 0:
        return None, 0

    return line[4:length].decode(), length


def _v2_protocol_request(url: str, method: str, data: Any | None = None) -> bytes:
    headers = {"git-protocol": "version=2"}
    request = urllib.request.Request(method=method, url=url, headers=headers, data=data)
    with urllib.request.urlopen(request) as response:
        return response.read()


def _create_tree(path: str | None) -> list[TreeEntry]:
    tree: list[TreeEntry] = []
    for entry in os.scandir(path):
        if entry.is_file():
            with open(entry.path, "rb") as file:
                contents = file.read()

            content_length = len(contents)
            blob_object = b"blob " + str(content_length).encode() + b"\0" + contents
            object_hash = sha1(blob_object).hexdigest()

            write_contents_to_disk(contents, "blob")

            tree.append(
                TreeEntry(
                    mode=_get_mode_for_entry(entry),
                    name=entry.name,
                    sha_hash=object_hash,
                )
            )
        elif entry.is_dir():
            if entry.name in [".git"]:
                continue

            object_hash = write_tree(entry.path)
            tree.append(
                TreeEntry(
                    mode=_get_mode_for_entry(entry),
                    name=entry.name,
                    sha_hash=object_hash,
                )
            )

    return tree


def _encode_tree(tree: list[TreeEntry]) -> tuple[bytes, bytes]:
    contents = b"".join(
        [entry.to_bytes() for entry in sorted(tree, key=lambda entry: entry.name)]
    )

    blob_object = encode_object(contents, "tree")
    object_hash = sha1(blob_object).digest()

    return object_hash, contents


def _get_mode_for_entry(entry: os.DirEntry) -> str:
    if entry.is_dir():
        return "40000"
    if entry.is_symlink():
        return "120000"
    if entry.is_file():
        if os.access(entry.path, os.X_OK):
            return "100755"
        return "100644"
    raise Exception("Invalid entry")
