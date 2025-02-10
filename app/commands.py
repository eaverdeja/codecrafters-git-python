from argparse import ArgumentParser
import os
from hashlib import sha1
from dataclasses import dataclass
from pprint import pprint
import sys
import zlib

from app.encoder import encode_object
from app.git_object import create_git_object
from app.packfile import fetch_packfile, parse_packfile
from app.pkt_line import decode_pkt_line, encode_pkt_line
from app.protocol_v2 import v2_protocol_request
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

    return create_git_object(contents, "blob", args.write)


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
    # Initialize the directory
    os.mkdir(directory)
    os.chdir(directory)
    init()

    # Discover references
    head_ref = ls_remote_head(url)

    # Fetch packfile
    packfile = fetch_packfile(url, head_ref)

    # Parse the packfile
    pack_objects = parse_packfile(packfile)

    # Create the git objects
    for obj in pack_objects:
        create_git_object(obj.data, obj.type, should_write=True)

    # Update HEAD reference
    ref_path = f".git/refs/heads/main"
    os.makedirs(os.path.dirname(ref_path), exist_ok=True)
    with open(ref_path, "w") as f:
        f.write(head_ref + "\n")

    # Checkout the HEAD reference
    checkout(head_ref)


def ls_remote_head(url: str) -> str:
    response = v2_protocol_request(
        url=f"{url}/info/refs?service=git-upload-pack", method="GET"
    )
    lines = response.split(b"\n")

    first_line, _ = decode_pkt_line(lines[0])
    assert first_line == "# service=git-upload-pack"
    second_line, _ = decode_pkt_line(lines[1])
    assert second_line is None

    # Iterate for sanity's sake, but skip capability advertisement as a whole
    for line in lines[2:]:
        parsed_line, _ = decode_pkt_line(line)
        if parsed_line is None:
            break

    # What we want is *reference advertisement*,
    # which in the v2 protocol is achieved with ls-refs
    data = encode_pkt_line("command=ls-refs")
    data += "0000"  # end of data

    response = v2_protocol_request(
        url=f"{url}/git-upload-pack", method="POST", data=data.encode()
    )

    for line in response.split(b"\n"):
        if b"HEAD" in line:
            # SHA hash is 40 bytes in length
            return line[4 : 40 + 4].decode()

    raise Exception("HEAD ref not found!")


def checkout(commit_sha1: str) -> None:
    type_str, data = _read_git_object(commit_sha1)
    if type_str != "commit":
        raise ValueError(f"Expected commit object, got {type_str}")

    # A commit object's data looks like something like:
    # tree 4e2e00e78d5e3ba51ad9f1846890328fb74d94a3
    # author eaverdeja <eaverdeja@gmail.com> 1739006338 -0300
    # committer eaverdeja <eaverdeja@gmail.com> 1739006338 -0300
    # msg
    commit_lines = data.decode().splitlines()
    tree_line = next(line for line in commit_lines if line.startswith("tree "))
    tree_sha1 = tree_line.split()[1]

    _checkout_tree(tree_sha1)


def _checkout_tree(tree_sha1: str, path: str = "") -> None:
    type_str, data = _read_git_object(tree_sha1)
    if type_str != "tree":
        raise ValueError(f"Expected tree object, got {type_str}")

    entries = _parse_tree(data)

    for mode, name, sha1 in entries:
        full_path = os.path.join(path, name)

        if mode == "40000":
            os.makedirs(full_path, exist_ok=True)
            _checkout_tree(sha1, full_path)
        else:
            type_str, data = _read_git_object(sha1)

            if type_str != "blob":
                raise ValueError(f"Expected blob object, got {type_str}")

            with open(full_path, "wb") as f:
                f.write(data)

            os.chmod(full_path, int(mode, 8))


def _read_git_object(sha1: str) -> tuple[str, bytes]:
    obj_path = f".git/objects/{sha1[:2]}/{sha1[2:]}"
    with open(obj_path, "rb") as file:
        compressed = file.read()

    raw = zlib.decompress(compressed)

    header_end = raw.index(b"\x00")
    header = raw[:header_end].decode()
    type_str, size_str = header.split()

    content = raw[header_end + 1 :]

    return type_str, content


def _parse_tree(data: bytes) -> list[tuple[str, str, str]]:
    entries = []
    i = 0

    # A tree object looks something like this:
    # {mode} {name}{hash}
    # 100644 bar\x00\xe5\nI\xf9U\x8d\t\xd4\xd3\xbf\xc1\x086;\xb2L\x12~\xd2c
    # 100644 foo\x00\x92\x9e\xfb0SE\x98SQ\x98p\x0b\x99N\xe48\xd4A\xd1\xaf
    while i < len(data):
        # Find the null byte separating the mode/name from the SHA-1 hash
        null_pos = data.index(b"\x00", i)

        mode_name = data[i:null_pos].decode()
        mode, name = mode_name.split(" ")
        sha1 = data[null_pos + 1 : null_pos + 1 + 20].hex()

        entries.append((mode, name, sha1))
        i = null_pos + 1 + 20
    return entries


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
