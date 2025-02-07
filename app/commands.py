from argparse import ArgumentParser
import os
from hashlib import sha1
from dataclasses import dataclass
import sys
import zlib

from app.encoder import encode_object
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

    content_length = len(contents)
    blob_object = b"blob " + str(content_length).encode() + b"\0" + contents
    object_hash = sha1(blob_object).hexdigest()

    if args.write:
        folder = object_hash[0:2]
        filename = object_hash[2:]
        if not os.path.isdir(f".git/objects/{folder}"):
            os.mkdir(f".git/objects/{folder}")

        compressed_object = zlib.compress(blob_object)
        with open(f".git/objects/{folder}/{filename}", "wb") as file:
            file.write(compressed_object)

    return object_hash


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
    Just compiling some info and docs for now:
    https://app.codecrafters.io/courses/git/stages/mg6
    https://forum.codecrafters.io/t/step-for-git-clone-implementing-the-git-protocol/4407/3
    https://i27ae15.github.io/git-protocol-doc/docs/git-protocol/intro
    """
    ...


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
