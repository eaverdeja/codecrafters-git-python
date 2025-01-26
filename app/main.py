import sys
import os
import zlib
from hashlib import sha1
from argparse import ArgumentParser
from dataclasses import dataclass


@dataclass
class TreeEntry:
    mode: str
    name: str
    sha_hash: str


def main():
    # https://blog.meain.io/2023/what-is-in-dot-git/

    command = sys.argv[1]
    match command:
        case "init":
            os.mkdir(".git")
            os.mkdir(".git/objects")
            os.mkdir(".git/refs")
            with open(".git/HEAD", "w") as f:
                f.write("ref: refs/heads/main\n")
            print("Initialized git directory")
        case "cat-file":
            parser = ArgumentParser(description="Reads a git object")
            parser.add_argument("object_hash")
            parser.add_argument(
                "--content",
                "-p",
                action="store_true",
                help="Specifies that the content of the git object should be yielded",
            )
            args = parser.parse_args(sys.argv[2:])
            object_hash = args.object_hash
            folder = object_hash[0:2]
            filename = object_hash[2:]

            with open(f".git/objects/{folder}/{filename}", "rb") as file:
                compressed_contents = file.read()

            contents = zlib.decompress(compressed_contents)

            # Parse the content
            end_of_type_marker = contents.find(b" ")
            end_of_size_marker = contents.find(b"\x00")
            content_type = contents[:end_of_type_marker].decode()
            assert content_type == "blob"

            content_size = int(
                contents[end_of_type_marker + 1 : end_of_size_marker].decode()
            )
            contents = contents[
                end_of_size_marker + 1 : end_of_size_marker + content_size + 1
            ].decode()

            sys.stdout.write(contents)
        case "hash-object":
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

            print(object_hash)
        case "ls-tree":
            parser = ArgumentParser(
                description="Prints the structure of a tree object."
            )
            parser.add_argument("tree_hash")
            parser.add_argument(
                "--name-only",
                action="store_true",
                help="Specifies that only file/directory names should be outputted",
            )
            args = parser.parse_args(sys.argv[2:])
            tree_hash = args.tree_hash

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
            content_size = int(
                contents[end_of_type_marker + 1 : end_of_size_marker].decode()
            )
            entries = contents[
                end_of_size_marker + 1 : end_of_size_marker + content_size + 1
            ]
            parsed_entries = []
            pos = 0
            while entries := entries[pos:]:
                end_of_mode_marker = entries.find(b" ")
                end_of_name_marker = entries.find(b"\x00")

                mode = entries[:end_of_mode_marker]
                name = entries[end_of_mode_marker + 1 : end_of_name_marker].decode()
                # The SHA hash is 20 bytes in length
                sha_hash = entries[end_of_name_marker + 1 : end_of_name_marker + 20 + 1]

                # +2 for the control markers
                pos = len(mode) + len(name) + len(sha_hash) + 2
                parsed_entries.append(
                    TreeEntry(mode=mode, name=name, sha_hash=sha_hash)
                )

            # Print out entry names
            if args.name_only:
                for entry in sorted(parsed_entries, key=lambda entry: entry.name):
                    print(entry.name)
        case _:
            raise RuntimeError(f"Unknown command #{command}")


if __name__ == "__main__":
    main()
