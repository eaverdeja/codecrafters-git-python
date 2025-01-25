import sys
import os
import zlib
from hashlib import sha1
from argparse import ArgumentParser


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
            parser.add_argument('object_hash')
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
            
            with open(f".git/objects/{folder}/{filename}", 'rb') as file:
                compressed_contents = file.read()
                
            contents = zlib.decompress(compressed_contents)
            
            # Parse the content
            end_of_type_marker = contents.find(b" ")
            end_of_size_marker = contents.find(b"\x00")
            content_type = contents[: end_of_type_marker].decode()
            assert content_type == "blob"
            
            content_size = int(contents[end_of_type_marker + 1 : end_of_size_marker].decode())
            contents = contents[end_of_size_marker + 1 : end_of_size_marker + content_size + 1].decode()
            
            sys.stdout.write(contents)
        case "hash-object":
            parser = ArgumentParser(description="Computes the SHA hash of a git object. Optionally writes the object.")
            parser.add_argument('file_name')
            parser.add_argument(
                "--write",
                "-w",
                action="store_true",
                help="Specifies that the git object should be written to .git/objects",
            )
            args = parser.parse_args(sys.argv[2:])
            file_name = args.file_name

            with open(file_name, 'rb') as file:
                contents = file.read()
            
            content_length = len(contents)
            blob_object = b'blob ' + str(content_length).encode() + b'\0' + contents
            object_hash = sha1(blob_object).hexdigest()
            
            if args.write:
                folder = object_hash[0:2]
                filename = object_hash[2:]
                if not os.path.isdir(f".git/objects/{folder}"):
                    os.mkdir(f".git/objects/{folder}")
                
                compressed_object = zlib.compress(blob_object)
                with open(f".git/objects/{folder}/{filename}", 'wb') as file:
                    file.write(compressed_object)
            
            print(object_hash)
        case _:
            raise RuntimeError(f"Unknown command #{command}")


if __name__ == "__main__":
    main()
