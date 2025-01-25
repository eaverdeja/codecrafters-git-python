import sys
import os
import zlib
from argparse import ArgumentParser
from pathlib import Path

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
            cwd = os.getcwd()
            
            with open(Path(cwd)/'.git/objects'/folder/filename, 'rb') as file:
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
        case _:
            raise RuntimeError(f"Unknown command #{command}")


if __name__ == "__main__":
    main()
