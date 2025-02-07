from argparse import ArgumentParser
import sys

from app.commands import cat_file, commit_tree, hash_object, init, ls_tree, write_tree


def main():
    # https://blog.meain.io/2023/what-is-in-dot-git/

    command = sys.argv[1]
    match command:
        case "init":
            init()
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

            contents = cat_file(object_hash)

            sys.stdout.write(contents)
        case "hash-object":
            object_hash = hash_object()

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

            tree_entries = ls_tree(tree_hash)

            if args.name_only:
                for entry in sorted(tree_entries, key=lambda entry: entry.name):
                    print(entry.name)
        case "write-tree":
            object_hash = write_tree()

            print(object_hash)
        case "commit-tree":
            parser = ArgumentParser(description="Creates a commit object")
            parser.add_argument("tree_sha")
            parser.add_argument(
                "-p",
                "--parent",
                help="Specifies the parent commit",
            )
            parser.add_argument(
                "-m",
                "--message",
                help="The commit message",
            )
            args = parser.parse_args(sys.argv[2:])
            tree_sha = args.tree_sha
            parent = args.parent
            message = args.message

            object_hash = commit_tree(tree_sha, parent, message)

            print(object_hash)
        case _:
            raise RuntimeError(f"Unknown command #{command}")


if __name__ == "__main__":
    main()
