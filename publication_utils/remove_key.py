#!/usr/bin/env python3
"""
Remove a key from the top level of a JSON dictionary, and print the new
dictionary to stdout.
"""
import sys
import argparse
import json


def main(arg_list):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "key",
        help="Key to remove from the JSON file"
    )
    parser.add_argument(
        "file",
        type=argparse.FileType("r"),
        help="Path to JSON file"
    )

    args = parser.parse_args(arg_list)
    doc = json.load(args.file)
    del doc[args.key]
    json.dump(doc, sys.stdout)


if __name__ == "__main__":
    main(sys.argv[1:])
