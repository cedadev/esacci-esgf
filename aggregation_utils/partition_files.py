#!/usr/bin/env python3
"""
Read file paths from stdin and partition into sets such that paths in each set
only differ by having a different date in the directory components of the path.

Print the directory name for each group on stdout, with date characters
replaced with 'x'.
"""
import os
import sys
import argparse


def partition_files(file_list):
    """
    Partition `file_list` into one or more sub-lists such that the filenames
    in each sub-list differ only by having a different date in the file path.

    Return a dictionary mapping common names (dirnames with dates replaced
    with 'x') to lists of filenames.
    """
    d = {}

    for path in file_list:
        # Create a key for each file by replacing date parts of path with 'x'.
        # Paths with the same key will then be in the same partition

        # Discard basename (it is assumed that all files in the same directory
        # can be aggregated)
        components = path.split(os.path.sep)[:-1]
        for i, comp in enumerate(components):
            # Another assumption is that all dates take up a whole component
            # of the hierarchy, and conversely that any components consisting
            # of only digits is a date.
            if comp.isnumeric():
                components[i] = "x" * len(comp)

        replaced_path = os.path.sep.join(components)
        if replaced_path not in d:
            d[replaced_path] = []
        d[replaced_path].append(path)

    return d


def main(arg_list):
    # Use argparse just for consistency with other scripts and automatic help
    # text, even though this script takes no arguments...
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    _args = parser.parse_args(arg_list)

    path_list = [line for line in sys.stdin.read().split(os.linesep) if line]
    groups = partition_files(path_list)
    print(os.linesep.join(groups.keys()))


if __name__ == "__main__":
    main(sys.argv[1:])
