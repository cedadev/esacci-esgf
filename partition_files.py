#!/usr/bin/env python
import os
import sys


def partition_files(file_list):
    """
    Partition `file_list` into one or more sub-lists such that the filenames
    in each sub-list differ only by having a different date in the file path.
    """
    d = {}

    for path in file_list:
        # Create a key for each file by replacing date parts of path with 0s.
        # Paths with the same key will then be in the same partition

        # Discard basename (it is assumed that all files in the same directory
        # can be aggregated)
        components = path.split(os.path.sep)[:-1]
        for i, comp in enumerate(components):
            # Another assumption is that all dates take up a whole component
            # of the hierarchy, and conversely that any components consisting
            # of only digits is a date.
            if comp.isnumeric():
                components[i] = "0" * len(comp)

        replaced_path = os.path.sep.join(components)
        if replaced_path not in d:
            d[replaced_path] = []
        d[replaced_path].append(path)

    return d.values()


def usage(exit_code):
    """
    Print usage and exit with the given exit code
    """
    prog = os.path.basename(sys.argv[0])
    usage = """Usage: {} DIR

Read file paths from standard input and partition into sets such that paths
in each set only differ by having a different date in the path.

Write partitioned file lists to DIR/1, DIR/2 etc...

  -h, --help    Display help and exit"""
    print(usage.format(prog))
    sys.exit(exit_code)


def main(args):
    for arg in args:
        if arg in ("-h", "--help"):
            usage(0)

    try:
        output_dir = sys.argv[1]
    except IndexError:
        usage(1)

    # Show a warning if output directory is not empty. This is because it may
    # be misleading if a previous run had produced DIR/1, DIR/2 but this run
    # only produces DIR/1, for example
    if os.listdir(output_dir):
        print("Warning: Output directory '{}' is not empty".format(output_dir),
              file=sys.stderr)

    path_list = [line for line in sys.stdin.read().split(os.linesep) if line]
    partitions = partition_files(path_list)

    for i, paths in enumerate(partitions):
        out_path = os.path.join(output_dir, str(i + 1))
        with open(out_path, "w") as f:
            f.write(os.linesep.join(paths))
            f.write(os.linesep)
        print("Wrote {} paths to {}".format(len(paths), out_path))


if __name__ == "__main__":
    main(sys.argv[1:])
