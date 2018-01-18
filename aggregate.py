#!/usr/bin/env python

import sys
from getopt import getopt, GetoptError

import xml.etree.cElementTree as ET


def usage(exit_code):
    """
    Print usage and exit with the given exit code
    """
    usage = """Usage: {} [OPTION] [FILE_LIST]

Read filenames of datasets from FILE_LIST and print an NcML aggregation to
standard output. If FILE_LIST is not specified then read filenames from
standard input.

  -s, --split    Split input files into groups of files with similar names to
                 create several aggregations (for when the input file list is
                 heterogeneous)
  -h, --help     Display help and exit"""
    print(usage.format(sys.argv[0]))
    sys.exit(exit_code)


def get_file_list(stream):
    """
    Return a list of non-empty lines from a stream.
    """
    return [line for line in stream.read().split("\n") if line]


def element_to_string(element, indentation=0):
    """
    Return a string representation of an ET.Element object with indentation and
    line breaks.

    `indentation` is how many levels to indent the returned string (2 spaces
    per level).
    """
    children = ""
    for child in element:
        children += element_to_string(child, indentation=indentation + 1)
        children += "\n"

    indentation_str = " " * (2 * indentation)
    elem_str = "{ind}<{tag}".format(ind=indentation_str, tag=element.tag)

    attrs = " ".join('{}="{}"'.format(key, value) for key, value in element.items())
    if attrs:
        elem_str += " " + attrs

    if children:
        elem_str += ">"
        elem_str += "\n" + children
        elem_str += "</{tag}>".format(tag=element.tag)
    else:
        elem_str += "/>"

    # If this is the top level then include <xml> root
    if indentation == 0:
        elem_str = '<?xml version="1.0" encoding="UTF-8"?>\n' + elem_str
    return elem_str


def write_aggregation(file_list, output_stream):
    """
    Create an NcML aggregation for the filenames in `file_list` and write the
    output to `output_stream`.
    """
    root = ET.Element("netcdf", xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2")
    aggregation = ET.SubElement(root, "aggregation", dimName="time", type="joinExisting")

    ET.SubElement(aggregation, "variableAgg", name="time")

    for filename in file_list:
        ET.SubElement(aggregation, "netcdf", location=filename)

    output_stream.write(element_to_string(root))


def main(args):
    try:
        opts, args = getopt(args, "hs", longopts=["help", "split"])
    except GetoptError:
        usage(1)

    split = False

    for opt, value in opts:
        if opt in ("-h", "--help"):
            usage(0)
        elif opt in ("-s", "--split"):
            split = True

    if split:
        raise NotImplementedError("split is not implemented yet")

    input_files = None
    if not args or args[0] == "-":
        input_files = get_file_list(sys.stdin)
    else:
        with open(args[0]) as f:
            input_files = get_file_list(f)

    write_aggregation(input_files, sys.stdout)


if __name__ == "__main__":
    main(sys.argv[1:])
