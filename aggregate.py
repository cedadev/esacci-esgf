#!/usr/bin/env python
import os
import sys

import xml.etree.cElementTree as ET


def usage(exit_code):
    """
    Print usage and exit with the given exit code
    """
    usage = """Usage: {}

Read filenames of datasets from standard input and print an NcML aggregation to
standard output.

  -h, --help     Display help and exit"""
    print(usage.format(sys.argv[0]))
    sys.exit(exit_code)


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
        children += os.linesep

    indentation_str = " " * (2 * indentation)
    elem_str = "{ind}<{tag}".format(ind=indentation_str, tag=element.tag)

    attrs = " ".join('{}="{}"'.format(key, value) for key, value in element.items())
    if attrs:
        elem_str += " " + attrs

    if children:
        elem_str += ">"
        elem_str += os.linesep + children
        elem_str += "</{tag}>".format(tag=element.tag)
    else:
        elem_str += "/>"

    # If this is the top level then include <?xml?> element
    if indentation == 0:
        prolog = '<?xml version="1.0" encoding="UTF-8"?>'
        elem_str = prolog + os.linesep + elem_str
    return elem_str


def create_aggregation(file_list):
    """
    Create an NcML aggregation for the filenames in `file_list` and return the
    root element as an instance of ET.Element
    """
    root = ET.Element("netcdf", xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2")
    aggregation = ET.SubElement(root, "aggregation", dimName="time", type="joinExisting")
    ET.SubElement(aggregation, "variableAgg", name="time")
    for filename in file_list:
        ET.SubElement(aggregation, "netcdf", location=filename)
    return root


def main(args):
    for arg in args:
        if arg in ("-h", "--help"):
            usage(0)

    path_list = [line for line in sys.stdin.read().split(os.linesep) if line]
    ncml_el = create_aggregation(path_list)
    print(element_to_string(ncml_el))


if __name__ == "__main__":
    main(sys.argv[1:])
