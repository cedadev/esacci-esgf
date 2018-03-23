#!/usr/bin/env python3
import os
import sys
import operator
import bisect
import xml.etree.cElementTree as ET

from netCDF4 import Dataset


def usage(exit_code):
    """
    Print usage and exit with the given exit code
    """
    prog = os.path.basename(sys.argv[0])
    usage = """Usage: {}

Read filenames of datasets from standard input and print an NcML aggregation to
standard output.

  -h, --help     Display help and exit"""
    print(usage.format(prog))
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
        elem_str += "{ind}</{tag}>".format(ind=indentation_str, tag=element.tag)
    else:
        elem_str += "/>"

    # If this is the top level then include <?xml?> element
    if indentation == 0:
        prolog = '<?xml version="1.0" encoding="UTF-8"?>'
        elem_str = prolog + os.linesep + elem_str
    return elem_str


def get_coord_value(filename, dimension):
    """
    Return (units, value) of the coordinate variable for the given dimension
    in a NetCDF file.

    Raises AssertionError if the coordinate contains multiple values
    """
    ds = Dataset(filename)
    var = ds.variables[dimension]
    assert var.shape == (1,)
    val = var[0]
    try:
        units = var.units
    except AttributeError:
        units = None
    ds.close()
    return (units, val)


def create_aggregation(file_list):
    """
    Create an NcML aggregation for the filenames in `file_list` and return the
    root element as an instance of ET.Element
    """
    agg_dimension = "time"

    root = ET.Element("netcdf", xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2")
    aggregation = ET.SubElement(root, "aggregation", dimName=agg_dimension, type="joinExisting")

    coord_values = []
    # Keep track of whether the files seen have the same units
    found_units = set([])
    multiple_units = False

    for filename in file_list:
        units, value = get_coord_value(filename, agg_dimension)

        # Insert whilst preserving sort order (sorted by value)
        bisect.insort(coord_values, (value, filename))

        if not multiple_units:
            found_units.add(units)
            if len(found_units) > 1:
                multiple_units = True

    for coord_value, filename in coord_values:
        kwargs = {"location": filename}
        if not multiple_units:
            kwargs["coordValue"] = str(coord_value)

        ET.SubElement(aggregation, "netcdf", **kwargs)

    if multiple_units:
        aggregation.attrib["timeUnitsChange"] = "true"

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
