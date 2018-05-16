#!/usr/bin/env python3
"""
Read filenames of NetCDF datasets from standard input and print an NcML
aggregation to standard output.
"""
import os
import sys
import bisect
import xml.etree.cElementTree as ET
import argparse

from netCDF4 import Dataset


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
    try:
        var = ds.variables[dimension]
    except KeyError:
        raise AggregationError("Variable '{}' not found in file '{}'".format(dimension, filename))

    expected_shape = (1,)
    if var.shape != expected_shape:
        raise AggregationError("Shape of time variable in file '{}' is {} - should be {}"
                               .format(filename, var.shape, expected_shape))

    val = var[0]
    try:
        units = var.units
    except AttributeError:
        units = None
    ds.close()
    return (units, val)


def create_aggregation(file_list, agg_dimension, cache=False):
    """
    Create a 'joinExisting' NcML aggregation for the filenames in `file_list`
    and return the root element as an instance of ET.Element.

    If `cache` is True then open each file to write the coordinate values in
    the NcML.
    """
    root = ET.Element("netcdf", xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2")
    aggregation = ET.SubElement(root, "aggregation", dimName=agg_dimension, type="joinExisting")

    sub_el_attrs = []

    if cache:
        coord_values = []
        # Keep track of whether the files seen have the same units
        found_units = set([])
        multiple_units = False

        for filename in file_list:
            try:
                units, value = get_coord_value(filename, agg_dimension)
            except AggregationError as ex:
                print("WARNING: {}".format(ex), file=sys.stderr)
                continue

            # Insert whilst preserving sort order (sorted by value)
            bisect.insort(coord_values, (value, filename))

            if not multiple_units:
                found_units.add(units)
                if len(found_units) > 1:
                    multiple_units = True

        if not coord_values:
            raise AggregationError("No aggregation could be created")

        for coord_value, filename in coord_values:
            attrs = {"location": filename}
            if not multiple_units:
                attrs["coordValue"] = str(coord_value)
            sub_el_attrs.append(attrs)

        if multiple_units and agg_dimension == "time":
            aggregation.attrib["timeUnitsChange"] = "true"

    # If not caching coordinate values then include in the order given
    else:
        sub_el_attrs = [{"location": filename} for filename in file_list]

    for attrs in sub_el_attrs:
        ET.SubElement(aggregation, "netcdf", **attrs)
    return root


class AggregationError(Exception):
    """
    Custom exception to indicate that aggregation creation has failed
    """


def main(arg_list):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        "-d", "--dimension",
        default="time",
        help="The dimension along which to aggregate [default: %(default)s]"
    )
    parser.add_argument(
        "-c", "--cache",
        action="store_true",
        default=False,
        help="Open NetCDF files to read coordinate values to include in the "
             "NcML. This caches the values so that TDS does not need to open "
             "each file when accessing the aggregation [default: %(default)s]"
    )

    args = parser.parse_args(arg_list)

    path_list = [line for line in sys.stdin.read().split(os.linesep) if line]
    ncml_el = create_aggregation(path_list, args.dimension, cache=args.cache)
    print(element_to_string(ncml_el))


if __name__ == "__main__":
    main(sys.argv[1:])
