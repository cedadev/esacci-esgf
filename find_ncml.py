"""
Parse a THREDDS catalog and print paths of all referenced NcML aggregations to
stdout.
"""
import sys
import re
import os
import argparse
import xml.etree.cElementTree as ET

from xml_utils import find_by_tagname


def find_ncml_references(catalog_filename):
    """
    Find <netcdf> elements and extract paths from their 'location' attributes
    """
    for el in find_by_tagname(catalog_filename, "netcdf"):
        yield el.attrib["location"]


def main(arg_list):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "catalog",
        help="Path to THREDDS XML catalog"
    )

    args = parser.parse_args(arg_list)
    print(os.linesep.join(find_ncml_references(args.catalog)))


if __name__ == "__main__":
    main(sys.argv[1:])
