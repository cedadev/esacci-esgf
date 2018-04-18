"""
Parse a THREDDS catalog and find paths of all referenced NcML aggregations.
Print the path of each NcML file relative to the aggregation dir root.
"""
import sys
import re
import os
import argparse
import xml.etree.cElementTree as ET

from modify_catalogs import REMOTE_AGGREGATIONS_DIR


def find_ncml_references(catalog_filename):
    """
    Find <netcdf> elements and extract paths from their 'location' attributes
    """
    tree = ET.ElementTree()
    tree.parse(catalog_filename)
    root = tree.getroot()

    # Regex to optionally match namspace in tag name
    netcdf_tag_regex = re.compile("({[^}]+})?netcdf")
    els = (el for el in root.iter() if re.fullmatch(netcdf_tag_regex, el.tag))

    for el in els:
        path = el.attrib["location"]
        yield os.path.relpath(path, REMOTE_AGGREGATIONS_DIR)

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
