#!/usr/bin/env python3
"""
Parse a THREDDS catalog and list the references NetCDF files
"""

import sys
import argparse
import xml.etree.cElementTree as ET


def main(arg_list):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "catalog",
        help="THREDDS catalog to parse"
    )
    args = parser.parse_args(arg_list)

    tree = ET.ElementTree()
    tree.parse(args.catalog)
    cat = tree.getroot()
    top_level_ds = [el for el in cat if el.tag.endswith("dataset")][0]
    for el in top_level_ds:
        if el.tag.endswith("dataset"):
            path = el.get("urlPath")
            print(path.replace("esg_esacci", "/neodc/esacci"))


if __name__ == "__main__":
    main(sys.argv[1:])
