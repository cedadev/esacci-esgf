#!/usr/bin/env python3
"""
Query the publication database to find the path of a THREDDS catalog relative
to the THREDDS root
"""
import sys
import argparse

from esacci_esgf.get_catalogs import CatalogGetter


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "dataset_name",
        help="Versioned dataset name to lookup - e.g. my.dataset.v1234"
    )
    parser.add_argument(
        "-e", "--esg-ini",
        required=True,
        help="Path to esg.ini containing DB connection URL"
    )

    args = parser.parse_args(sys.argv[1:])
    getter = CatalogGetter(args.esg_ini)
    locations = getter.get_catalog_locations(args.dataset_name)

    if args.dataset_name not in locations:
        sys.exit(1)

    print(locations[args.dataset_name])
