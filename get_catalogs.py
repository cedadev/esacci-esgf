#!/usr/bin/env python3
"""
Get dataset names from a JSON file, find the associated THREDDS catalogs, and
run `modify_catalogs.py' on each with appropriate arguments.

The JSON input file should be in the format as required by `make_mapfiles.py'.
"""
import os
import sys
import argparse
import json
import psycopg2

from modify_catalogs import ProcessBatch


DB_NAME = "esgcet"
DB_USER = "esgcet"
DB_HOST = "localhost"

# The path that the locations of THREDDS catalogs in the publiation database
# are relative to
THREDDS_CATALOGS_ROOT = "/esg/content/thredds/esgcet"


def get_catalog_locations(ds_names):
    """
    Return a dictionary mapping dataset name to path of the corresponding
    THREDDS catalog produced by the ESGF publisher
    """
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST)
    cursor = conn.cursor()
    cursor.execute("SELECT dataset_name, version, location FROM catalog;")

    locations = {}

    for name, version, location in cursor:
        # Name from JSON file contains version whereas name in DB does not,
        # since several versions of a dataset may exist. Thus we lookup the
        # concatenated name and version
        versioned_ds_name = "{}.v{}".format(name, version)

        if versioned_ds_name in ds_names:
            locations[versioned_ds_name] = location

    return locations


def main(arg_list):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "input_json",
        help="JSON file containing IDs of datasets to obtain the modified "
             "catalog for"
    )
    # These arguments are passed straight through to modiy_catalogs
    parser.add_argument(
        "-o", "--output-dir",
        dest="output_dir",
        required=True,
        help="Directory to write modified catalog(s) to"
    )
    parser.add_argument(
        "-n", "--ncml-dir",
        dest="ncml_dir",
        required=True,
        help="Directory to write NcML aggregations to"
    )

    args = parser.parse_args(arg_list)

    with open(args.input_json) as f:
        json_doc = json.load(f)

    ds_names = json_doc.keys()
    cat_locations = get_catalog_locations(ds_names)

    # Print a warning if not all datasets in JSON were found in the DB
    not_found = set(ds_names) - set(cat_locations.keys())
    if not_found:
        print("WARNING: Failed to find the following datasets in the DB:",
              file=sys.stderr)
        for ds_name in sorted(not_found):
            print(ds_name, file=sys.stderr)
        print("", file=sys.stderr)

    for ds_name, cat_loc in cat_locations.items():
        info = json_doc[ds_name]
        # Need to preserve the directory structure found under
        # THREDDS_CATALOGS_ROOT so that the links in the top-level catalog are
        # correct when catalogs are moved.
        #
        # Thus take the directory name from the cat_log and append it to output
        # dir
        output_dir = os.path.join(args.output_dir, os.path.dirname(cat_loc))
        if not os.path.isdir(output_dir):
            os.mkdir(output_dir)

        options = ["--output-dir", output_dir, "--ncml-dir", args.ncml_dir]
        if info["generate_aggregation"]:
            options.append("--aggregate")

            if info["include_in_wms"]:
                options.append("--wms")

        options.append(os.path.join(THREDDS_CATALOGS_ROOT,
                                    cat_locations[ds_name]))
        pb = ProcessBatch(options)
        pb.do_all()


if __name__ == "__main__":
    main(sys.argv[1:])
