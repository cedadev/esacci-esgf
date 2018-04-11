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
from configparser import ConfigParser

import psycopg2

from modify_catalogs import ProcessBatch


class CatalogGetter(object):
    def __init__(self, esg_ini, output_dir, ncml_dir):
        self.output_dir = output_dir
        self.ncml_dir = ncml_dir

        # Parse esg.ini config file
        config = ConfigParser()
        config.read(esg_ini)
        self.dburl = config["DEFAULT"]["dburl"]
        # This is the directory that paths in the DB are relative to
        self.thredds_root = config["DEFAULT"]["thredds_root"]

    def get_catalog_locations(self, ds_names):
        """
        Return a dictionary mapping dataset name to path of the corresponding
        THREDDS catalog produced by the ESGF publisher
        """
        conn = psycopg2.connect(self.dburl)
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

    def get_and_modify(self, json_filename):
        """
        Parse a JSON file to get dataset names, retrieve the associated
        catalogs and modify them as necessary
        """
        with open(json_filename) as f:
            json_doc = json.load(f)

        ds_names = json_doc.keys()
        cat_locations = self.get_catalog_locations(ds_names)

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
            # thredds catalog root so that the links in the top-level catalog
            # are correct when catalogs are moved.
            #
            # Thus take the directory name from the cat_log and append it to
            # output dir
            output_dir = os.path.join(self.output_dir, os.path.dirname(cat_loc))
            if not os.path.isdir(output_dir):
                os.mkdir(output_dir)

            options = ["--output-dir", output_dir, "--ncml-dir", self.ncml_dir]
            if info["generate_aggregation"]:
                options.append("--aggregate")

                if info["include_in_wms"]:
                    options.append("--wms")

            options.append(os.path.join(self.thredds_root, cat_loc))
            pb = ProcessBatch(options)
            pb.do_all()


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
    parser.add_argument(
        "-e", "--esg-ini",
        required=True,
        help="Path to esg.ini containing DB connection URL and THREDDS "
             "catalog root directory"
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
    getter = CatalogGetter(args.esg_ini, args.output_dir, args.ncml_dir)
    getter.get_and_modify(args.input_json)


if __name__ == "__main__":
    main(sys.argv[1:])
