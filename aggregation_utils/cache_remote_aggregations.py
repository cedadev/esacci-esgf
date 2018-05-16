#!/usr/bin/env python3
"""
Send HTTP requests to OPeNDAP/WMS endpoints for aggregate datasets on a remote
THREDDS server so that they are cached for future access.

This script will not wait for each request to complete, so the aggregations
will not necessarily be cached immediately after the script terminates.

JSON input should be of the following form:
{
    "<dataset ID>": {
        "generate_aggregation": <boolean>,
        "include_in_wms": <boolean>
    },
    ...
}
(any extra keys in the inner dictionaries are ignored)
"""
import sys
import json
import argparse

import requests


class AggregationCacher(object):
    def __init__(self, input_json, base_thredds_url, verbose=False):
        if base_thredds_url.endswith("/"):
            base_thredds_url = base_thredds_url[:-1]

        self.base_thredds_url = base_thredds_url
        self.verbose = verbose

        with open(input_json) as f:
            self.json_doc = json.load(f)

    def aggregation_url(self, ds_id, wms=False):
        """
        Return the URL to an OPenDAP or WMS endpoint for a dataset
        """
        if wms:
            service = "wms"
            suffix = "?service=WMS&version=1.3.0&request=GetCapabilities"
        else:
            service = "dodsC"
            suffix = ".dds"

        url = "{base}/{service}/{ds_id}{suffix}"
        return url.format(base=self.base_thredds_url, service=service,
                          ds_id=ds_id, suffix=suffix)

    def get_all_urls(self):
        """
        Return a generator containing URLs of aggregation endpoints for
        all applicable datasets
        """
        for ds_id, ds_info in self.json_doc.items():
            if ds_info["generate_aggregation"]:
                yield self.aggregation_url(ds_id, wms=ds_info["include_in_wms"])

    def cache_all(self):
        """
        Send HTTP requests to each aggregation endpoint and do not wait for a
        response
        """
        for url in self.get_all_urls():
            if self.verbose:
                print(url)
            # Set the 'read' timeout very small so that we do not wait for the
            # request to finish, as caching the aggregations can take a long
            # time
            try:
                requests.get(url, timeout=(10, 0.001))
            except requests.exceptions.ReadTimeout:
                pass


def main(arg_list):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "input_json",
        help="JSON file containing IDs of datasets"
    )
    parser.add_argument(
        "base_thredds_url",
        help="Base URL of the THREDDS server hosting aggregations"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        default=False,
        help="Print aggregation URLs as they are requested"
    )

    args = parser.parse_args(arg_list)
    agg_cacher = AggregationCacher(args.input_json, args.base_thredds_url,
                                   args.verbose)
    agg_cacher.cache_all()


if __name__ == "__main__":
    main(sys.argv[1:])
