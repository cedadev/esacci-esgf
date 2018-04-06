#!/usr/bin/env python3
"""
Create mapfiles for use with the ESGF publisher from a JSON file of the form
{
    "<dataset name>.v<version>": {
        "generate_aggregation": <boolean>,
        "include_in_wms": <boolean>,
        "tech_note_url": "<url>",
        "tech_note_title": "<title>",
        "files": [
            {
                "path": "<path>",
                "sha256": "<checksum>",
                "mtime": "<mtime>",
                "size": "<size in bytes>"
            },
            ...
        ]
    },
    ...
}


Mapfiles are written to <output dir>/<prefix>/metadata/mapfiles/by_name/A/B/C/D/E/
where the data files are under /<prefix>/data/ and A.B.C.D.E are the leading
elements of the DRS.

By default version is taken from the current date. The paths of the output
mapfiles are written on stdout.

(Adapted from https://github.com/cedadev/esgf-processing/blob/76f6ce8/misc_tasks/make_esacci_mapfiles_from_json.py)
"""
import sys
import argparse
import json
import os
import re


class MakeMapfile(object):

    versioned_dsid_regex = re.compile("(.*)\.v([0-9]+)$")

    def __init__(self, depth=5, out_root=None):
        self.depth = depth
        self.out_root = out_root

    def parse_json(self, filename):
        f = open(filename)
        content = f.read()
        j = json.loads(content)
        f.close()
        return j

    def get_mapfile_root_one_file(self, path):
        bits = path.split("/")[1:]
        assert bits[0] == "neodc"
        assert bits[1] == "esacci"
        assert bits[3] == "data"
        return "/neodc/esacci/{}/metadata/mapfiles/by_name".format(bits[2])

    def get_mapfile_root(self, file_dicts):
        roots = set()
        for d in file_dicts:
            roots.add(self.get_mapfile_root_one_file(d["path"]))
        assert len(roots) == 1  # all files in dset must be under same data dir
        return list(roots)[0]

    def get_mapfile_path(self, dsid, file_dicts):
        mapfile_root = self.get_mapfile_root(file_dicts)
        path_els = ([self.out_root, mapfile_root]
                    + dsid.split('.')[:self.depth]
                    + [dsid])
        path = os.path.join(*path_els)
        if self.out_root:
            assert path.startswith("/")
            path = self.out_root + path
        return path

    def get_mapfile_line(self, unversioned_dsid, version, file_dict):
        line = ("{dsid}#{version} | {path} | {size} | mod_time={mtime:.5f} | "
                "checksum={sha256} | checksum_type=SHA256\n")
        return line.format(dsid=unversioned_dsid, version=version, **file_dict)

    def make_mapfile(self, dsid, file_dicts):
        path = self.get_mapfile_path(dsid, file_dicts)
        content = ""
        unversioned_dsid, version = self.split_versioned_dsid(dsid)
        for file_dict in file_dicts:
            content += self.get_mapfile_line(unversioned_dsid, version,
                                             file_dict)
        self.write_file(path, content)
        print(path)

    def write_file(self, path, content):
        parent = os.path.dirname(path)
        if not os.path.isdir(parent):
            os.makedirs(parent)
        f = open(path, "w")
        f.write(content)
        f.close()

    def split_versioned_dsid(self, dsid):
        """
        Split a versioned dataset id and return (unversioned id, version)
        """
        match = self.versioned_dsid_regex.match(dsid)
        if not match:
            err = "Dataset ID '{}' does not contain a version number"
            raise ValueError(err.format(dsid))

        return (match.group(1), match.group(2))

    def make_mapfiles(self, filename):
        j = self.parse_json(filename)
        for dsid, ds_dict in j.items():
            self.make_mapfile(dsid, ds_dict["files"])


def main(arg_list):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "input_json",
        help="JSON file to generate mapfile from"
    )
    parser.add_argument(
        "-o", "--output-dir",
        dest="output_dir",
        default=None,
        help="Directory to save mapfile(s) in. If omitted save directly in "
             "the `metadata' directory next to the directory containing the "
             "data files"
    )

    args = parser.parse_args(arg_list)
    mm = MakeMapfile(out_root=args.output_dir)
    mm.make_mapfiles(args.input_json)


if __name__ == "__main__":
    main(sys.argv[1:])
