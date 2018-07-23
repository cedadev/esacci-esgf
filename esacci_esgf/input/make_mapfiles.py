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

Mapfiles are written to <output dir>/A/B/C/D/E/<dsid> where A.B.C.D.E are the
leading elements of the dataset ID.

The paths of the output mapfiles are written on stdout.

(Adapted from https://github.com/cedadev/esgf-processing/blob/76f6ce8/misc_tasks/make_esacci_mapfiles_from_json.py)
"""
import sys
import argparse
import json
import os
import re


class MakeMapfile(object):

    versioned_dsid_regex = re.compile("(.*)\.v([0-9]+)$")

    def __init__(self, out_root, depth=5):
        self.depth = depth
        self.out_root = out_root

    def parse_json(self, filename):
        f = open(filename)
        content = f.read()
        j = json.loads(content)
        f.close()
        return j

    def get_mapfile_path(self, dsid):
        path_els = [self.out_root] + dsid.split(".")[:self.depth] + [dsid]
        return os.path.join(*path_els)

    def get_mapfile_line(self, unversioned_dsid, version, file_dict,
                         tech_notes):
        try:
            parts = [
                "{dsid}#{version}".format(dsid=unversioned_dsid, version=version),
                file_dict["path"],
                str(file_dict["size"]),
                "mod_time={mtime:.5f}".format(mtime=file_dict["mtime"]),
                "checksum={sha256}".format(sha256=file_dict["sha256"]),
                "checksum_type=SHA256"
            ]
        except KeyError as ex:
            ds = "{}.v{}".format(unversioned_dsid, version)
            raise KeyError("Missing key for dataset '{}': {}".format(ds, ex))

        if tech_notes:
            parts += [
                "dataset_tech_notes={}".format(tech_notes["url"]),
                "dataset_tech_notes_title={}".format(tech_notes["title"])
            ]

        return " | ".join(parts) + "\n"

    def make_mapfile(self, dsid, file_dicts, tech_notes):
        path = self.get_mapfile_path(dsid)
        content = ""
        unversioned_dsid, version = self.split_versioned_dsid(dsid)
        for i, file_dict in enumerate(file_dicts):
            # Only include tech notes in the first line
            tn = tech_notes if i == 0 else None
            content += self.get_mapfile_line(unversioned_dsid, version,
                                             file_dict, tn)
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
            tech_note = {"url": ds_dict["tech_note_url"],
                         "title": ds_dict["tech_note_title"]}
            self.make_mapfile(dsid, ds_dict["files"], tech_note)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "input_json",
        help="JSON file to generate mapfile from"
    )
    parser.add_argument(
        "output_dir",
        help="Directory to save mapfile(s) in"
    )

    args = parser.parse_args(sys.argv[1:])
    mm = MakeMapfile(args.output_dir)
    mm.make_mapfiles(args.input_json)
