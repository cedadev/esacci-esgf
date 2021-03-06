#!/usr/bin/env python3
"""
Create a unified JSON document from a CSV file describing datasets to be
published, which references JSON files containing information about individual
data files.

The CSV file should contain the following header row, and a corresponding row
for each dataset:

ESGF DRS,No of files,Tech note URL,Tech note title,Aggregate,Include in WMS,JSON file

Booleans values should be `Yes' or `No'. Extraneous whitespace is ignored.

The output is formatted as required by `make_mapfiles.py'.
"""
import sys
import json
from csv import reader
import argparse
from collections import namedtuple

HEADER_ROW = ["ESGF DRS", "No of files", "Tech note URL", "Tech note title",
              "Aggregate", "Include in WMS", "JSON file"]


class Dataset(namedtuple("Dataset", ["drs", "num_files", "tech_note_url",
                                     "tech_note_title", "aggregate",
                                     "include_in_wms", "json_filename"])):
    """
    Class to represent a row in the CSV file that corresponds to a dataset
    """
    # Indices of columns should be converted from a string to some other type
    int_columns = [1]
    boolean_columns = [4, 5]

    @classmethod
    def from_strings(cls, values):
        """
        Instantiate a Dataset object from a line of the CSV file, and convert
        Yes/No to True/False
        """
        values = list(map(str.strip, values))

        # Convert boolean values
        for col in cls.boolean_columns:
            mapping = {"yes": True, "no": False}
            try:
                values[col] = mapping[values[col].lower()]
            except KeyError:
                raise ValueError("Invalid value for boolean column '{}'"
                                 .format(values[col]))

        # Convert int values
        for col in cls.int_columns:
            try:
                values[col] = int(values[col])
            except ValueError:
                raise ValueError("Invalid value for int column: '{}'"
                                 .format(values[col]))

        return cls(*values)

    def get_dict(self):
        """
        Return a dictionary that contains metadata about the dataset for this
        row and its data files
        """
        output = {
            "generate_aggregation": self.aggregate,
            "include_in_wms": self.include_in_wms,
            "tech_note_url": self.tech_note_url,
            "tech_note_title": self.tech_note_title,
            "files": []
        }

        with open(self.json_filename) as json_file:
            json_doc = json.load(json_file)

        try:
            ds_info = json_doc[self.drs]
        except KeyError:
            raise KeyError("Dataset '{}' not found in JSON file '{}'"
                           .format(self.drs, self.json_filename))

        for file_dict in ds_info:
            # Rename 'file' to 'path'...
            file_dict["path"] = file_dict["file"]
            del file_dict["file"]
            output["files"].append(file_dict)

        return output


def parse_file(csv_filename):
    """
    Parse a CSV file and construct JSON output, and print the JSON output to
    stdout
    """
    with open(csv_filename) as csv_file:
        r = reader(csv_file)

        if next(r) != HEADER_ROW:
            raise ValueError("Incorrect header row in '{}' - see {} --help"
                             .format(csv_filename, sys.argv[0]))

        output = {}
        for values in r:
            ds = Dataset.from_strings(values)
            output[ds.drs] = ds.get_dict()
        json.dump(output, sys.stdout, indent=4)


def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "input_csv",
        help="CSV file to parse"
    )
    args = parser.parse_args(sys.argv[1:])
    parse_file(args.input_csv)
