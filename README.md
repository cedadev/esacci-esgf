# esgf_wms

This repo contains scripts related to modifying ESGF-produced THREDDS catalogs
and creating NcML aggregations.

Install requirements with `pip install -r requirements.txt` (create and
activate a python3 virtualenv first - the code has been developed and tested
with python version 3.4.5).

To run the entire process, use something like the following:
```bash
in_csv="<path to input CSV>"
in_json=`mktemp`

# Get input CSV in a JSON format used throughout the rest of the process
python publication_utils/merge_csv_json.py $in_csv > $in_json

# Get mapfiles to feed into ESGF publisher
mapfile_dir="<root dir to store mapfiles under>"
mapfiles=`python make_mapfiles.py $in_json $mapfile_dir`

#-----------------------------------------------------------------------------#
# Step 1 of publication: publish to Postgres and THREDDS on publication machine
# for mapfile in $mapfiles; do
#     ...
# done
#-----------------------------------------------------------------------------#

# Retrieve generated THREDDS catalogs and modify them as necessary
out_cats=`mktemp -d`
out_aggs=`mktemp -d`
python get_catalogs.py -o $out_cats -n $out_aggs -e <path to esg.ini> $in_json

# Copy catalogs and aggregations to CCI server and restart tomcat
python transfer_catalogs.py -c $out_cats -n $out_aggs -v

# Make sure aggregations on CCI server are cached ready for users to access
python aggregation_utils/cache_remote_aggregations.py $in_json -v

#-----------------------------------------------------------------------------#
# Step 2 of publication: publish to Solr from THREDDS on CCI node
# for mapfile in $mapfiles; do
#     ...
# done
#-----------------------------------------------------------------------------#

python modify_solr_links.py <solr node>

rm -r $input_json $out_cats $out_aggs
```

## Publication

### modify_catalogs.py

This script modifies THREDDS xml catalogs generated by the ESGF publisher to
remove ESGF-specific markup, and optionally creates NcML aggregations and makes
these accessible through OPeNDAP/WMS/WCS.

Basic usage: `./modify_catalogs.py <catalog> [<catalog> ...]`

This will process the catalog(s) given and write the modified catalog(s) to a
file in `output_catalogs` with the same basename as `<catalog>`.

If `--aggregate` is used, NcML files for the aggregate datasets will be saved
in `aggregations` and OPeNDAP endpoints are added. Use `--wms` to additionally
create WMS/WCS endpoints.

(The directory names `input_catalogs`, `output_catalogs` and `aggregations` can
be overridden with the `--input-dir`, `--output-dir` and `--ncml-dir` options
respectively)

The code assumes that the files and directories created in `aggregations` will
be placed under `/usr/local/aggregations` on the live server.

### make_mapfiles.py

This script generates ESGF mapfiles from a JSON file of the form

```json
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
```

Usage: `./make_mapfiles.py <input JSON> <root output dir>`.

Mapfiles will be written in directories under `<root output dir>`, and the
paths to the generated files are written to stdout.

### get_catalogs.py

Usage: `./get_catalogs.py -o <outdir> -n <ncml dir> -e <path to esg.ini> <input JSON>`.

This script is a wrapper around `modify_catalogs.py` that takes input JSON in
the same format as `make_mapfiles.py` (see above), finds the location of
THREDDS catalogs produced by the publisher for each dataset, and runs
`modify_catalogs.py` with appropriate arguments.

It must be run after the first step of publication since the THREDDS catalogs
need to exist and be recorded in the publication database.

The DB connection URL and root THREDDS directory is obtained from an INI file,
the path to which must be given on the command line. This file must at least
contain a `DEFAULT` section containing the following:

```INI
[DEFAULT]
dburl = postgresql://user:password@host/dbname
thredds_root = /path/to/thredds/root
```

(any extra sections and settings are ignored, so the full `esg.ini` can be
used)


### merge_csv_json.py

This script is located in `publication_utils`.

Usage:  `./merge_csv_json.py <input CSV>`.

Read a CSV file containing information about datasets to be published and
print JSON in the format required by `make_mapfiles.py` to stdout.

See `./merge_csv_json.py --help` for the required format of the CSV.

### transfer_catalogs.py

Usage: `./transfer_catalogs.py -c <catalog dir> -n <ncml dir>`.

This script copies THREDDS catalogs and NcML aggregations to a remote THREDDS
server (default: cci-odp-data.ceda.ac.uk) and restarts tomcat.

It will also copy the root catalog from `static/catalog.xml` in this repo, and
the 'top-level' catalog generated by the ESGF publisher that links to the
actual datasets.

## Aggregation helper scripts

These scripts are located in `aggregation_utils`.

### partition_files.py

Usage: `./partition_files.py <outdir>`.

Read file paths from standard input and partitions them into groups of files that can likely be
aggregated based on their filenames. These groups are written to files `<outdir>/1`, `<outdir>/2`
etc...

### aggregate.py

Read file paths from standard input (one per line) and write an NcML aggregation of those files to
standard output.

### agg_wrapper.sh

A convenience script `./agg_wrapper.sh <dir>` finds NetCDF files in `<dir>`, runs
`partition_files.py` on the list and `aggregate.py` on each output.

### cache_remote_aggregations.py

Usage: `./cache_remote_aggregations.py <input JSON> [<base THREDDS URL]`.

Send HTTP requests to OPeNDAP/WMS aggregation endpoints based on dataset IDs
found in the input JSON. This makes sure THREDDS caches aggregations before any
end-user tries to access them.

The JSON input should be in the same format as required by `make_mapfiles.py`
etc...

## Tests

`tests.py` contains some tests - run `pytest tests.py`.
