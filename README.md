# esacci-esgf

This repo contains scripts related to modifying ESGF-produced THREDDS catalogs
and creating NcML aggregations.

## Installation

Dependencies should be installed in a conda environment with Python 3.4.5
installed:

```bash
# Install conda (python 3)
conda_root="/path/to/create/conda/installation"
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -b -p $conda_root

# Create a new env and activate it
export PATH="${conda_root}/bin:$PATH"
conda env create -f environment.yml
conda activate esacci-esgf

# Install esacci-esgf python pakage
pip install -e .
```

To activate it:
```
source "${conda_root}/bin/activate esacci-esgf
```

(the use of a `$conda_root` variable is for demo purposes and is not strictly
necessary)

This project currently uses a patched version of the ESGF publisher (the
`esacci-patches` branch of
[this fork](https://github.com/joesingo/esg-publisher/tree/esacci-patches))
which should be installed in a separate conda environment (see `PUB_CONDA_ENV`
below).

## Usage

### Publishing

First ensure a proxy certificate has been generated
(see [below](#generating-a-proxy-certificate)).

To run the entire publication process, run

```
scripts/publish.sh <input CSV>
```

The following environment variable are required by `publish.sh`:

* `INI_DIR` - directory containing ESGF ini config files. This directory
  should contain `esg.ini` and `esg.esacci.ini`

* `MAPFILES_DIR` - directory under which to write mapfiles

* `CATALOG_DIR` - directory to write modified THREDDS catalogs to

* `NCML_DIR` - directory to write NcML aggregations to

* `PUB_CONDA_ENV` - name of the conda enviroment used to run the ESGF publisher

* `PUB_CONDA_ROOT` - conda root directory that has `PUB_CONDA_ENV` enviroment
  setup in it

* `ESACCI_CONDA_ROOT` - conda root directory that has `esacci-esgf` enviroment
  setup (see [installation](#installation))

The following variables are optional:

* `REMOTE_TDS_USER` - user to SSH to the remote THREDDS host as (default: `root`)

* `REMOTE_CATALOG_DIR` - directory in which to store modified THREDDS
  catalogs on the remote THREDDS host (default: `/var/lib/tomcat/content/thredds/esacci`)

* `REMOTE_AGGREGATIONS_DIR` - directory in which to store NcML aggregations on
  the remote THREDDS host (default: `/usr/local/aggregations/`)

* `CERT_FILE` - path to the the certificate file used to authenticate when
  publishing to Solr -- see [generating a proxy certificate](#generating-a-proxy-certificate)
  (default: `~/.globus/certificate-file`)

The format of the input CSV is [documented below](#csv).

Note that `publish.sh` invokes both the `esacci-esgf` and `$PUB_CONDA_ENV`
conda environments, so make sure neither one is activated before running.

### Generating a proxy certificate

This is required to authenticate when publishing to Solr:

```bash
mkdir -p ~/.globus
myproxy-logon -l <CEDA username> -s slcs1.ceda.ac.uk -o ~/.globus/certificate-file -b -t 72
```

(The `-b` flag downloads trustroots to `~/.globus` and only needs to be used
the first time a certificate is generated)

### Un-publishing

The shell script `unpublish.sh` will un-publish data and delete the modified
THREDDS catalogs and aggregations from the remote server and from `CATALOG_DIR`
and `NCML_DIR` on the local machine (if they exist in these directories).

It is run as `scripts/unpublish.sh <mapfile>`.

It requires the following environment variables to be set (see
[publish.sh usage](#publishing) for their meaning): `INI_DIR`, `PUB_CONDA_ROOT`,
`ESACCI_CONDA_ROOT`, `CATALOG_DIR`, `NCML_DIR`.

As with `publish.sh`, ensure a valid proxy certificate has been generated
before running.

## Input formats

### CSV

The CSV file passed as an argument to `publish.sh` should have the following
header row:

```
ESGF DRS,No of files,Tech note URL,Tech note title,Aggregate,Include in WMS,JSON file
```

Each row corresponds to a dataset to be published. Boolean values should be
'Yes' or 'No'.

The references JSON files should be in ['CSV JSON'](#csv-json) format.

### CSV JSON

These JSON files should be of the form

```json
{
    "<dataset name>.v<version>": [
        {
            "file": "<path>",
            "size": "<size in bytes>",
            "mtime": "<mtime>",
            "sha256": "<checksum>"
        },
        ...
    ],
    ...
}
```

This format is only used because it exists for previously previously published
datasets, and reusing it means avoiding having to re-calculate checksums etc.

### Dataset JSON

This format combines CSV (which is easy to edit by hand) and 'CSV JSON', and
is used in various scripts throughout publication:

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

## Other scripts

`publish.sh` calls lots of python scripts throughout the process, some of which
are in the [tds-utils](https://github.com/cedadev/tds-utils) repository.
Scripts from this repo are documented below:

### modify_catalogs.py

This script modifies THREDDS xml catalogs generated by the ESGF publisher to
remove ESGF-specific markup, and optionally creates NcML aggregations and makes
these accessible through OPeNDAP/WMS/WCS.

Basic usage: `modify_catalogs <catalog> [<catalog> ...]`

This will process the catalog(s) given and write the modified catalog(s) to a
file in `output_catalogs` with the same basename as `<catalog>`.

If `--aggregate` is used, NcML files for the aggregate datasets will be saved
in `aggregations` and OPeNDAP endpoints are added. Use `--wms` to additionally
create WMS/WCS endpoints.

The directory names `input_catalogs`, `output_catalogs` and `aggregations` can
be overridden with the `--input-dir`, `--output-dir` and `--ncml-dir` options
respectively.

By default the code assumes that the files and directories created in
`aggregations` will be placed under `/usr/local/aggregations` on the live server
- this can be changed with `--remote-agg-dir`.

### make_mapfiles.py

This script generates ESGF mapfiles from a JSON file in
['dataset JSON'](#dataset-json) format.

Usage: `make_mapfiles <input JSON> <root output dir>`.

Mapfiles will be written in directories under `<root output dir>`, and the
paths to the generated files are written to stdout.

### get_catalogs.py

Usage: `get_catalogs -o <outdir> -n <ncml dir> -e <path to esg.ini> [<input JSON>...]`.

This script is a wrapper around `modify_catalogs.py` that takes input JSON in
['dataset JSON'](#dataset-json) format, finds the location of
THREDDS catalogs produced by the publisher for each dataset, and runs
`modify_catalogs.py` with appropriate arguments.

The modified catalogs are saved under `<outdir>` and the directory structure of
the THREDDS root dir (i.e. the directory under which the publisher writes the
catalogs) is preserved.

The top level catalog is also copied to `<outdir>/catalog.xml`.

If no JSON files are given then only the top level catalog is copied.

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

Usage: `merge_csv_json <input CSV>`.

Read a CSV file containing information about datasets to be published and
print JSON in ['dataset JSON'](#dataset-json) format to stdout.

### transfer_catalogs.py

This script manages THREDDS catalogs and NcML aggregations on a remote THREDDS
server. It supports copying content from the local machine, deleting content on
the remote machine, and retrieving content from the remote machine.

Usage:

`transfer_catalogs [-c <catalog>] [-n <ncml>] (copy | delete | retrieve)`

When copying, `<catalog>` and `<ncml>` should be local files or directories
that are to be copied.

When deleting, `<catalog>` and `<ncml>` should be paths of files on the remote
server relative to the THREDDS/NcML root directories (given by
`--remote-catalog-dir` and `--remote-agg-dir`).

Both `-c` and `-n` can be used multiple times when copying or deleting.

When retrieving, `<catalog>` and `<ncml>` are interpreted in the same way as
with deletion. Exactly one catalog or NcML file must be specified. The file's
contents is written to stdout.

The server hostname and user to connect as can be changed with `-s` and `-u`
respectively.

### get_catalog_path.py

Usage: `get_catalog_path -e <path to esg.ini> <dataset name>`

Query the publication database to find the path of a THREDDS catalog relative
to the THREDDS root.

### parse_esg_ini.py

Usage `parse_esg_ini <path to esg.ini> (solr_host | thredds_host | thredds_password | thredds_root | publication_db_url)`.

Parse an ESGF ini config file and extract a value.

### remove_key.py

Usage: `remove_key <key> <json file>`.

Remove a key from the top level of a JSON dictionary, and print the new
dictionary to stdout.

## Tests

`tests.py` contains some tests - run `pytest esacci_esgf/tests.py`.
