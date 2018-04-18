# esgf_wms

This repo contains scripts related to modifying ESGF-produced THREDDS catalogs
and creating NcML aggregations.

## Installation

Requirements are listed in `requirements.txt` and can be installed with
`pip install -r requirements.txt`. All the Python code has been developed and
tested under Python 3.4.5.

Use the following to set up a conda environment on `esgf-pub.ceda.ac.uk` to
run the Python code under:

```
source /usr/local/publication/setup_env.sh`
wget --no-check-certificate https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh
bash Miniconda2-latest-Linux-x86_64.sh -b -p $ESACCI_CONDA_ROOT
export PATH=${ESACCI_CONDA_ROOT}/bin:$PATH
conda create -y -n esgf_wms -c conda-forge python=3.4.5
source activate esgf_wms
pip install -r requirements.txt
source deactivate
```

## Usage

First ensure a proxy certificate has been generated
(see [below](#generating-a-proxy-certificate)).

To run the entire publication process, run

```
./publish.sh <input CSV>
```

Some environment variable are required by `publish.sh`:

* `INI_ROOT` - parent directory containing ESGF ini config files.
  The script looks at `${INI_ROOT}/cci-odp-data/esg.ini` to get the CCI config

* `MAPFILES_ROOT` - parent directory under which to write mapfiles

* `CATALOG_DIR` - directory to write modified THREDDS catalogs to

* `NCML_DIR` - directory to write NcML aggregations to

* `PUB_CONDA_ROOT` - conda root directory that has `esgf-pub` enviroment
  setup in it

* `ESACCI_CONDA_ROOT` - conda root directory that has `esgf_wms` enviroment
  setup (see [installation](#installation))

The format of the input CSV is [documented below](#csv).

### Generating a proxy certificate

This is required to authenticate when publishing to Solr:

```bash
mkdir -p ~/.globus
myproxy-logon -l <CEDA username> -s slcs1.ceda.ac.uk -o ~/.globus/certificate-file -b
```

(The `-b` flag downloads trustroots to `~/.globus` and only needs to be used
the first time a certificate is generated)

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

`publish.sh` calls lots of python scripts throughout the process - they are
documented below.

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

This script generates ESGF mapfiles from a JSON file in
['dataset JSON'](#dataset-json) format.

Usage: `./make_mapfiles.py <input JSON> <root output dir>`.

Mapfiles will be written in directories under `<root output dir>`, and the
paths to the generated files are written to stdout.

### get_catalogs.py

Usage: `./get_catalogs.py -o <outdir> -n <ncml dir> -e <path to esg.ini> <input JSON>`.

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

Usage:  `./merge_csv_json.py <input CSV>`.

Read a CSV file containing information about datasets to be published and
print JSON in ['dataset JSON'](#dataset-json) format to stdout.

### transfer_catalogs.py

Usage: `./transfer_catalogs.py -c <catalog dir> -n <ncml dir>`.

This script copies THREDDS catalogs and NcML aggregations to a remote THREDDS
server (default: cci-odp-data.ceda.ac.uk) and restarts tomcat.

It will also copy the root catalog from `static/catalog.xml` in this repo.

### get_host_from_ini.py

Usage `./get_host_from_ini.py <path to esg.ini> (solr | thredds)`.

Parse an ESGF ini config file and extract the hostname of the THREDDS or Solr
server.

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

Input JSON should be in ['dataset JSON'](#dataset-json) format.

## Tests

`tests.py` contains some tests - run `pytest tests.py`.
