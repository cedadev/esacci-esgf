# esgf_wms

### add_wms_for_wms_server.py

This script modifies THREDDS XML catalogs to add aggregate datasets with
WMS/WCS/OpenDAP access.

Install requirements with `pip install -r requirements.txt` (create and activate a python3
virtualenv first).

To run, first create directories `input_catalogs`, `output_catalogs` and `aggregations`.
Running `./add_wms_for_wms_server.py -a` will read the catalogs in `input_catalogs`, make the
necessary changes, and write the modified catalogs to `output_catalogs`.

NcML files for the aggregate datasets will be saved in `aggregations`.

By defalt the code assumes that the files and directories created in `aggregations` will be placed
under `/usr/local/aggregations` on the live server.

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

## Tests

`tests.py` contains some *very simple* tests - to run:

* Create a directory `test_input_catalogs` containing an un-modified THREDDS
  catalog (this is used as a base catalog to modify during the tests - but the
  modified catalog will *not* be written to disk)
* Run `pytest tests.py`.
