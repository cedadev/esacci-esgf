# Publication

`publish.sh` does the following (taken from the header in the source code):

- Parse the input CSV and generate mapfiles.

- Use `esgpublish` to publish to the PostgreSQL database and generate initial
  THREDDS catalogs.

- Get catalogs and call `modify_catalogs` on each with appropriate arguments.
  This creates NcML aggregations and adds WMS access links to the catalog (if
  required).

- Transfer the catalogs/aggregations to the remote THREDDS server, and call the
  THREDDS reinit URL.

- Send a HTTP request to OpenDAP/WMS endpoints on the remote THREDDS server to
  cache aggregations

- Use `esgpublish` to publish to Solr

- Correct OpenDAP/WMS links in Solr

## Unpublishing

`unpublish.sh` does the following:

- Unpublish from Solr, THREDDS and PostgreSQL

- Remove local and remote copies of THREDDS catalogs and NcML aggregations

- Recreate top level catalog and transfer to remote node
