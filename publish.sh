#!/bin/bash

usage() {
    echo "usage: $0 CSV_FILE"
    exit 1
}

# Source definitions of `esg_env', `cci_env' and others, and constants
source `dirname $0`/publication_utils/common.sh

# Get CSV input from arguments
in_csv="$1"
[[ -n "$in_csv" ]] || usage

# Check required environment variables are set
[[ -n "$MAPFILES_ROOT" ]]     || die '$MAPFILES_ROOT not set'

MAPFILES_DIR="${MAPFILES_ROOT}/testing/"  # TODO: Avoid hardcoding

# Get hostnames of services from esg.ini
REMOTE_TDS_HOST=`cci_env python publication_utils/get_host_from_ini.py "$INI_FILE" thredds` || \
    die "could not get THREDDS host from $INI_FILE"
SOLR_HOST=`cci_env python publication_utils/get_host_from_ini.py "$INI_FILE" solr` || \
    die "could not get Solr host from $INI_FILE"

# Check we have SSH access before starting
ssh_check

# Get input CSV in a JSON format used throughout the rest of the process
in_json=`mktemp`
cci_env python publication_utils/merge_csv_json.py "$in_csv" > "$in_json" || \
    die "failed to parse input CSV"

# Get mapfiles to feed into ESGF publisher
log "generating mapfiles in $MAPFILES_DIR..."
mapfiles=`cci_env python make_mapfiles.py "$in_json" "$MAPFILES_DIR"` || \
    die "failed to generate mapfiles"

for mapfile in $mapfiles; do
    log "processing mapfile ${mapfile}..."

    # Publish to postgres - this step may be slow as the publisher will need
    # to open each data file
    esg_env esgpublish -i "$INI_DIR" --project "$PROJ" --map "$mapfile" || \
        die "failed to publish to postgres"

    # Create THREDDS catalogs for each dataset
    esg_env esgpublish -i "$INI_DIR" --project "$PROJ" --map "$mapfile" --noscan --thredds \
               --service fileservice --no-thredds-reinit || \
        die "failed to create THREDDS catalogs"

    # Create top level catalog and reinit THREDDS
    # TODO: Handle error here once thredds-reinit is working on cci-odp-data
    esg_env esgpublish -i "$INI_DIR" --project "$PROJ" --thredds-reinit
done

# Retrieve generated THREDDS catalogs and modify them as necessary.
# This may be slow as to create aggregations each data file needs to be opened
log "modifying catalogs..."
cci_env python get_catalogs.py -o "$CATALOG_DIR" -n "$NCML_DIR" -e "$INI_FILE" "$in_json" \
    > /dev/null || die "failed to retrieve/modify THREDDS catalogs"

# Copy catalogs and aggregations to CCI server and restart tomcat
log "transferring catalogs to remote machine..."
cci_env python transfer_catalogs.py -c "$CATALOG_DIR" -n "$NCML_DIR" -v \
                                    -u "$REMOTE_TDS_USER" -s "$REMOTE_TDS_HOST" copy || \
    die "failed to transfer catalogs"

# Make sure aggregations on CCI server are cached ready for users to access
log "caching aggregations on the remote machine..."
cci_env python aggregation_utils/cache_remote_aggregations.py "$in_json" -v || \
    die "failed caching remote aggregations"

log "publishing to Solr..."
for mapfile in $mapfiles; do
    log "processing mapfile ${mapfile}..."

    # Publish to Solr by looking at endpoints on THREDDS server
    esg_env esgpublish -i "$INI_DIR" --project "$PROJ" --map "$mapfile" --noscan --publish || \
        die "failed to publish to Solr"
done

log "modifying WMS links in Solr..."
cci_env python modify_solr_links.py "http://${SOLR_HOST}:8984" || die "failed to modify Solr links"

# Clean up
rm "$in_json"
