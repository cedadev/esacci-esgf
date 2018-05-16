#!/bin/bash

# Source definitions of `esg_env', `cci_env' and others, and constants
source `dirname $0`/publication_utils/common.sh

usage() {
    echo "usage: $0 CSV_FILE"
    exit 1
}

# Make sure the proxy certificate has at least an hour remaining; if not then
# loop until it is renewed
certificate_check_loop() {
    while ! certificate_test 1; do
        if [[ -z "$cert_msg_shown" ]]; then
            cert_msg_shown="yep"
            log "certificate at '$CERT_FILE' has less than one hour before expiry. waiting for it to be renewed"
        fi
        sleep 1
    done
    unset cert_msg_shown
}

# Usage: remove_exclusions LIST EXCLUSIONS
# Remove strings in EXCLUSIONS from LIST. Both lists should contain strings
# separated by spaces
remove_exclusions() {
    list="$1"
    exclusions="$2"
    sed_cmd="s/ /\n/g"
    comm -23 <(echo "$list" | sed "$sed_cmd" | sort) <(echo "$exclusions" | sed "$sed_cmd" | sort)
}

# Get CSV input from arguments
in_csv="$1"
[[ -n "$in_csv" ]] || usage

# Check required environment variables are set
[[ -n "$MAPFILES_ROOT" ]] || die '$MAPFILES_ROOT not set'

MAPFILES_DIR="${MAPFILES_ROOT}/testing/"  # TODO: Avoid hardcoding

# Get hostnames of services from esg.ini
REMOTE_TDS_HOST=`cci_env python publication_utils/get_host_from_ini.py "$INI_FILE" thredds` || \
    die "could not get THREDDS host from $INI_FILE"
SOLR_HOST=`cci_env python publication_utils/get_host_from_ini.py "$INI_FILE" solr` || \
    die "could not get Solr host from $INI_FILE"

# Check SSH access and proxy certificate before starting
ssh_check
certificate_check 70

# Get input CSV in a JSON format used throughout the rest of the process
in_json=`mktemp`
cci_env python publication_utils/merge_csv_json.py "$in_csv" > "$in_json" || \
    die "failed to parse input CSV"

# Get mapfiles to feed into ESGF publisher
log "generating mapfiles in $MAPFILES_DIR..."
mapfiles=`cci_env python make_mapfiles.py "$in_json" "$MAPFILES_DIR"` || \
    die "failed to generate mapfiles"
# Build a list of mapfiles to exclude from later steps
excluded_mapfiles=""

for mapfile in $mapfiles; do
    log "processing mapfile ${mapfile}..."

    # Publish to postgres - this step may be slow as the publisher will need
    # to open each data file
    certificate_check_loop
    esg_env esgpublish -i "$INI_DIR" --project "$PROJ" --map "$mapfile"
    publish_status=$?

    if [[ $publish_status -eq 0 ]]; then
        # Create THREDDS catalogs for each dataset
        esg_env esgpublish -i "$INI_DIR" --project "$PROJ" --map "$mapfile" --noscan --thredds \
                           --service fileservice --no-thredds-reinit
        thredds_status=$?
        if [[ $thredds_status -ne 0 ]]; then
            warn "failed to create THREDDS catalogs"
        fi
    else
        warn "failed to publish to postgres"
    fi

    if [[ $publish_status -ne 0 || $thredds_status -ne 0 ]]; then
        # Add to exclude list
        dsid=`dsid_from_mapfile "$mapfile"`
        warn "excluding '$dsid' from publication"
        if [[ -z $excluded_mapfiles ]]; then
            excluded_mapfiles="$mapfile"
        else
            excluded_mapfiles="${excluded_mapfiles} ${mapfile}"
        fi

        # Remove from JSON
        temp_json="${in_json}.bak"
        mv "$in_json" $temp_json
        cci_env python publication_utils/remove_key.py "$dsid" "$temp_json" > "$in_json"
        rm "$temp_json"
    fi
done

mapfiles=`remove_exclusions "$mapfiles" "$excluded_mapfiles"`

# Create top level catalog and reinit THREDDS
# TODO: Handle error here once thredds-reinit is working on cci-odp-data
esg_env esgpublish -i "$INI_DIR" --project "$PROJ" --thredds-reinit

# Retrieve generated THREDDS catalogs and modify them as necessary.
# This may be slow as to create aggregations each data file needs to be opened
log "modifying catalogs..."
cci_env python get_catalogs.py -o "$CATALOG_DIR" -n "$NCML_DIR" -e "$INI_FILE" \
                               --remote-agg-dir "$REMOTE_AGGREGATIONS_DIR" "$in_json" \
    > /dev/null || die "failed to retrieve/modify THREDDS catalogs"

# Copy catalogs and aggregations to CCI server and restart tomcat
log "transferring catalogs to remote machine..."
cci_env python transfer_catalogs.py -c "$CATALOG_DIR" -n "$NCML_DIR" -v \
                                    -u "$REMOTE_TDS_USER" -s "$REMOTE_TDS_HOST" \
                                    --remote-catalog-dir="$REMOTE_CATALOG_DIR" \
                                    --remote-agg-dir="$REMOTE_AGGREGATIONS_DIR" \
                                    copy || \
    die "failed to transfer catalogs"

# Make sure aggregations on CCI server are cached ready for users to access
log "caching aggregations on the remote machine..."
cci_env python aggregation_utils/cache_remote_aggregations.py -v "$in_json" "$REMOTE_TDS_URL" || \
    warn "failed caching remote aggregations"

log "publishing to Solr..."
for mapfile in $mapfiles; do
    log "processing mapfile ${mapfile}..."

    # Publish to Solr by looking at endpoints on THREDDS server
    certificate_check_loop
    esg_env esgpublish -i "$INI_DIR" --project "$PROJ" --map "$mapfile" --noscan --publish || \
        warn "failed to publish to Solr"
done

log "modifying WMS links in Solr..."
cci_env python modify_solr_links.py "http://${SOLR_HOST}:8984" || die "failed to modify Solr links"

# Clean up
rm "$in_json"
