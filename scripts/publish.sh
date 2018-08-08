#!/bin/bash

###############################################################################
# This is the main script used to publish CCI data. It does the following:
#
# - Parse the input CSV and generate mapfiles.

# - Use esgpublish to publish to the PostgreSQL database and generate initial
#   THREDDS catalogs.
#
# - Get catalogs and call modify_catalogs on each with appropriate arguments.
#   This creates NcML aggregations and adds WMS access links to the catalog (if
#   required).
#
# - Transfer the catalogs/aggregations to the remote THREDDS server, and call
#   the THREDDS reinit URL.
#
# - Send a HTTP request to OpenDAP/WMS endpoints on the remote THREDDS server
#   to cache aggregations
#
# - Use esgpublish to publish to Solr
#
# - Correct OpenDAP/WMS links in Solr
#
###############################################################################

# Source definitions of `esg_env', `cci_env' and others, and constants
source `dirname "$0"`/common.sh

usage() {
    echo "usage: `basename $0` CSV_FILE"
    exit 1
}

# Make sure the user certificate has at least an hour remaining; if not then
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
[[ -n "$MAPFILES_DIR" ]] || die '$MAPFILES_DIR not set'

# Check SSH access and user certificate before starting
ssh_check
certificate_check 70

# Get input CSV in a JSON format used throughout the rest of the process
in_json=`mktemp`
cci_env merge_csv_json "$in_csv" > "$in_json" || \
    die "failed to parse input CSV"

# Check facet values in DRSes match those defined in the project INI.
# esgcheckvocab writes its output to stdout, but we want it on stderr, so
# redirect 1>&2 and discard esgcheckvocab's stderr. Note that the ordering of
# the redirects is important here
log "Checking facets in DRSes..."
esg_env esgcheckvocab -i "$INI_DIR" --project "$PROJ" \
                      --dataset-list <(tail -n+2 "$in_csv" | cut -d, -f1) \
    1>&2 2>/dev/null || die "Invalid facet values found in DRSes"

# Get mapfiles to feed into ESGF publisher
log "generating mapfiles in $MAPFILES_DIR..."
mapfiles=`cci_env make_mapfiles "$in_json" "$MAPFILES_DIR"` || \
    die "failed to generate mapfiles"
# Build a list of mapfiles to exclude from later steps
excluded_mapfiles=""
excluded_dsids=""  # Record DRSes too just to show a message at the end

for mapfile in $mapfiles; do
    log "processing mapfile ${mapfile}..."

    # Publish to postgres - this step may be slow as the publisher will need
    # to open each data file
    certificate_check_loop
    esg_env esgpublish -i "$INI_DIR" --project "$PROJ" --map "$mapfile" --commit-every 100
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
            excluded_dsids="$dsid"
        else
            excluded_mapfiles="${excluded_mapfiles} ${mapfile}"
            excluded_dsids="${excluded_dsids} ${dsid}"
        fi

        # Remove from JSON
        temp_json="${in_json}.bak"
        mv "$in_json" $temp_json
        cci_env remove_key "$dsid" "$temp_json" > "$in_json"
        rm "$temp_json"
    fi
done

mapfiles=`remove_exclusions "$mapfiles" "$excluded_mapfiles"`

# No point in continuing if all datasets failed above
if [[ -z $mapfiles ]]; then
    die "all datasets have been excluded -- aborting"
fi

# Create top level catalog (this must be done by reinit-ing THREDDS through the
# publisher
esg_env esgpublish -i "$INI_DIR" --project "$PROJ" --thredds-reinit || \
    die "failed to create top level catalog or THREDDS reinit"

# Retrieve generated THREDDS catalogs and modify them as necessary.
# This may be slow as to create aggregations each data file needs to be opened
log "modifying catalogs..."
cci_env get_catalogs -o "$CATALOG_DIR" -n "$NCML_DIR" -e "$INI_FILE" \
                     --remote-agg-dir "$REMOTE_NCML_DIR" "$in_json"  || \
    die "failed to retrieve/modify THREDDS catalogs"

# Copy catalogs and aggregations to CCI server and reinit THREDDS
log "transferring catalogs to remote machine..."
cci_env transfer_catalogs -c "$CATALOG_DIR" -n "$NCML_DIR" -v \
                          -u "$REMOTE_TDS_USER" -s "$REMOTE_TDS_HOST" \
                          --remote-catalog-dir="$REMOTE_CATALOG_DIR" \
                          --remote-agg-dir="$REMOTE_NCML_DIR" \
                          --reinit --thredds-username="$TDS_ADMIN_USER" \
                          --thredds-password="$TDS_ADMIN_PASSWORD" \
                          copy || \
    die "failed to transfer catalogs"

# Make sure aggregations on CCI server are cached ready for users to access
log "caching aggregations on the remote machine..."
cci_env cache_remote_aggregations -v "$in_json" "$REMOTE_TDS_URL" || warn "failed to cache remote aggregations"

log "publishing to Solr..."
for mapfile in $mapfiles; do
    log "processing mapfile ${mapfile}..."

    # Publish to Solr by looking at endpoints on THREDDS server
    certificate_check_loop
    esg_env esgpublish -i "$INI_DIR" --project "$PROJ" --map "$mapfile" --noscan --publish || \
        warn "failed to publish to Solr"
done

log "modifying WMS links in Solr..."
cci_env modify_solr_links "http://${SOLR_HOST}:8984" || die "failed to modify Solr links"

# Clean up
rm "$in_json"

log "publication complete"
if [[ -n $excluded_dsids ]]; then
    log "The following datasets could not be published:"
    log "$excluded_dsids"
fi
