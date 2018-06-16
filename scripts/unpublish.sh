#!/bin/bash

usage() {
    echo "usage: `basename $0` MAPFILE"
    exit 1
}

source `dirname "$0"`/common.sh

mapfile="$1"
[[ -n "$mapfile" ]] || usage

# Check SSH access and proxy certificate before starting
ssh_check
certificate_check 70

# Get dataset ID from mapfile. This assumes the mapfile only describes a single
# dataset
dsid=`dsid_from_mapfile "$mapfile"`

# Find the path to catalog and any aggregations referenced by it
relative_cat_path=`cci_env get_catalog_path -e "$INI_FILE" "$dsid"` || \
    die "could not find paths to catalogs in DB"
temp=`mktemp`
cci_env transfer_catalogs -u "$REMOTE_TDS_USER" -s "$REMOTE_TDS_HOST" \
                          --remote-catalog-dir="$REMOTE_CATALOG_DIR" \
                          --remote-agg-dir="$REMOTE_AGGREGATIONS_DIR" \
                          -c "$relative_cat_path" retrieve > "$temp" || \
    die "could not retrieve catalog '$relative_cat_path' from remote node"

full_agg_paths=`cci_env find_ncml "$temp"` || \
    die "could not find paths to NcML files in $temp"
agg_paths=`echo "$full_agg_paths" | sed "s,${REMOTE_AGGREGATIONS_DIR},,g"`
rm "$temp"

# Delete from Solr
log "deleting from Solr..."
esg_env esgunpublish -i "$INI_DIR" --project "$PROJ" --map "$mapfile" \
                     --skip-thredds --delete || die "failed to delete from Solr"

# Delete catalogs
log "deleting THREDDS catalogs..."
esg_env esgunpublish -i "$INI_DIR" --project "$PROJ" --map "$mapfile" \
                     --skip-index --no-thredds-reinit || \
                     die "failed to delete THREDDS catalogs"

# Recreate top level catalog and copy
log "re-creating top level catalog..."
esg_env esgpublish -i "$INI_DIR" --project "$PROJ" --thredds-reinit || \
    die "failed to create top level catalog or THREDDS reinit"

cci_env get_catalogs -e "$INI_FILE" -o "$CATALOG_DIR" -n "$NCML_DIR" \
                     --remote-agg-dir="$REMOTE_AGGREGATIONS_DIR" || \
    die "failed to copy top level catalog to $CATALOG_DIR"

# Delete from DB
log "deleting from database..."
esg_env esgunpublish -i "$INI_DIR" --project "$PROJ" --map "$mapfile" --database-only || \
    die "failed to delete from DB"

# Delete catalogs and aggregations from local directories
log "deleting local catalogs..."
cat_path="${CATALOG_DIR}/${relative_cat_path}"
rm "$cat_path" || warn "could not delete local catalog '$cat_path'"
if [[ -n "$agg_paths" ]]; then
    pushd "$NCML_DIR" > /dev/null
        rm "$agg_paths" || warn "could not delete local aggregations: $agg_paths"
    popd > /dev/null
else
    log "no aggregations to delete"
fi

# Remove any empty directories
find "${CATALOG_DIR}" -mindepth 1 -type d -empty -delete
find "${NCML_DIR}"    -mindepth 1 -type d -empty -delete

# Delete from remote node
log "deleting content from remote node..."
ncml_args=""
for agg_path in $agg_paths; do
    ncml_args="-n $agg_path $ncml_args"
done
cci_env transfer_catalogs -v -u "$REMOTE_TDS_USER" -s "$REMOTE_TDS_HOST" \
                          --remote-catalog-dir="$REMOTE_CATALOG_DIR" \
                          --remote-agg-dir="$REMOTE_AGGREGATIONS_DIR" \
                          -c "$relative_cat_path" $ncml_args delete || \
    die "failed to delete content from remote node"

log "copying top level catalog to remote node..."
cci_env transfer_catalogs -v -u "$REMOTE_TDS_USER" -s "$REMOTE_TDS_HOST" \
                          --remote-catalog-dir="$REMOTE_CATALOG_DIR" \
                          --remote-agg-dir="$REMOTE_AGGREGATIONS_DIR" \
                          --reinit --thredds-username="$TDS_ADMIN_USER" \
                          --thredds-password="$TDS_ADMIN_PASSWORD" \
                          -c "${CATALOG_DIR}/catalog.xml" copy || \
    die "failed to copy top level catalog"

log "unpublication complete"
