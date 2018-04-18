#!/bin/bash

usage() {
    echo "usage: $0 MAPFILE"
    exit 1
}

source `dirname $0`/publication_utils/common.sh

mapfile="$1"
[[ -n "$mapfile" ]] || usage

# Get dataset ID from mapfile. This assumes the mapfile only describes a single
# dataset
dsid=`head -n1 "$mapfile" | cut -d'|' -f1 | sed 's/#/.v/'`

# Find the path to catalog and any aggregations referenced by it
relative_cat_path=`cci_env python get_catalog_path.py -e "$INI_FILE" "$dsid"` || \
    die "could not find paths to catalogs in DB"
cat_path="${CATALOG_DIR}/${relative_cat_path}"
agg_paths=`cci_env python find_ncml.py "$cat_path"` || \
    die "could not find paths to NcML files in $cat_path"

# Check we have SSH access before starting
ssh_check

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
# TODO: Handle error when thredds reinit problem fixed
log "re-creating top level catalog..."
esg_env esgpublish -i "$INI_DIR" --project "$PROJ" --thredds-reinit
cci_env python get_catalogs.py -e "$INI_FILE" -o "$CATALOG_DIR" -n "$NCML_DIR" || \
    die "failed to copy top level catalog to $CATALOG_DIR"

# Delete from DB
log "deleting from database..."
esg_env esgunpublish -i "$INI_DIR" --project "$PROJ" --map "$mapfile" --database-only || \
    die "failed to delete from DB"

# Delete catalogs and aggregations from local directories
log "deleting modified catalogs and aggregations..."
rm "$cat_path" || die "could not delete catalogs"
if [[ -n "$agg_paths" ]]; then
    pushd "$NCML_DIR" > /dev/null
        rm "$agg_paths" || die "could not delete aggregations"
    popd > /dev/null
else
    log "no aggregations to delete"
fi

# Remove any empty directories
find "${CATALOG_DIR}" -mindepth 1 -type d -empty -delete
find "${NCML_DIR}"    -mindepth 1 -type d -empty -delete
# Sync remote node
log "syncing content on remote node..."
cci_env python transfer_catalogs.py -n "$NCML_DIR" -c "$CATALOG_DIR" -u "$REMOTE_TDS_USER" --delete \
    "$REMOTE_TDS_HOST" || \
    die "failed to sync catalogs and agregations"
