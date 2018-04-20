#!/bin/bash

usage() {
    echo "usage: $0 MAPFILE"
    exit 1
}

source `dirname $0`/publication_utils/common.sh

mapfile="$1"
[[ -n "$mapfile" ]] || usage

# Check SSH access and proxy certificate before starting
ssh_check
certificate_check 70

# Get dataset ID from mapfile. This assumes the mapfile only describes a single
# dataset
dsid=`head -n1 "$mapfile" | cut -d'|' -f1 | sed 's/#/.v/'`

# Find the path to catalog and any aggregations referenced by it
relative_cat_path=`cci_env python get_catalog_path.py -e "$INI_FILE" "$dsid"` || \
    die "could not find paths to catalogs in DB"
temp=`mktemp`
cci_env python transfer_catalogs.py -u "$REMOTE_TDS_USER" -s "$REMOTE_TDS_HOST" \
                                    -c "$relative_cat_path" retrieve > "$temp" || \
    die "could not retrieve catalog '$relative_cat_path' from remote node"

agg_paths=`cci_env python find_ncml.py "$temp"` || \
    die "could not find paths to NcML files in $temp"
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
cci_env python transfer_catalogs.py -v -u "$REMOTE_TDS_USER" -s "$REMOTE_TDS_HOST" \
                                    -c "$relative_cat_path" $ncml_args delete || \
    die "failed to delete content from remote node"

log "copying top level catalog to remote node..."
cci_env python transfer_catalogs.py -c "${CATALOG_DIR}/catalog.xml" -u "$REMOTE_TDS_USER" \
                                    -s "$REMOTE_TDS_HOST" copy || \
    die "failed to copy top level catalog"
