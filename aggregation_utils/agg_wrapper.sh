#!/bin/bash

if [[ -z "$1" ]]; then
    cat >&2 <<EOF
Usage: $0 DIR

Find NetCDF files in DIR, partition into groups that can be aggregated, and
write NcML aggregations to a temporary directory. Prints the name of the
temporary directory on completion.
EOF
    exit 1
fi

partition_dir=`mktemp -d`
agg_dir=`mktemp -d`
# The following assumes filenames don't contain newlines...
find "$1" -type f -name "*.nc" 2>/dev/null | python partition_files.py $partition_dir
for i in $partition_dir/*; do
    name=`basename $i`
    python aggregate.py < $i > "${agg_dir}/${name}.ncml"
done

rm -rf $partition_dir
echo $agg_dir
