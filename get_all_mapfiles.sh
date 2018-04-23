#!/bin/bash

all_mapfiles=`find ~/cci-misc/existing_mapfiles -type f`
tmp="/tmp/cci-aggregations"

for m in $all_mapfiles; do
    find $tmp -mindepth 1 -delete
    n=`cut -d'|' -f2 $m | sed 's/ //' | python aggregation_utils/partition_files.py $tmp | wc -l | cut -d' ' -f1`
    if [[ $n -gt 1 ]]; then
        echo ""
        echo "$m produced $n aggregations" >&2
    else
        echo -n "."
    fi
done

/home/users/jsingo/cci-misc/existing_mapfiles/cloud/metadata/mapfiles/by_name/esacci/CLOUD/mon/L3C/CLD_PRODUCTS/esacci.CLOUD.mon.L3C.CLD_PRODUCTS.AVHRR.multi-platform.AVHRR-AM.2-0.r1.v20170410 produced 4 aggregations
/home/users/jsingo/cci-misc/existing_mapfiles/cloud/metadata/mapfiles/by_name/esacci/CLOUD/mon/L3C/CLD_PRODUCTS/esacci.CLOUD.mon.L3C.CLD_PRODUCTS.AVHRR.multi-platform.AVHRR-PM.2-0.r1.v20170410 produced 7 aggregations
/home/users/jsingo/cci-misc/existing_mapfiles/sea_ice/metadata/mapfiles/by_name/esacci/SEAICE/day/L4/SICONC/esacci.SEAICE.day.L4.SICONC.AMSRE.Aqua.AMSR.01-11.r2.v20160704 produced 2 aggregations
/home/users/jsingo/cci-misc/existing_mapfiles/sea_ice/metadata/mapfiles/by_name/esacci/SEAICE/day/L4/SICONC/esacci.SEAICE.day.L4.SICONC.AMSRE.Aqua.AMSR.01-11.r1.v20160909 produced 2 aggregations
/home/users/jsingo/cci-misc/existing_mapfiles/sea_ice/metadata/mapfiles/by_name/esacci/SEAICE/day/L4/SICONC/esacci.SEAICE.day.L4.SICONC.SSMI.multi-platform.SSMI.01-11.r1.v20160704 produced 2 aggregations
/home/users/jsingo/cci-misc/existing_mapfiles/sea_ice/metadata/mapfiles/by_name/esacci/SEAICE/day/L4/SICONC/esacci.SEAICE.day.L4.SICONC.SSMI.multi-platform.SSMI.01-11.r2.v20160704 produced 2 aggregations
/home/users/jsingo/cci-misc/existing_mapfiles/sea_ice/metadata/mapfiles/by_name/esacci/SEAICE/day/L4/SICONC/esacci.SEAICE.day.L4.SICONC.AMSRE.Aqua.AMSR.01-11.r1.v20160704 produced 2 aggregations
/home/users/jsingo/cci-misc/existing_mapfiles/sea_ice/metadata/mapfiles/by_name/esacci/SEAICE/day/L4/SICONC/esacci.SEAICE.day.L4.SICONC.AMSRE.Aqua.AMSR.01-11.r2.v20160909 produced 2 aggregations
/home/users/jsingo/cci-misc/existing_mapfiles/sea_ice/metadata/mapfiles/by_name/esacci/SEAICE/day/L4/SICONC/esacci.SEAICE.day.L4.SICONC.SSMI.multi-platform.SSMI.01-11.r1.v20160909 produced 2 aggregations
/home/users/jsingo/cci-misc/existing_mapfiles/sea_ice/metadata/mapfiles/by_name/esacci/SEAICE/day/L4/SICONC/esacci.SEAICE.day.L4.SICONC.SSMI.multi-platform.SSMI.01-11.r2.v20160909 produced 2 aggregations



esacci.SEAICE.day.L4.SICONC.AMSRE.Aqua.AMSR.01-11.r2.v20160704
esacci.SEAICE.day.L4.SICONC.AMSRE.Aqua.AMSR.01-11.r1.v20160909
esacci.SEAICE.day.L4.SICONC.SSMI.multi-platform.SSMI.01-11.r1.v20160704
esacci.SEAICE.day.L4.SICONC.SSMI.multi-platform.SSMI.01-11.r2.v20160704
esacci.SEAICE.day.L4.SICONC.AMSRE.Aqua.AMSR.01-11.r1.v20160704
esacci.SEAICE.day.L4.SICONC.AMSRE.Aqua.AMSR.01-11.r2.v20160909
esacci.SEAICE.day.L4.SICONC.SSMI.multi-platform.SSMI.01-11.r1.v20160909
esacci.SEAICE.day.L4.SICONC.SSMI.multi-platform.SSMI.01-11.r2.v20160909
