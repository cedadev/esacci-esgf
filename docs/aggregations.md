# Aggregations

Aggregations are created using
[NcML](https://www.unidata.ucar.edu/software/thredds/current/netcdf-java/ncml/Aggregation.html).
The core code to generate the NcML is in the [tds-utils
library](https://github.com/cedadev/tds-utils/blob/master/tds_utils/aggregation/aggregate.py),
but the `esacci-esgf` code has a few customisations regarding global
attributes.

By default, the aggregation inherits all global attributes from the penultimate
file in the list. The following attributes are added/overwritten:

| Attribute     | Description |
| ------------- | ----------- |
| `history`     | A message is appended to state that the CCI open data portal has aggregated a collection of files |
| `id`          | This is overwritten to contain the DRS (if left unchanged if would be the filename of the penultimate file) |
| `tracking_id` | A new version 4 UUID is created |
| `source`      | A comma separated list of values from the original files (with duplicated removed) |
| `sensor`      | As with `source`, but values in original files are comma separated lists of strings themselves |
| `platform`    | As with `sensor` |
| `time_coverage_{start,end}` | This is set to the earliest/latest time across the constituent files |
| `{start,stop}_time`         | Same as above |
| `time_coverage_duration`    | The time range the aggregation covers, calculated as the difference between latest end time and earliest start time |
| `geospatial_{lat,lon}_{min,max}` | Similar to time coverage, this is set to the max/min longitude/latitude |
| `{north,south}ernmost_latitude` and `{east,west}ernmost_longitude` | Same as above |

Note that there are several formats for start/end times and geospatial bounds.
This is to work around datasets that do not follow the [CCI data
requirements](http://cci.esa.int/sites/default/files/CCI_Data_Requirements_Iss1.2_Mar2015.pdf).
The requirements document specifies only `time_coverage_{start,end}` and
`geospatial_{lat,lon}_{min,max}`.


There are also some attributes that are removed from the aggregation (if present):

* `number_of_processed_orbits`
* `number_of_files_composited`

## Aerosol data

The aerosol NetCDF files do not have a `time` variable or dimension. However
the default aggregation type in the `tds-utils` code is `joinExisting`, which
requires there to be an existing `time` dimension in each file.

To work around this there is a custom aggregation class for aerosol data at
`esacci_esgf.aggregation.aersol.CCIAerosolAggregationCreator`. It creates
`joinNew` aggregations instead, and extracts the time coordinates from each
file from global attributes or the filename.

Unfortunately different aerosol products do not use the same format for
timestamps, or even the same attribute names. Various cases are handled in
`CCIAerosolAggregationCreator` but it has only been tested with a small number
of files from each product.

Another issue is that for `joinNew` aggregations one must explicitly list the
variables that depend on time with `<variable>` elements in the NcML, since
from the NetCDF file there is no indication as to what is time-dependent and
what is just a coordinate variable. This has not been practical to automate, as
each product has different variables.

For these reasons aerosol data has **not** been aggregated in the latest
publishing run. The code is left in this repository in case it is re-visited
in the future.
