import os.path
import re
from datetime import datetime, timedelta, timezone
import xml.etree.cElementTree as ET

from tds_utils.aggregation import (NetcdfDatasetReader, BaseAggregationCreator,
                                   AggregationType, NcMLVariable)


UNITS = "seconds since 1970-01-01 00:00:00 UTC"


class CCIAerosolDatasetReader(NetcdfDatasetReader):

    # datetime formats to try (in order of preference)
    time_formats = (
        # ISO 8601 formats
        "%Y%m%dT%H%M%SZ",
        "%Y-%m-%dT%H:%M:%SZ",

        # e.g. 24-JUL-2002 04:31:33.070626
        "%d-%b-%Y %H:%M:%S.%f",
        # as above with no time
        "%d-%b-%Y",

        # date in filenames
        "%Y%m%d",
    )

    time_attr_names = (
        ("time_coverage_start", "time_coverage_end"),
        ("startdate", "stopdate"),
        ("Startdate", "Stopdate")
    )

    @classmethod
    def get_datetime(cls, date_str):
        """
        Try to parse a date from a string using the above list of formats
        """
        for fmt in cls.time_formats:
            try:
                return (datetime.strptime(date_str, fmt)
                                .replace(tzinfo=timezone.utc))
            except ValueError:
                continue
        raise ValueError("Could not parse date string '{}'".format(date_str))

    def get_start_end_date(self):
        """
        Return (start, end) for the time range the dataset covers, where
        start and end are datetime objects
        """
        for attrs in self.time_attr_names:
            try:
                date_strs = [getattr(self.ds, attr) for attr in attrs]
            except AttributeError:
                continue
            return [self.get_datetime(s) for s in date_strs]

        # GOMOS data uses days since 'modified Julian day'
        if (hasattr(self.ds, "title") and "GOMOS" in self.ds.title and
            hasattr(self.ds, "startDate") and hasattr(self.ds, "endDate")):

            epoch = datetime(year=1858, month=11, day=17, hour=0, minute=0,
                             second=0, tzinfo=timezone.utc)

            start = epoch + timedelta(days=int(self.ds.startDate))
            end = epoch + timedelta(days=int(self.ds.endDate), hours=23,
                                    minutes=59, seconds=59)
            return start, end

        # Try getting date from filename as last resort
        filename = os.path.basename(self.ds.filepath())
        match = re.search(r"^([0-9]{8})-([0-9]{8})", filename)
        if match:
            date_strs = [match.group(1), match.group(2)]
            return [self.get_datetime(s) for s in date_strs]

        raise ValueError("Could not determine start and end time for file "
                         "'{}'".format(self.ds.filepath()))

    def get_coord_values(self, dimension):
        """
        CCI aerosol datasets do not have 'time' variables, so the timestamp
        must be calculated by other means
        """
        try:
            start, end = self.get_start_end_date()
        except ValueError as ex:
            filename = os.path.basename(self.ds.filepath())
            raise ValueError("Error in file '{}': {}".format(filename, ex))

        midpoint = (start.timestamp() + end.timestamp()) / 2
        return (UNITS, [int(midpoint)])


class CCIAerosolAggregationCreator(BaseAggregationCreator):
    # Must be a joinNew aggregation as files do not have an existing 'time'
    # dimension
    aggregation_type = AggregationType.JOIN_NEW
    dataset_reader_cls = CCIAerosolDatasetReader

    extra_variables = [
        NcMLVariable(name="time", type="int", shape="time",
                    attrs={"units": UNITS, "standard_name": "time",
                           "calendar": "standard",
                           "_CoordinateAxisType": "Time" })
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if self.dimension != "time":
            raise ValueError("Aerosol special case only handles time "
                             "aggregations - not '{}'".format(self.dimension))
