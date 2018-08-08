import sys
import re
from datetime import datetime
from uuid import uuid4

from netCDF4 import Dataset
import isodate

from tds_utils.aggregation import AggregationCreator, AggregatedGlobalAttr


# Functions to convert between ISO datetime string and datetime objects
str_to_date = isodate.parse_datetime
def str_to_date(date_str):
    # Fix some formats known to be in used in CCI data
    if re.match("^\d{12}Z$", date_str):
        date_str = "{date}T{time}Z".format(date=date_str[:8], time=date_str[8:12])

    return isodate.parse_datetime(date_str)

def date_to_str(dt):
    return isodate.datetime_isoformat(dt, format="%Y%m%dT%H%M%S%Z")

# Functions to find earliest/latest dates from a list of ISO-format strings
def min_date(dates):
    return date_to_str(min(map(str_to_date, dates)))
def max_date(dates):
    return date_to_str(max(map(str_to_date, dates)))


def combine_lists(lists):
    """
    Take a list of comma-separated strings and return a string containing the
    unique items, separated by commas
    e.g. ["one,two", "two,three"] -> "one,two,three"
    """
    unique_items = set([])
    for string in lists:
        items = filter(None, map(str.strip, string.split(",")))
        unique_items.update(items)
    return ",".join(sorted(unique_items))


def unique_strings(strings):
    """
    Find unique strings in the list `strings` and combine them into a single
    comma-separated string
    """
    # Remove whitespace and empty strings
    return ",".join(sorted(set(filter(None, map(str.strip, strings)))))


class CCIAggregationCreator(AggregationCreator):

    # List of (start_attr, end_attr) for possible attribute names for time
    # coverage
    date_range_formats = [
        ("time_coverage_start", "time_coverage_end"),
        ("start_time", "stop_time")
    ]

    # Same as above for geospatial bounds
    geospatial_bounds_formats = [
        ("geospatial_lat_max", "geospatial_lon_max",
         "geospatial_lat_min", "geospatial_lon_min"),

        ("nothernmost_latitude", "easternmost_longitude",
         "southernmost_latitude", "westernmost_longitude")
    ]

    def create_aggregation(self, drs, file_list, *args, **kwargs):
        # Add extra global attributes
        global_attrs = kwargs.pop("global_attrs", {})
        global_attrs.update(self.get_global_attrs(file_list, drs))

        # Add aggregated global attributes
        attr_aggs = kwargs.pop("attr_aggs", [])
        ds = Dataset(file_list[0])

        # Platform, sensor and source
        attr_aggs += [
            AggregatedGlobalAttr(attr="platform", callback=combine_lists),
            AggregatedGlobalAttr(attr="sensor", callback=combine_lists),
            AggregatedGlobalAttr(attr="source", callback=unique_strings)
        ]

        # Time coverage
        for start_attr, end_attr in self.date_range_formats:
            if hasattr(ds, start_attr) and hasattr(ds, end_attr):
                attr_aggs += [
                    AggregatedGlobalAttr(attr=start_attr, callback=min_date),
                    AggregatedGlobalAttr(attr=end_attr, callback=max_date)
                ]
                break

        # Geospatial bounds
        for attr_names in self.geospatial_bounds_formats:
            if all(hasattr(ds, attr) for attr in attr_names):
                n_attr, e_attr, s_attr, w_attr = attr_names
                attr_aggs += [
                    AggregatedGlobalAttr(attr=n_attr, callback=max),
                    AggregatedGlobalAttr(attr=e_attr, callback=max),
                    AggregatedGlobalAttr(attr=s_attr, callback=min),
                    AggregatedGlobalAttr(attr=w_attr, callback=min)
                ]
                break

        return super().create_aggregation(file_list, *args,
                                          global_attrs=global_attrs,
                                          attr_aggs=attr_aggs, **kwargs)

    @classmethod
    def get_global_attrs(cls, file_list, drs):
        """
        Return a dictionary mapping attribute name to value for global
        attributes that an aggregation should have
        """
        attrs = {}

        # Get 'history' from the first file, and add our text to it
        now = datetime.now()
        extra_history = ("{}: The CCI Open Data Portal aggregated all files "
                         "in the dataset over the time variable for OPeNDAP "
                         "access".format(now.strftime("%Y-%m-%d %H:%M:%S")))
        dataset = Dataset(file_list[0])
        try:
            attrs["history"] = dataset.history
            if not attrs["history"].endswith(". "):
                attrs["history"] += ". "
            attrs["history"] += extra_history
        except AttributeError:
            msg = ("WARNING: Could not read 'history' global attribute "
                   "from '{}'")
            print(msg.format(file_list[0]), file=sys.stderr)

        # Overwrite ID with DRS
        attrs["id"] = drs
        # Generate a new tracking ID
        attrs["tracking_id"] = str(uuid4())
        return attrs

    def process_root_element(self, root):
        # Add additional global attributes that require files to have been
        # read first
        attr_dict = {}
        for el in root.findall("attribute"):
            attr_dict[el.attrib["name"]] = el.attrib["value"]

        for start, end in self.date_range_formats:
            if start in attr_dict and end in attr_dict:
                duration = str_to_date(attr_dict[end]) - str_to_date(attr_dict[start])
                self.add_global_attr(root, "time_coverage_duration",
                                     isodate.duration_isoformat(duration))
        return root
