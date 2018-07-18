import sys
from datetime import datetime
from uuid import uuid4

from netCDF4 import Dataset
import isodate

from tds_utils.aggregation import AggregationCreator


class CCIAggregationCreator(AggregationCreator):

    def create_aggregation(self, drs, file_list, *args, **kwargs):
        # Override default method to add extra global attributes
        global_attrs = kwargs.pop("global_attrs", {})
        global_attrs.update(self.get_global_attrs(file_list, drs))

        return super().create_aggregation(file_list, *args,
                                          global_attrs=global_attrs, **kwargs)

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
        # read and sorted by time first
        aggregation_element = root.findall("aggregation")[0]
        files = aggregation_element.findall("netcdf")
        attrs = {}

        # List of start/end datetime attribute names. This is required since
        # unfortunately not all products use the same attribute names
        date_range_formats = [
            ("time_coverage_start", "time_coverage_end"),
            ("start_time", "stop_time")
        ]
        first_ds = Dataset(files[0].attrib["location"])
        last_ds = Dataset(files[-1].attrib["location"])
        for start_attr_name, end_attr_name in date_range_formats:
            try:
                start = getattr(first_ds, start_attr_name)
                end = getattr(last_ds, end_attr_name)
                attrs[start_attr_name] = start
                attrs[end_attr_name] = end

                duration = isodate.parse_datetime(end) - isodate.parse_datetime(start)
                attrs["time_coverage_duration"] = isodate.duration_isoformat(duration)
                break
            except AttributeError:
                continue
        else:
            print("WARNING: Could not read start/end coverage times", file=sys.stderr)

        for attr, value in attrs.items():
            self.add_global_attr(root, attr, value)
        return root
