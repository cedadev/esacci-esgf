import os
import json
import xml.etree.cElementTree as ET
from glob import glob

import pytest
from netCDF4 import Dataset
import numpy as np

from modify_catalogs import ProcessBatch
from aggregation_utils.aggregate import create_aggregation, element_to_string, AggregationError
from aggregation_utils.partition_files import partition_files
from publication_utils.merge_csv_json import Dataset as CsvRowDataset, parse_file, HEADER_ROW


def get_full_tag(tag, ns="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0"):
    return "{%s}%s" % (ns, tag)


def get_base_tag(full_tag):
    return full_tag.split("}")[1:]


class TestCatalogUpdates(object):
    """
    Tests re updating THREDDS catalog.
    """

    @pytest.fixture
    def thredds_catalog(self, tmpdir_factory):
        """
        Test fixture to return the root element of a processed catalog.
        """
        input_dir = "test_input_catalogs"
        output_dir = str(tmpdir_factory.mktemp("output", numbered=True))
        # Process all catalogs in input dir and create aggregations with WMS
        pb = ProcessBatch(["-aw", "-o", output_dir] + glob("{}/*.xml".format(input_dir)))
        pb.do_all()
        tree = ET.ElementTree()
        tree.parse(os.path.join(output_dir, os.listdir(input_dir)[0]))
        return tree.getroot()

    def has_access_method(self, element, name):
        for access in element.findall(get_full_tag("access")):
            if access.get("serviceName") == name:
                return True
        return False

    def test_wms_wcs_services(self, thredds_catalog):
        """
        Check WMS and WCS services are present
        """
        services = thredds_catalog.findall(get_full_tag("service"))
        assert len(services) > 0
        wms = [s for s in services if s.get("name") == "wms"]
        wcs = [s for s in services if s.get("name") == "wcs"]

        for s in services:
            print(s.get("name") == "wms")
        assert len(wms) > 0
        assert len(wcs) > 0

    def test_aggregate_ds(self, thredds_catalog):
        """
        Test that the aggregate dataset is present and that is has WMS, WCS and
        OpenDAP as access methods
        """
        ncml_ns = "http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2"

        top_level_ds = [el for el in thredds_catalog if el.tag == get_full_tag("dataset")]
        agg_ds = None
        for el in top_level_ds[0]:
            if el.tag == get_full_tag("dataset"):
                for subel in el:
                    if subel.tag == get_full_tag("netcdf", ns=ncml_ns):
                        agg_ds = el
                        break

        assert agg_ds is not None, "Aggregation dataset not found"
        assert self.has_access_method(agg_ds, "wms")
        assert self.has_access_method(agg_ds, "wcs")
        assert self.has_access_method(agg_ds, "OpenDAPServer")


class TestAggregationCreation(object):

    def netcdf_file(self, tmpdir, filename):
        """
        Create a NetCDF file containing just a time dimension with a single
        value. Return the path at which the dataset is saved.
        """
        path = str(tmpdir.join(filename))
        ds = Dataset(path, "w")
        ds.createDimension("time", None)
        time_var = ds.createVariable("time", np.float32, ("time",))
        time_var[:] = [1234]
        ds.close()
        return path

    def test_different_time_units(self, tmpdir):
        """
        Check that the 'timeUnitsChange' attribute is present on the
        aggregation when files have different time units
        """
        diff_files = [
            ("diff_units_1.nc", "days since 1970-01-01 00:00:00 UTC"),
            ("diff_units_2.nc", "days since 1970-01-02 00:00:00 UTC"),
            ("diff_units_3.nc", "days since 1970-01-03 00:00:00 UTC")
        ]
        same_files = [
            ("same_units_1.nc", "days since 1973-01-03 00:00:00 UTC"),
            ("same_units_2.nc", "days since 1973-01-03 00:00:00 UTC"),
            ("same_units_3.nc", "days since 1973-01-03 00:00:00 UTC")
        ]

        for filename, units in diff_files + same_files:
            path = tmpdir.join(filename)
            ds = Dataset(path, "w")
            ds.createDimension("time", None)
            time_var = ds.createVariable("time", np.float32, ("time",))
            time_var.units = units
            time_var[:] = [0]
            ds.close()

        # timeUnitsChange should be present in the aggregation with different
        # time units...
        diff_agg = create_aggregation([tmpdir.join(fname) for fname, _ in diff_files])
        diff_agg_el = list(diff_agg)[0]
        assert "timeUnitsChange" in diff_agg_el.attrib
        assert diff_agg_el.attrib["timeUnitsChange"] == "true"

        # ...but not present otherwise
        same_agg = create_aggregation([tmpdir.join(fname) for fname, _ in same_files])
        same_agg_el = list(same_agg)[0]
        assert "timeUnitsChange" not in same_agg_el.attrib

        # Check coordValue is not present for the different units aggregation
        netcdf_els = diff_agg_el.findall("netcdf")
        assert len(netcdf_els) > 1
        for el in netcdf_els:
            assert "coordValue" not in el.attrib

    def test_xml_to_string(self):
        """
        Test that the method to convert an ET.Element instance to a string
        produces valid XML with correct indentation
        """
        el = ET.Element("parent", myattr="myval")
        ET.SubElement(el, "child", childattr="childval")
        ET.SubElement(el, "child")
        xml = element_to_string(el)

        try:
            _parsed_el = ET.fromstring(xml)
        except ET.ParseError:
            assert False, "element_to_string() returned malformed XML"

        lines = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<parent myattr="myval">',
            '  <child childattr="childval"/>',
            '  <child/>',
            '</parent>'
        ]
        assert xml == os.linesep.join(lines)

    def test_aggregation(self, tmpdir):
        """
        Test that the method to create an NcML aggregation includes references
        to the all the input files and the expected attributes are present
        with correct values
        """
        n = 5
        filenames = ["ds_{}.nc".format(i) for i in range(n)]
        files = [self.netcdf_file(tmpdir, filename) for filename in filenames]

        agg = create_aggregation(files)
        agg_el = list(agg)[0]
        netcdf_els = agg_el.findall("netcdf")

        assert len(netcdf_els) == n

        for i, el in enumerate(netcdf_els):
            assert "location" in el.attrib
            assert "coordValue" in el.attrib
            assert el.attrib["location"].endswith(filenames[i])
            assert el.attrib["coordValue"] == "1234.0"

    def test_file_order(self, tmpdir):
        """
        Test that the file list in the NcML aggregation is in chronological
        order with respect to the time coordinate values in each file
        """
        f1 = self.netcdf_file(tmpdir, "ds_1.nc")
        f2 = self.netcdf_file(tmpdir, "ds_2.nc")
        ds1 = Dataset(f1, "a")
        ds2 = Dataset(f2, "a")

        ds1.variables["time"][:] = 300
        ds2.variables["time"][:] = 10

        ds1.close()
        ds2.close()

        # Give file list in reverse order
        agg = create_aggregation([f1, f2])
        found_files = [el.attrib["location"] for el in list(agg)[0].findall("netcdf")]
        assert found_files == [f2, f1]

    def test_error_when_multiple_time_values(self, tmpdir):
        """
        Check that an error is raised when trying to process a file that
        contains more than one time coordinate value
        """
        f = self.netcdf_file(tmpdir, "ds.nc")
        ds = Dataset(f, "a")
        ds.variables["time"][:] = [1, 2, 3, 4, 5]
        ds.close()
        assert pytest.raises(AggregationError, create_aggregation, [f])


class TestPartitioning(object):
    def test_partition(self):
        """
        Test the algorithm to detect dates in file paths and partition a list
        into groups
        """
        all_files = [
            "/path/one/2018/01/01/f1.nc",
            "/path/one/2018/01/02/f2.nc",
            "/path/two/2019/01/01/f3.nc",
            # Paths only differ by digits but one of the changes is version
            # number - check they get split into two
            "/path/three/v1/2009/01/01/f4.nc",
            "/path/three/v1/2008/01/01/f5.nc",
            "/path/three/v2/2009/01/01/f6.nc",
            # Same as above but with no alphabetic characters in version
            "/path/four/1.0/2007/01/01/f7.nc",
            "/path/four/1.0/2003/01/01/f8.nc",
            "/path/four/2.0/2007/01/01/f9.nc"
        ]

        expected_part = {
            "/path/one/xxxx/xx/xx": [
                "/path/one/2018/01/01/f1.nc",
                "/path/one/2018/01/02/f2.nc"
            ],
            "/path/two/xxxx/xx/xx": ["/path/two/2019/01/01/f3.nc"],

            "/path/three/v1/xxxx/xx/xx": [
                "/path/three/v1/2009/01/01/f4.nc",
                "/path/three/v1/2008/01/01/f5.nc"
            ],

            "/path/three/v2/xxxx/xx/xx": ["/path/three/v2/2009/01/01/f6.nc"],

            "/path/four/1.0/xxxx/xx/xx": [
                "/path/four/1.0/2007/01/01/f7.nc",
                "/path/four/1.0/2003/01/01/f8.nc"
            ],

            "/path/four/2.0/xxxx/xx/xx": ["/path/four/2.0/2007/01/01/f9.nc"]
        }
        assert partition_files(all_files) == expected_part


class TestMergeCSV(object):
    def test_invalid_header(self, tmpdir):
        """
        Check that an invalid header row in the CSV causes an error
        """
        path1 = tmpdir.join("invalid.csv")
        path1.write("not,a,valid,header,row")
        with pytest.raises(ValueError):
            parse_file(str(path1))

        path2 = tmpdir.join("valid.csv")
        path2.write(",".join(HEADER_ROW))
        try:
            parse_file(str(path2))
        except ValueError:
            assert False, "Unexpected ValueError"

    def test_row_parsing(self, tmpdir):
        """
        Check that a row can be parse from a string
        """
        json_file = str(tmpdir.join("f.json"))
        with open(json_file, "w") as f:
            json.dump({"ds": [{"file": "data.nc", "size": 0, "mtime": 0, "sha256": 0}]}, f)

        create = CsvRowDataset.from_strings

        # Check extraneous whitespace is ignored and Yes/No to boolean
        # conversion
        got1 = create(["ds", "100", " url", "title, here", "Yes", json_file])
        expected1 = CsvRowDataset("ds", 100, "url", "title, here", True, json_file)
        assert got1 == expected1

        got2 = create(["ds", "100", " url", "title, here", "No", json_file])
        expected2 = CsvRowDataset("ds", 100, "url", "title, here", False, json_file)
        assert got2 == expected2

        # Check invalid int and bool values
        assert pytest.raises(ValueError, create, ["ds", "blah", "url", "title", "Yes", json_file])
        assert pytest.raises(ValueError, create, ["ds", "200", "url", "title", "blah", json_file])
