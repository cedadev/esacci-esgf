import os
import sys
import json
import xml.etree.cElementTree as ET
from glob import glob
from io import StringIO

import pytest
from netCDF4 import Dataset
import numpy as np

from modify_catalogs import ProcessBatch
from find_ncml import find_ncml_references
from find_netcdf import find_netcdf_references
from aggregation_utils.aggregate import create_aggregation, element_to_string, AggregationError
from aggregation_utils.partition_files import partition_files
from aggregation_utils.cache_remote_aggregations import AggregationCacher
from publication_utils.merge_csv_json import Dataset as CsvRowDataset, parse_file, HEADER_ROW
from publication_utils.get_host_from_ini import HostnameExtractor
from make_mapfiles import MakeMapfile


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
        aggregation when files have different time units and time coordinates
        are cached
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
        diff_agg = create_aggregation([tmpdir.join(fname) for fname, _ in diff_files],
                                      "time", cache=True)
        diff_agg_el = list(diff_agg)[0]
        assert "timeUnitsChange" in diff_agg_el.attrib
        assert diff_agg_el.attrib["timeUnitsChange"] == "true"

        # ...but not present otherwise
        same_agg = create_aggregation([tmpdir.join(fname) for fname, _ in same_files],
                                      "time", cache=True)
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

        agg = create_aggregation(files, "time", cache=True)
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
        Test that the file list in the NcML aggregation is sorted in time order
        when cache=True, and in the order given otherwise
        """
        f1 = self.netcdf_file(tmpdir, "ds_1.nc")
        f2 = self.netcdf_file(tmpdir, "ds_2.nc")
        ds1 = Dataset(f1, "a")
        ds2 = Dataset(f2, "a")

        ds1.variables["time"][:] = 300
        ds2.variables["time"][:] = 10

        ds1.close()
        ds2.close()

        # Give file list in reverse order - result should be sorted
        agg = create_aggregation([f1, f2], "time", cache=True)
        found_files = [el.attrib["location"] for el in list(agg)[0].findall("netcdf")]
        assert found_files == [f2, f1]

        # Don't cache coordinate values - should stay in the wrong order
        agg2 = create_aggregation([f1, f2], "time", cache=False)
        found_files2 = [el.attrib["location"] for el in list(agg2)[0].findall("netcdf")]
        assert found_files2 == [f1, f2]

    def test_error_when_multiple_time_values(self, tmpdir):
        """
        Check that an error is raised when trying to process a file that
        contains more than one time coordinate value
        """
        f = self.netcdf_file(tmpdir, "ds.nc")
        ds = Dataset(f, "a")
        ds.variables["time"][:] = [1, 2, 3, 4, 5]
        ds.close()
        assert pytest.raises(AggregationError, create_aggregation, [f], "time",
                             cache=True)

    def test_no_caching(self, tmpdir):
        """
        Check that files are not opened if cache=False when creating an
        aggregation
        """
        f = self.netcdf_file(tmpdir, "ds.nc")
        try:
            create_aggregation([f], "nonexistantdimension", cache=False)
        except AggregationError as ex:
            assert False, "Unexpected error: {}".format(ex)


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
        Check that a row can be parsed from a string
        """
        json_file = str(tmpdir.join("f.json"))
        with open(json_file, "w") as f:
            json.dump({"ds": [{"file": "data.nc", "size": 0, "mtime": 0, "sha256": 0}]}, f)

        create = CsvRowDataset.from_strings

        # Check extraneous whitespace is ignored and Yes/No to boolean
        # conversion
        got1 = create(["ds", "100", " url", "title, here", "no", "Yes", json_file])
        expected1 = CsvRowDataset("ds", 100, "url", "title, here", False, True, json_file)
        assert got1 == expected1

        got2 = create(["ds", "100", " url", "title, here", "No", "yes", json_file])
        expected2 = CsvRowDataset("ds", 100, "url", "title, here", False, True, json_file)
        assert got2 == expected2

        # Check invalid int and bool values
        assert pytest.raises(ValueError, create, ["ds", "blah", "url", "title", "Yes", json_file])
        assert pytest.raises(ValueError, create, ["ds", "200", "url", "title", "blah", json_file])

    def test_parse(self, tmpdir):
        """
        Parse a CSV file and check the output JSON is valid and what we expect
        """
        json_file = tmpdir.join("f.json")
        obj = {"ds": [{"file": "data.nc", "size": 0, "mtime": 0, "sha256": 0}]}
        with open(str(json_file), "w") as f:
            json.dump(obj, f)

        csv_file = tmpdir.join("f.csv")
        csv_file.write("\n".join([
            ",".join(HEADER_ROW),
            "ds,1,url,title,yes,no,{}".format(str(json_file))
        ]))

        expected = {
            "ds": {
                "generate_aggregation": True,
                "include_in_wms": False,
                "tech_note_title": "title",
                "tech_note_url": "url",
                "files": [
                    {"path": "data.nc", "size": 0, "mtime": 0, "sha256": 0}
                ]
            }
        }

        s = StringIO()
        sys.stdout = s
        parse_file(str(csv_file))
        sys.stdout = sys.__stdout__

        output_json = s.getvalue()
        try:
            parsed = json.loads(output_json)
        except ValueError:
            assert False, "parse_file() produced invalid JSON"

        assert parsed == expected


class TestMakeMapfile(object):
    def test_extract_version(self):
        valid = [
            ("mydataset.v1234", ("mydataset", "1234")),
            ("otherdataset.v9", ("otherdataset", "9"))
        ]
        invalid = [
            "noversion",
            "badversion.vABCDE",
            "other.v"
        ]
        mm = MakeMapfile("/tmp")

        for dsid, expected in valid:
            assert mm.split_versioned_dsid(dsid) == expected

        for dsid in invalid:
            with pytest.raises(ValueError):
                mm.split_versioned_dsid(dsid)

    def test_mapfile_line(self):
        mm = MakeMapfile("/tmp")
        file_dict = {
            "size": 1, "mtime": 2.123456, "sha256": 3, "path": "/some/file.nc"
        }
        expected = ("mydataset#12345 | /some/file.nc | 1 | mod_time=2.12346 | "
                    "checksum=3 | checksum_type=SHA256\n")
        got = mm.get_mapfile_line("mydataset", "12345", file_dict,
                                  tech_notes=None)
        assert got == expected

        expected2 = ("mydataset#12345 | /some/file.nc | 1 | mod_time=2.12346 | "
                     "checksum=3 | checksum_type=SHA256 | "
                     "dataset_tech_notes=http://tech.notes | "
                     "dataset_tech_notes_title=title for the tech notes\n")
        tech_notes = {"title": "title for the tech notes",
                      "url": "http://tech.notes"}
        got2 = mm.get_mapfile_line("mydataset", "12345", file_dict,
                                   tech_notes=tech_notes)
        assert got2 == expected2

    def test_get_mapfile_path(self):
        mm = MakeMapfile("/tmp", depth=3)
        tests = [
            ("my.dataset", "/tmp/my/dataset/my.dataset"),
            ("lots.of.facets.in.dataset.name",
             "/tmp/lots/of/facets/lots.of.facets.in.dataset.name")
        ]
        for dsid, expected in tests:
            assert mm.get_mapfile_path(dsid) == expected

    def test_tech_notes(self, tmpdir):
        """
        Check that tech notes are only included in the first line of the
        generated mapfiles
        """
        outdir = tmpdir.join("mapfiles")
        mm = MakeMapfile(str(outdir))
        json_file = tmpdir.join("input.json")
        json_file.write(json.dumps({
            "myds.v1234": {
                "tech_note_url": "http://tech.notes",
                "tech_note_title": "my tech notes",
                "generate_aggregation": False,
                "include_in_wms": False,
                "files": [
                    {"path": "/data/file1.nc", "sha256": "1", "mtime": 1, "size": 1},
                    {"path": "/data/file2.nc", "sha256": "2", "mtime": 2, "size": 2}
                ]
            }
        }))

        s = StringIO()
        sys.stdout = s
        mm.make_mapfiles(str(json_file))
        sys.stdout = sys.__stdout__

        mapfile_path = s.getvalue().strip()
        assert os.path.isfile(mapfile_path)
        with open(mapfile_path) as f:
            lines = f.readlines()
        assert len(lines) == 2
        l1, l2 = lines

        assert "dataset_tech_notes" in l1
        assert "dataset_tech_notes" not in l2


class TestAggregationCaching(object):
    def test_get_agg_url(self, tmpdir):
        json_file = tmpdir.join("ds.json")
        json_file.write(json.dumps({
            "opendap-dataset": {
                "generate_aggregation": True,
                "include_in_wms": False,
                "tech_note_url": "some url",
                "tech_note_title": "some title",
                "files": []
            },
            "wms-dataset": {
                "generate_aggregation": True,
                "include_in_wms": True,
                "tech_note_url": "some url",
                "tech_note_title": "some title",
                "files": []
            },
            "no-aggregation-dataset": {
                "generate_aggregation": False,
                "include_in_wms": False,
                "tech_note_url": "some url",
                "tech_note_title": "some title",
                "files": []
            },
        }))

        ac = AggregationCacher(str(json_file), "http://server")
        expected = [
            "http://server/dodsC/opendap-dataset.dds",
            "http://server/wms/wms-dataset?service=WMS&version=1.3.0&request=GetCapabilities"
        ]
        assert set(ac.get_all_urls()) == set(expected)


class TestHostnameExtractor(object):
    @classmethod
    def do_test(self, tmpdir, ini_lines, service):
        ini = tmpdir.join("esg.ini")
        host = "some-host.ac.uk"
        ini.write("\n".join(ini_lines).format(host=host))
        assert HostnameExtractor.get_hostname(str(ini), service) == host

    def test_thredds_host(self, tmpdir):
        lines = [
            "[DEFAULT]",
            "thredds_url = http://{host}/thredds/data",
        ]
        self.do_test(tmpdir, lines, "thredds")

    def test_solr_use_result_api(self, tmpdir):
        lines = [
            "[DEFAULT]",
            "use_rest_api = true",
            "rest_service_url = http://{host}/solr/one/two/three",
            "hessian_service_url = somethingelse",
        ]
        self.do_test(tmpdir, lines, "solr")

    def test_solr_no_rest_api(self, tmpdir):
        lines = [
            "[DEFAULT]",
            "use_rest_api = ",
            "rest_service_url = somethingelse",
            "hessian_service_url = http://{host}/solr/one/two/three",
        ]
        self.do_test(tmpdir, lines, "solr")

    def test_solr_no_rest_api_2(self, tmpdir):
        lines = [
            "[DEFAULT]",
            "rest_service_url = somethingelse",
            "hessian_service_url = http://{host}/solr/one/two/three",
        ]
        self.do_test(tmpdir, lines, "solr")

    def test_solr_no_rest_api_3(self, tmpdir):
        lines = [
            "[DEFAULT]",
            "hessian_service_url = http://{host}/solr/one/two/three",
        ]
        self.do_test(tmpdir, lines, "solr")

    def test_solr_no_rest_url(self, tmpdir):
        lines = [
            "[DEFAULT]",
            "use_rest_api = true",
            "hessian_service_url = http://{host}/solr/one/two/three",
        ]
        self.do_test(tmpdir, lines, "solr")


class TestNcmlFinder(object):
    def test_no_ncml(self, tmpdir):
        """
        Check that no paths are returned if no NcML files are referenced in the
        XML
        """
        catalog = tmpdir.join("catalog.xml")
        catalog.write("""
            <?xml version="1.0" encoding="UTF-8"?>
            <catalog>
                <dataset name="some.dataset" ID="some.dataset">
                </dataset>
            </catalog>
        """.strip())
        got = list(find_ncml_references(str(catalog)))
        assert got == []

    def test_ncml_present(self, tmpdir):
        """
        Check paths are returned when expected
        """
        catalog = tmpdir.join("catalog.xml")
        catalog.write("""
            <?xml version="1.0" encoding="UTF-8"?>
            <catalog xmlns="some-namespace1">
                <dataset name="some.dataset" ID="some.dataset">
                    <dataset>
                        <netcdf location="/my/ncml/aggregation.ncml"/>
                    </dataset>
                    <dataset>
                        <netcdf xmlns="some-namespace2"
                                location="/my/other/aggregation.ncml"/>
                    </dataset>
                </dataset>
            </catalog>
        """.strip())
        expected = ["/my/ncml/aggregation.ncml", "/my/other/aggregation.ncml"]
        got = list(find_ncml_references(str(catalog)))
        assert got == expected

    def test_non_netcdf_element(self, tmpdir):
        """
        Check that other elements with a 'location' attribute are not also
        included
        """
        catalog = tmpdir.join("catalog.xml")
        catalog.write("""
            <?xml version="1.0" encoding="UTF-8"?>
            <catalog>
                <dataset name="some.dataset" ID="some.dataset">
                    <dataset><somethingelsenetcdf location="/not/an/aggregation"/></dataset>
                    <dataset><netcdfsomethingelse location="/also/not/an/aggregation"/></dataset>
                </dataset>
            </catalog>
        """.strip())
        got = list(find_ncml_references(str(catalog)))
        assert got == []

class TestNetcdfFinder(object):
    def test_netcdf_present(self, tmpdir):
        """
        Check that a NetCDF file is found and the dataset roots are replaced
        with path on disk
        """
        catalog = tmpdir.join("catalog.xml")
        catalog.write("""
            <?xml version="1.0" encoding="UTF-8"?>
            <catalog>
                <dataset name="some.dataset1" ID="some.dataset1" urlPath="prefix1/one.nc"/>
                <dataset name="some.dataset2" ID="some.dataset2" urlPath="prefix2/two.nc"/>
                <dataset name="some.dataset3" ID="some.dataset3" urlPath="prefix3/three.nc"/>
                <dataset name="some.dataset4" ID="some.dataset4">
                    <dataset name="nested.dataset" ID="nested.dataset" urlPath="nested.nc"/>
                </dataset>
            </catalog>
        """.strip())
        roots = {
            "prefix1": "/first/path",
            "prefix2": "/second/path"
        }
        got = list(find_netcdf_references(str(catalog), dataset_roots=roots))
        assert got == ["/first/path/one.nc", "/second/path/two.nc",
                       "prefix3/three.nc", "nested.nc"]
