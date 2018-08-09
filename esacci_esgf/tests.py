import os
import sys
import re
import json
import xml.etree.cElementTree as ET
from glob import glob
from io import StringIO
import freezegun

import pytest
import numpy as np
from netCDF4 import Dataset

from esacci_esgf.modify_catalogs import ProcessBatch
from esacci_esgf.input.merge_csv_json import Dataset as CsvRowDataset, parse_file, HEADER_ROW
from esacci_esgf.input.parse_esg_ini import EsgIniParser
from esacci_esgf.input.make_mapfiles import MakeMapfile
from esacci_esgf.aggregation.base import CCIAggregationCreator


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
        input_dir = os.path.abspath("esacci_esgf/test_input_catalogs")
        output_dir = str(tmpdir_factory.mktemp("output", numbered=True))
        # Process all catalogs in input dir and create aggregations with WMS
        test_files = glob("{}/*.xml".format(input_dir))
        assert test_files, "No test catalogs found"

        pb = ProcessBatch(["-aw", "-o", output_dir] + test_files)
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

        properties = agg_ds.findall(get_full_tag("property"))
        assert len(properties) == 1
        assert "name" in properties[0].attrib
        assert "value" in properties[0].attrib
        assert "jasmin.eofrom.space" in properties[0].attrib["value"]


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


class TestEsgIniParser(object):
    @classmethod
    def do_hostname_test(self, tmpdir, ini_lines, key):
        ini = tmpdir.join("esg.ini")
        host = "some-host.ac.uk"
        ini.write("\n".join(ini_lines).format(host=host))
        assert EsgIniParser.get_value(str(ini), key) == host

    def test_thredds_host(self, tmpdir):
        lines = [
            "[DEFAULT]",
            "thredds_url = http://{host}/thredds/data",
        ]
        self.do_hostname_test(tmpdir, lines, "thredds_host")

    def test_solr_use_result_api(self, tmpdir):
        lines = [
            "[DEFAULT]",
            "use_rest_api = true",
            "rest_service_url = http://{host}/solr/one/two/three",
            "hessian_service_url = somethingelse",
        ]
        self.do_hostname_test(tmpdir, lines, "solr_host")

    def test_solr_no_rest_api(self, tmpdir):
        lines = [
            "[DEFAULT]",
            "use_rest_api = ",
            "rest_service_url = somethingelse",
            "hessian_service_url = http://{host}/solr/one/two/three",
        ]
        self.do_hostname_test(tmpdir, lines, "solr_host")

    def test_solr_no_rest_api_2(self, tmpdir):
        lines = [
            "[DEFAULT]",
            "rest_service_url = somethingelse",
            "hessian_service_url = http://{host}/solr/one/two/three",
        ]
        self.do_hostname_test(tmpdir, lines, "solr_host")

    def test_solr_no_rest_api_3(self, tmpdir):
        lines = [
            "[DEFAULT]",
            "hessian_service_url = http://{host}/solr/one/two/three",
        ]
        self.do_hostname_test(tmpdir, lines, "solr_host")

    def test_solr_no_rest_url(self, tmpdir):
        lines = [
            "[DEFAULT]",
            "use_rest_api = true",
            "hessian_service_url = http://{host}/solr/one/two/three",
        ]
        self.do_hostname_test(tmpdir, lines, "solr_host")

    def test_thredds_data_path(self, tmpdir):
        ini = tmpdir.join("esg.ini")
        ini.write("\n".join([
            "[DEFAULT]",
            "thredds_dataset_roots =",
            "    some_root | blah",
            " \t  esg_esacci       |      /my/data    ",
            " some_other_root | bleh"
        ]))
        assert EsgIniParser.get_value(str(ini), "thredds_data_path") == "/my/data"

    def test_thredds_data_path_not_found(self, tmpdir):
        ini = tmpdir.join("esg.ini")
        ini.write("\n".join([
            "[DEFAULT]",
            "thredds_dataset_roots =",
            "   root1  | a",
            "   root2  | b",
            "   esacci | c"
        ]))
        with pytest.raises(ValueError):
            EsgIniParser.get_value(str(ini), "thredds_data_path")

class TestAggregations:
    def netcdf_file(self, tmpdir, filename, dim="time", values=[1234],
                    units=None, global_attrs=None):
        """
        Create a NetCDF file containing a single dimension. Return the path
        at which the dataset is saved.
        """
        path = str(tmpdir.join(filename))
        ds = Dataset(path, "w")
        ds.createDimension(dim, None)
        var = ds.createVariable(dim, np.float32, (dim,))
        if units:
            var.units = units
        var[:] = values
        if global_attrs:
            for attr, value in global_attrs.items():
                setattr(ds, attr, value)
        ds.close()
        return path

    def get_attrs_dict(self, root_element):
        """
        Extract <attribute> tags from root XML element, and return the
        attributes as a dictionary mapping attribute names to
        {"value": val, "type": type}
        """
        attr_elements = root_element.findall("attribute")
        attrs_dict = {}
        for el in attr_elements:
            attrs_dict[el.attrib["name"]] = {
                "value": el.attrib["value"],
                "type": el.attrib.get("type", None)
            }
        return attrs_dict

    def test_time_coverage_attributes(self, tmpdir):
        formats = [
            ("time_coverage_start", "time_coverage_end"),
            ("start_time", "stop_time"),
        ]
        for i, (start_attr_name, end_attr_name) in enumerate(formats):
            data_dir = tmpdir.mkdir(str(i))
            files = [
                self.netcdf_file(data_dir, "f1.nc", values=[1], global_attrs={
                    # 1st Jan 2000. Sometimes omit the 'T' separator and seconds to
                    # mimic the real data
                    start_attr_name: "200001010745Z",
                    end_attr_name:   "20000101T120000Z",
                }),
                self.netcdf_file(data_dir, "f2.nc", values=[2], global_attrs={
                    # 4th Jan 2000
                    start_attr_name: "20000104T000000Z",
                    end_attr_name:   "200001041200Z",
                }),
                self.netcdf_file(data_dir, "f3.nc", values=[3], global_attrs={
                    # 6th Jan 2000
                    start_attr_name: "200001060000Z",
                    end_attr_name:   "20000106T120000Z",
                })
            ]
            agg = CCIAggregationCreator("time").create_aggregation("drs", files)

            attrs_dict = self.get_attrs_dict(agg)
            assert start_attr_name in attrs_dict
            assert end_attr_name in attrs_dict
            assert "time_coverage_duration" in attrs_dict

            assert attrs_dict[start_attr_name]["value"] == "20000101T074500Z"
            assert attrs_dict[end_attr_name]["value"] == "20000106T120000Z"
            assert attrs_dict["time_coverage_duration"]["value"] == "P5DT4H15M"

    def test_multiple_time_coverage_attrs(self, tmpdir):
        """
        Check that aggregation of time coverage attributes is done when more
        than one format is used.

        Also check that 'time_coverage_{start,end}' is present in the
        aggregation regardless of whether it is in the source files
        """
        files = [
            self.netcdf_file(tmpdir, "f1.nc", values=[1], global_attrs={
                "start_time": "200001010745Z",
                "stop_time":   "20000101T120000Z",

                "start_date": "01-JAN-2000 07:45:00.000000",
                "stop_date":   "01-JAN-2000 12:00:00.000000",
            }),
            self.netcdf_file(tmpdir, "f2.nc", values=[2], global_attrs={
                "start_time": "20000101T120000Z",
                "stop_time":   "200001041200Z",

                "start_date": "04-JAN-2000 00:00:00.000000",
                "stop_date":   "04-JAN-2000 12:00:00.000000",
            })
        ]
        agg = CCIAggregationCreator("time").create_aggregation("drs", files)

        attrs_dict = self.get_attrs_dict(agg)
        expected_attrs = [
            "start_time", "stop_time",
            "start_date", "stop_date",
            # Still expect to find time_coverage_{start,end} even though
            # they're not in the source files
            "time_coverage_start", "time_coverage_end",
            "time_coverage_duration"
        ]
        for attr in expected_attrs:
            assert attr in attrs_dict

        assert attrs_dict["time_coverage_start"]["value"] == "20000101T074500Z"
        assert attrs_dict["start_time"]["value"] == "20000101T074500Z"
        # Note that ISO date is still used in output even though input format
        # is different for {start,stop}_date
        assert attrs_dict["start_date"]["value"] == "20000101T074500Z"

        assert attrs_dict["time_coverage_end"]["value"] == "20000104T120000Z"
        assert attrs_dict["stop_time"]["value"] == "20000104T120000Z"
        assert attrs_dict["stop_date"]["value"] == "20000104T120000Z"

        assert attrs_dict["time_coverage_duration"]["value"] == "P3DT4H15M"

    def test_global_attributes(self, tmpdir):
        files = [
            self.netcdf_file(tmpdir, "f.nc", values=[1], global_attrs={"history": "helo"})
        ]
        agg = CCIAggregationCreator("time").create_aggregation("mydrs", files)
        attr_dict = self.get_attrs_dict(agg)

        assert "history" in attr_dict
        assert ("The CCI Open Data Portal aggregated all files in the dataset"
                in attr_dict["history"]["value"])

        assert "id" in attr_dict
        assert attr_dict["id"]["value"] == "mydrs"

        assert "tracking_id" in attr_dict
        assert re.match("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
                        attr_dict["tracking_id"]["value"])

    def test_sensor_platform_source_attributes(self, tmpdir):
        platform_values = [
            "one,two,three",
            "two, four,",
            "five,one, three"
        ]
        sensor_values = [
            "dog,cat,frog",
            "cat, cow",
            "rabbit,dog, frog"
        ]
        source_values = [
            "source1 ",
            "something,with,commas",
            "something,else,with,commas"
        ]
        files = []
        for i, (platforms, sensors, source) in enumerate(zip(platform_values, sensor_values, source_values)):
            files.append(self.netcdf_file(
                tmpdir,
                "f_{}.nc".format(i),
                values=[i],
                global_attrs={"platform": platforms, "sensor": sensors, "source": source}
            ))

        agg = CCIAggregationCreator("time").create_aggregation("mydrs", files)
        attr_dict = self.get_attrs_dict(agg)

        assert "platform" in attr_dict
        assert attr_dict["platform"]["value"] == "five,four,one,three,two"
        assert "sensor" in attr_dict
        assert attr_dict["sensor"]["value"] == "cat,cow,dog,frog,rabbit"
        assert "source" in attr_dict
        assert attr_dict["source"]["value"] == (
            "something,else,with,commas,something,with,commas,source1"
        )

    def test_removed_attributes(self, tmpdir):
        files = [
            self.netcdf_file(tmpdir, "f.nc", values=[1], global_attrs={
                "number_of_processed_orbits": "12",
                "number_of_files_composited": "104",
                "creation_date": "some date",
            })
        ]
        agg = CCIAggregationCreator("time").create_aggregation("mydrs", files)
        remove_elements = agg.findall("remove")
        assert len(remove_elements) >= 2
        remove_names = [el.attrib["name"] for el in remove_elements]
        assert "number_of_processed_orbits" in remove_names
        assert "number_of_files_composited" in remove_names
        assert "creation_date" in remove_names

    @freezegun.freeze_time("2018-12-25", tz_offset=0)
    def test_date_created(self, tmpdir):
        files = [
            self.netcdf_file(tmpdir, "f1.nc", values=[1], global_attrs={
                "date_created": "some date that should not be parsed"
            }),
            self.netcdf_file(tmpdir, "f2.nc", values=[2], global_attrs={
                "date_created": "another date"
            })
        ]

        agg = CCIAggregationCreator("time").create_aggregation("mydrs", files)
        attrs_dict = self.get_attrs_dict(agg)
        assert "date_created" in attrs_dict
        assert attrs_dict["date_created"]["value"] == "20181225T000000Z"

    def test_geospatial_attributes(self, tmpdir):
        # List attribute names as N E S W
        formats = [
            ("geospatial_lat_max", "geospatial_lon_max", "geospatial_lat_min",
             "geospatial_lon_min"),
            ("nothernmost_latitude", "easternmost_longitude",
             "southernmost_latitude", "westernmost_longitude")
        ]
        for i, attr_names in enumerate(formats):
            n_attr, e_attr, s_attr, w_attr = attr_names
            data_dir = tmpdir.mkdir(str(i))
            files = [
                self.netcdf_file(data_dir, "f1.nc", values=[1], global_attrs={
                    w_attr: 0.0,
                    e_attr: 45.0,
                    s_attr: -70.0,
                    n_attr: 10.0
                }),
                self.netcdf_file(data_dir, "f2.nc", values=[2], global_attrs={
                    w_attr: -120.0,
                    e_attr: 45.0,
                    s_attr: 0.0,
                    n_attr: 85.0
                }),
                self.netcdf_file(data_dir, "f3.nc", values=[3], global_attrs={
                    w_attr: -119.0,
                    e_attr: 175.0,
                    s_attr: 75.0,
                    n_attr: 77.0
                })
            ]
            agg = CCIAggregationCreator("time").create_aggregation("drs", files)

            attrs_dict = self.get_attrs_dict(agg)
            for attr in attr_names:
                assert attr in attrs_dict
                assert attrs_dict[attr]["type"] == "float"

            assert attrs_dict[w_attr]["value"] == "-120.0"
            assert attrs_dict[e_attr]["value"] == "175.0"
            assert attrs_dict[s_attr]["value"] == "-70.0"
            assert attrs_dict[n_attr]["value"] == "85.0"
