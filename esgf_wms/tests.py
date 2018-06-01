import os
import sys
import json
import xml.etree.cElementTree as ET
from glob import glob
from io import StringIO

import pytest

from esgf_wms.modify_catalogs import ProcessBatch
from esgf_wms.input.merge_csv_json import Dataset as CsvRowDataset, parse_file, HEADER_ROW
from esgf_wms.input.parse_esg_ini import EsgIniParser
from esgf_wms.input.make_mapfiles import MakeMapfile


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
        input_dir = os.path.abspath("esgf_wms/test_input_catalogs")
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
