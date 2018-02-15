import os

import pytest
import xml.etree.cElementTree as ET

from modify_catalogs import ProcessBatch
from aggregate import create_aggregation, element_to_string
from partition_files import partition_files


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
        outdir = str(tmpdir_factory.mktemp("output", numbered=True))
        pb = ProcessBatch(["-a"], indir=input_dir, outdir=outdir)
        pb.do_all()
        tree = ET.ElementTree()
        tree.parse(os.path.join(outdir, os.listdir(input_dir)[0]))
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
        nmcl_ns = "http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2"

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
            parsed_el = ET.fromstring(xml)
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

    def test_aggregation(self):
        """
        Test that the method to create an NcML aggregation includes references
        to the input files
        """
        files = ["/path/to/one", "/path/to/two"]
        agg = create_aggregation(files)
        xml = element_to_string(agg)
        for f in files:
            assert 'location="{}"'.format(f) in xml


class TestPartitioning(object):
    def test_partition(self):
        """
        Test the algorithm to detect dates in file paths and partition a list
        into groups
        """
        expected_part = [
            ["/path/one/2018/01/01/f1.nc",
             "/path/one/2018/01/02/f2.nc"],
            ["/path/two/2019/01/01/f3.nc"],
            # Paths only differ by digits but one of the changes is version
            # number - check they get split into two
            ["/path/three/v1/2009/01/01/f4.nc",
             "/path/three/v1/2008/01/01/f5.nc"],
            ["/path/three/v2/2009/01/01/f6.nc"],
            # Same as above but with no alphabetic characters in version
            ["/path/four/1.0/2007/01/01/f7.nc",
             "/path/four/1.0/2003/01/01/f8.nc"],
            ["/path/four/2.0/2007/01/01/f9.nc"]
        ]
        flattened = sum((group for group in expected_part), [])
        part = list(partition_files(flattened))

        assert len(part) == len(expected_part)

        # Order does not matter so convert expected and actual results to sets
        part = set([tuple(l) for l in part])
        expected_part = set([tuple(l) for l in expected_part])

        assert part == expected_part
