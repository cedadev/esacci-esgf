import os

import pytest
import xml.etree.cElementTree as ET

from add_wms_for_wms_server import ProcessBatch as ProcessWmsBatch
from add_wms_for_data_node import (ProcessBatch as ProcessDataNodeBatch,
                                   ThreddsXMLDatasetOnDataNode)


def get_full_tag(tag):
    ns = "http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0"
    return "{%s}%s" % (ns, tag)


def get_base_tag(full_tag):
    return full_tag.split("}")[1:]


class BaseTest(object):
    def get_catalog(self, pb_class, tmpdir_factory):
        """
        Return root element of a processed catalog using the process batch
        class given
        """
        input_dir = "test_input_catalogs"
        outdir = str(tmpdir_factory.mktemp("output", numbered=True))
        pb = pb_class(["-a"], indir=input_dir, outdir=outdir)
        pb.do_all()
        tree = ET.ElementTree()
        tree.parse(os.path.join(outdir, os.listdir(input_dir)[0]))
        return tree.getroot()

    @pytest.fixture
    def data_node_catalog(self, tmpdir_factory):
        return self.get_catalog(ProcessDataNodeBatch, tmpdir_factory)

    @pytest.fixture
    def wms_server_catalog(self, tmpdir_factory):
        return self.get_catalog(ProcessWmsBatch, tmpdir_factory)

    def has_access_method(self, element, name):
        for access in element.findall(get_full_tag("access")):
             if access.get("serviceName") == name:
                return True
        return False

class TestCommon(BaseTest):
    """
    Tests common to data node and WMS server
    """
    def test_wms_wcs_services(self, data_node_catalog, wms_server_catalog):
        """
        Check WMS and WCS services are present
        """
        for cat in (data_node_catalog, wms_server_catalog):
            services = cat.findall(get_full_tag("service"))
            assert len(services) > 0
            wms = [s for s in services if s.get("name") == "wms"]
            wcs = [s for s in services if s.get("name") == "wcs"]

            for s in services:
                print(s.get("name") == "wms")
            assert len(wms) > 0
            assert len(wcs) > 0


class TestDataNode(BaseTest):
    def test_access_methods(self, data_node_catalog):
        """
        Test that WMS, WCS and OpenDAP are listed as access methods in the top
        level dataset
        """
        top_level_ds = [el for el in data_node_catalog if el.tag == get_full_tag("dataset")]
        for ds in top_level_ds:
            assert self.has_access_method(ds, "wms")
            assert self.has_access_method(ds, "wcs")
            assert self.has_access_method(ds, ThreddsXMLDatasetOnDataNode.REMOTE_OPENDAP_SERVICE_NAME)


class TestWmsServer(BaseTest):
    def test_access_methods(self, wms_server_catalog):
        """
        Test that WMS, WCS and OpenDAP are listed as access methods in the
        aggregate dataset
        """
        top_level_ds = [el for el in wms_server_catalog if el.tag == get_full_tag("dataset")]
        agg_ds = top_level_ds[0].findall(get_full_tag("dataset"))[0]
        assert self.has_access_method(agg_ds, "wms")
        assert self.has_access_method(agg_ds, "wcs")
        assert self.has_access_method(agg_ds, "OpenDAPServer")
