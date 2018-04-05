#!/usr/bin/env python3
"""
Script to modify THREDDS xml files to remove ESGF-specific markup. Optionally
create NcML aggregations and make these accessible through OPeNDAP/WMS/WCS.
"""

import sys
import os
import traceback
import xml.etree.cElementTree as ET
import argparse

from cached_property import cached_property

from aggregation_utils.partition_files import partition_files
from aggregation_utils.aggregate import create_aggregation, AggregationError


class ThreddsXMLBase(object):
    """
    Base class re generic stuff we want to do to THREDDS XML files
    """
    def __init__(self,
                 ns="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0",
                 encoding="UTF-8",
                 xlink="http://www.w3.org/1999/xlink"):
        self.ns = ns
        self.encoding = encoding
        self.xlink = xlink
        self.in_filename = None
        self.tree = None
        self.root = None

    def set_root(self, root):
        self.tree = ET.ElementTree(root)
        self.root = root
        self.root.set("xmlns:xlink", self.xlink)

    def read(self, filename):
        self.in_filename = filename
        ET.register_namespace("", self.ns)
        self.tree = ET.ElementTree()
        self.tree.parse(filename)
        self.root = self.tree.getroot()
        self.root.set("xmlns:xlink", self.xlink)

    def write(self, filename):
        tmpfile = filename + ".tmp"
        self.tree.write(tmpfile, encoding=self.encoding, xml_declaration=True)
        os.system("xmllint --format %s > %s" % (tmpfile, filename))
        os.remove(tmpfile)

    def tag_full_name(self, tag_base_name):
        return "{%s}%s" % (self.ns, tag_base_name)

    def tag_base_name(self, tag_full_name):
        return tag_full_name[tag_full_name.index("}") + 1:]

    def tag_base_name_is(self, element, tag_name):
        return self.tag_base_name(element.tag) == tag_name

    def insert_element_before_similar(self, parent, new_child):
        """
        Add a child element, if possible putting it before another child with the same tag
        """
        new_tag = self.tag_base_name(new_child.tag)
        for i, child in enumerate(parent.getchildren()):
            if not self.tag_base_name_is(child, new_tag):
                parent.insert(i, new_child)
                break
        else:
            parent.append(new_child)

    def new_element(self, tag_base_name, *args, **attributes):
        """
        Create a new element. Arguments are the tag name, a single optional positional argument
        which is the element text, and then the attributes.
        """
        el = ET.Element(self.tag_full_name(tag_base_name), **attributes)
        if args:
            (text,) = args
            el.text = text
        return el

    def new_child(self, parent, *args, **kwargs):
        """
        As new_element, but add result as child of specified parent element
        """
        child = self.new_element(*args, **kwargs)
        parent.append(child)
        return child


class ThreddsXMLDataset(ThreddsXMLBase):
    """
    An intermediate class re THREDDS catalogs that describe datasets -
    methods in common to what we want to do on the data node
    and on the WMS server

       AND

    A class for processing THREDDS XML files and tweaking them to add WMS tags.

    """

    def __init__(self,
                 thredds_roots=None,
                 do_wcs=False,
                 aggregations_dir="/usr/local/aggregations",
                 **kwargs):
        """
        aggregations_dir is the directory in which NcML files will be placed on the
        server (used to reference aggregations from the THREDDS catalog)
        """

        super().__init__(**kwargs)

        self.thredds_roots = thredds_roots or {}
        self.thredds_roots.setdefault("esg_esacci", "/neodc/esacci")
        self.do_wcs = do_wcs
        self.aggregations_dir = aggregations_dir

        # For each NcML file for an aggregation of datasets, map
        # (file basename, subdir) -> ThreddsXMLBase, where subdir is the subdirectory
        # of self.aggregations_dir in which the file will live
        self.aggregations = {}

    @cached_property
    def top_level_dataset(self):
        for child in self.root.getchildren():
            if self.tag_base_name_is(child, "dataset"):
                return child

    @cached_property
    def second_level_datasets(self):
        return [child for child in self.top_level_dataset.getchildren()
                if self.tag_base_name_is(child, "dataset")]

    @cached_property
    def dataset_id(self):
        return self.top_level_dataset.attrib["ID"]

    def insert_viewer_metadata(self):
        mt = self.new_element("metadata", inherited="true")
        self.new_child(mt, "serviceName", "all")
        self.new_child(mt, "authority", "pml.ac.uk:")
        self.new_child(mt, "dataType", "Grid")
        self.new_child(mt, "property", name="viewer",
                       value="http://jasmin.eofrom.space/?wms_url={WMS},GISportal Viewer")
        self.insert_element_before_similar(self.top_level_dataset, mt)

    def insert_wms_service(self,
                           base="/thredds/wms/"):
        """
        Add a new 'service' element.
        """
        sv = self.new_element("service",
                              name="wms",
                              serviceType="WMS",
                              base=base)
        self.root.insert(0, sv)

    def insert_wcs_service(self,
                           base="/thredds/wcs/"):
        """
        Add a new 'service' element.
        """
        sv = self.new_element("service",
                              name="wcs",
                              serviceType="WCS",
                              base=base)
        self.root.insert(0, sv)

    def write(self, filename, agg_dir):
        """
        Write this catalog to 'filename', and save aggregations in 'agg_dir'
        """
        super().write(filename)

        for (fn, subdir), agg in self.aggregations.items():
            abs_subdir = os.path.join(agg_dir, subdir)
            if not os.path.isdir(abs_subdir):
                os.makedirs(abs_subdir)

            agg.write(os.path.join(abs_subdir, fn))

    def strip_restrict_access(self):
        """
        remove restrictAccess from the top-level dataset tag
        """
        att_name = "restrictAccess"
        att_dict = self.top_level_dataset.attrib
        if att_name in att_dict:
            del att_dict[att_name]

    def path_on_disk(self, fileserver_url):
        """
        translate the urlPath property of the dataset element into a
        path on disk, by translating the relevant THREDDS root.
        """
        pos = fileserver_url.index("/")
        ds_root = fileserver_url[:pos]
        path = os.path.join(self.thredds_roots[ds_root],
                            fileserver_url[pos + 1:])
        path = os.path.normpath(path)
        return path

    def netcdf_files(self):
        return [self.path_on_disk(element.attrib["urlPath"])
                for element in self.second_level_datasets
                if element.attrib["serviceName"] == "HTTPServer"]

    def add_aggregations(self, add_wms=False):
        """
        Create one or more NcML aggregations from netCDF files in this dataset,
        and link to them in the catalog.

        The NcML documents are saved in self.aggregations
        """
        # Get directory to store aggregation in by splitting file name into
        # its facets and having a subdirectory for each component.
        components = os.path.basename(self.in_filename).split(".")
        if components:
            if components[0] == "esacci":
                components.pop(0)
            if components[-1] == "xml":
                components.pop(-1)
        sub_dir = os.path.join(*components)

        services = ["OpenDAPServer"]
        if add_wms:
            services.append("wms")
            if self.do_wcs:
                services.append("wcs")

        # Partition file list into groups that can be aggregated
        groups = partition_files(self.netcdf_files())
        for common_name, filenames in groups.items():
            dsid = None
            if len(groups) > 1:
                # common_name is path on disk of files in aggregation, with
                # differences (i.e. dates) replaced with 'x'. Remove prefix
                # and replace '/' to create an ID
                dsid = common_name.replace("/neodc/esacci/", "").replace("/", ".")
            else:
                dsid = self.dataset_id

            print("Creating aggregation '{}'".format(dsid))
            try:
                agg_element = create_aggregation(filenames)
            except AggregationError:
                print("WARNING: Failed to create aggregation", file=sys.stderr)
                return

            ds = self.new_element("dataset", name=dsid, ID=dsid, urlPath=dsid)

            for service_name in services:
                self.new_child(ds, "access", serviceName=service_name, urlPath=dsid)

            agg_xml = ThreddsXMLBase()
            agg_xml.set_root(agg_element)

            agg_basename = "{}.ncml".format(dsid)
            self.aggregations[(agg_basename, sub_dir)] = agg_xml

            # Create a 'netcdf' element in the catalog that points to the file containing the
            # aggregation
            agg_full_path = os.path.join(self.aggregations_dir, sub_dir, agg_basename)
            self.new_child(ds, "netcdf", location=agg_full_path,
                           xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2")
            self.top_level_dataset.append(ds)

    def all_changes(self, create_aggs=False, add_wms=False):
        self.strip_restrict_access()

        if create_aggs:
            self.add_aggregations(add_wms=add_wms)

        # Add WMS/WCS services
        if add_wms:
            self.insert_viewer_metadata()
            self.insert_wms_service()
            if self.do_wcs:
                self.insert_wcs_service()


class ProcessBatch(object):
    def __init__(self, arg_list):
        """
        Parse command line arguments using argparse and store in self.args
        """
        parser = argparse.ArgumentParser(description=__doc__)

        parser.add_argument(
            "catalogs",
            nargs="+",
            help="Path to input catalog(s)"
        )

        parser.add_argument(
            "-a", "--aggregate",
            dest="aggregate",
            action="store_true",
            help="Produce NcML aggregations and add OPeNDAP endpoints"
        )
        parser.add_argument(
            "-w", "--wms",
            dest="wms",
            action="store_true",
            help="Add WMS and WCS endpoint for aggregations"
        )
        parser.add_argument(
            "-o", "--output-dir",
            dest="output_dir",
            default="output_catalogs",
            help="Directory to write modified catalog(s) to [default: %(default)s]"
        )
        parser.add_argument(
            "-n", "--ncml-dir",
            dest="ncml_dir",
            default="aggregations",
            help="Directory to write NcML aggregations to if using --aggregate "
                 "[default: %(default)s]"
        )

        self.args = parser.parse_args(arg_list)

        if self.args.wms and not self.args.aggregate:
            parser.error("Cannot add WMS/WCS aggregations without --aggregate")

    def do_all(self):
        for fn in self.args.catalogs:
            try:
                print(fn)
                self.process_file(fn)
                print("")
            except:
                print("WARNING: %s failed, exception follows\n" % fn)
                print("==============")
                traceback.print_exc()
                print("==============")

    def process_file(self, in_file):
        basename = os.path.basename(in_file)
        out_file = os.path.join(self.args.output_dir, basename)

        tx = ThreddsXMLDataset(do_wcs=True)
        tx.read(in_file)
        tx.all_changes(create_aggs=self.args.aggregate, add_wms=self.args.wms)
        tx.write(out_file, agg_dir=self.args.ncml_dir)


if __name__ == "__main__":
    pb = ProcessBatch(sys.argv[1:])
    pb.do_all()
