#!/usr/bin/env python3

"""
Script to modify THREDDS xml files to create NcML aggregations and
make these accessible through WMS/WCS.

For default filenames used, see default args to ProcessBatch.__init__()
"""

import sys
import os
import traceback
import xml.etree.cElementTree as ET

from cached_property import cached_property

from partition_files import partition_files
from aggregate import create_aggregation


class ThreddsXMLBase(object):
    """
    Base class re generic stuff we want to do to THREDDS XML files
    """
    def __init__(self,
                  ns = "http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0",
                  encoding = 'UTF-8',
                  xlink = "http://www.w3.org/1999/xlink"):
        self.ns = ns
        self.encoding = encoding
        self.xlink = xlink

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
        return tag_full_name[tag_full_name.index("}") + 1 :]

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
            element.append(new_child)

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

    def delete_all_children_called(self, parent, tagname):
        for child in parent.getchildren():
            if self.tag_base_name_is(child, tagname):
                parent.remove(child)


class ThreddsXMLTopLevel(ThreddsXMLBase):
    """
    A class for manipulating the top-level THREDDS catalog
    """

    def add_ref(self, href, name, title=None):
        if not title:
            title = name
        atts = {'xlink:title' : title,
                'xlink:href' : href,
                'name' : name}
        self.new_child(self.root, "catalogRef", **atts)


class ThreddsXMLDataset(ThreddsXMLBase):
    """
    An intermediate class re THREDDS catalogs that describe datasets -
    methods in common to what we want to do on the data node
    and on the WMS server

       AND

    A class for processing THREDDS XML files and tweaking them to add WMS tags.

    """

    def __init__(self,
                 thredds_roots = {},
                 check_filenames_similar = False,
                 valid_file_pattern = None,
                 check_vars_in_all_files = False,
                 do_wcs = False,
                 aggregations_dir = "/usr/local/aggregations",
                 **kwargs):
        """
        aggregations_dir is the directory in which NcML files will be placed on the
        server (used to reference aggregations from the THREDDS catalog)
        """

        super().__init__(**kwargs)

        self.thredds_roots = thredds_roots
        self.thredds_roots.setdefault("esg_esacci", "/neodc/esacci")
        self.do_wcs = do_wcs
        self.aggregations_dir = aggregations_dir

        # For each NcML file for an aggregation of datasets, map
        # (file basename, subdir) -> ThreddsXMLBase, where subdir is the subdirectory
        # of self.aggregations_dir in which the file will live
        self.aggregations = {}

        # options related to quirks in the data
        self.check_filenames_similar = check_filenames_similar
        self.check_vars_in_all_files = check_vars_in_all_files
        self.valid_file_pattern = valid_file_pattern


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
                              name = "wms",
                              serviceType="WMS",
                              base=base)
        #self.insert_element_before_similar(self.root, sv)
        self.root.insert(0, sv)

    def insert_wcs_service(self,
                           base="/thredds/wcs/"):
        """
        Add a new 'service' element.
        """
        sv = self.new_element("service",
                              name = "wcs",
                              serviceType="WCS",
                              base=base)
        #self.insert_element_before_similar(self.root, sv)
        self.root.insert(0, sv)

    def write(self, filename, agg_dir):
        """
        Write this catalog to 'filename', and save aggregations in 'agg_dir'
        """
        super().write(filename)

        for (filename, subdir), agg in self.aggregations.items():
            abs_subdir = os.path.join(agg_dir, subdir)
            if not os.path.isdir(abs_subdir):
                os.makedirs(abs_subdir)

            agg.write(os.path.join(abs_subdir, filename))

    def strip_restrictAccess(self):
        """
        remove restrictAccess from the top-level dataset tag
        """
        att_name = "restrictAccess"
        att_dict = self.top_level_dataset.attrib
        if att_name in att_dict:
            del(att_dict[att_name])

    def path_on_disk(self, fileserver_url):
        """
        translate the urlPath property of the dataset element into a
        path on disk, by translating the relevant THREDDS root.
        """
        pos = fileserver_url.index("/")
        ds_root = fileserver_url[:pos]
        path = os.path.join(self.thredds_roots[ds_root],
                            fileserver_url[pos + 1 :])
        path = os.path.normpath(path)
        return path

    def netcdf_files(self):
        return [self.path_on_disk(element.attrib['urlPath'])
                for element in self.second_level_datasets
                if element.attrib["serviceName"] == "HTTPServer"]

    def add_aggregations(self):
        groups = partition_files(self.netcdf_files())
        if len(groups) > 1:
            raise NotImplementedError("Multiple aggregations per dataset not yet supported")
        filenames = groups[0]

        dsid = self.dataset_id
        ds = self.new_element("dataset", name=dsid, ID=dsid, urlPath=dsid)

        services = ["wms", "OpenDAPServer"]
        if self.do_wcs:
            services.append("wcs")

        for service_name in services:
            self.new_child(ds, "access", serviceName=service_name, urlPath=dsid)

        agg_xml = ThreddsXMLBase()
        agg_xml.set_root(create_aggregation(filenames))

        # Get directory to store aggregation in by splitting dataset ID into
        # its facets and having a subdirectory for each component.
        components = os.path.basename(self.in_filename).split(".")
        if len(components) > 0:
            if components[0] == "esacci":
                components.pop(0)
            if components[-1] == "xml":
                components.pop(-1)

        sub_dir = os.path.join(*components)
        agg_basename = "%s.ncml" % dsid
        self.aggregations[(agg_basename, sub_dir)] = agg_xml

        # Create a 'netcdf' element in the catalog that points to the file containing the
        # aggregation
        agg_full_path = os.path.join(self.aggregations_dir, sub_dir, agg_basename)
        catalog_ncml = self.new_child(ds, "netcdf", location=agg_full_path,
                                      xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2")
        self.top_level_dataset.append(ds)

    def all_changes(self):
        self.insert_viewer_metadata()
        self.strip_restrictAccess()

        # Add WMS/WCS services
        self.insert_wms_service()
        if self.do_wcs:
            self.insert_wcs_service()

        self.add_aggregations()

class ProcessBatch(object):
    def __init__(self, args, indir='input_catalogs', outdir='output_catalogs',
                 agg_outdir='aggregations',
                 cat_in = 'catalog_in.xml',
                 cat_out = 'catalog.xml'):
        self.indir = indir
        self.outdir = outdir
        self.agg_outdir = agg_outdir
        self.cat_in = cat_in
        self.cat_out = os.path.join(outdir, cat_out)
        self.parse_args(args)

    def do_all(self):
        for fn in self.basenames:
            try:
                print(fn)
                self.process_file(fn)
                print("")
            except:
                print("WARNING: %s failed, exception follows\n" % fn)
                print("==============")
                traceback.print_exc()
                print("==============")
        tx_cat = ThreddsXMLTopLevel()
        tx_cat.read(self.cat_in)
        for fn in self.get_all_basenames(self.outdir):
            title = fn
            assert fn.endswith(".xml")
            name = fn[:-4]
            tx_cat.add_ref(os.path.join("1", title), name)
        tx_cat.write(self.cat_out)

    def get_kwargs(self, basename):
        """
        return argument dictionary to deal with special cases where files are heterogeneous
        """
        if basename.startswith("esacci.OC."):
            dirs = {'day': 'daily',
                    'mon': 'monthly',
                    'yr': 'annual',
                    '8-days': '8day',
                    '5-days': '5day'}
            freq = basename.split(".")[2]
            pattern = 'geographic.*' + dirs[freq]
            return {'valid_file_pattern' : pattern}
        elif basename == 'esacci.GHG.day.L2.CH4.TANSO-FTS.GOSAT.GOSAT.v2-3-6.r1.v20160427.xml':
            return {'valid_file_pattern' : 'SRPR'}
        elif basename.startswith("esacci.SEAICE.") and not (".NH." in basename or ".SH." in basename):
            return {'valid_file_pattern' : 'NorthernHemisphere'}
        else:
            return {}

    def process_file(self, basename):
        in_file = os.path.join(self.indir, basename)
        out_file = os.path.join(self.outdir, basename)

        kwargs = self.get_kwargs(basename)

        tx = ThreddsXMLDataset(check_filenames_similar = True,
                               do_wcs = True,
                               **kwargs)
        tx.read(in_file)
        tx.all_changes()
        tx.write(out_file, agg_dir=self.agg_outdir)

    def usage(self):
        prog = sys.argv[0]
        print("""Usage:

   %s -a    - add WMS tags to all files found in %s

   %s file1 [file2...]  - add WMS tags to specific files (base name only; files are assumed to be
                         in %s and any directory part will be ignored
""" % (prog, self.indir, prog, self.indir))

    def parse_args(self, args):
        if not args:
            self.usage()
            raise ValueError("bad command line arguments")

        if args == ['-a']:
            self.basenames = self.get_all_basenames()
        else:
            self.basenames = map(os.path.basename, args)

    def get_basenames(self):
        return self.basenames

    def get_all_basenames(self, dn=None):
        if dn == None:
            dn = self.indir
        return [fn for fn in os.listdir(dn) if
                fn.startswith("esacci") and fn.endswith(".xml")]


if __name__ == '__main__':
    pb = ProcessBatch(sys.argv[1:])
    pb.do_all()
