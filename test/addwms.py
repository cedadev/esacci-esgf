#!/usr/bin/env python2.7

import re
import os
import xml.etree.cElementTree as ET
import netCDF4
from itertools import takewhile

from cached_property import cached_property


class NcFile(object):

    def __init__(self, filename):
        self.ds = netCDF4.Dataset(filename)

    def multidim_vars(self):
        return [name for name, var in self.ds.variables.iteritems()
                if len(var.shape) >= 2]

class ThreddsXML(object):
    """
    A class for processing THREDDS XML files and tweaking them to add WMS tags.

    Instantiate with check_vars_in_all_files=True to open all the netCDF files and
    add tags for variables that appear in any file (as any variables encountered on 
    scanning but not formally listed will be served with wrong time information).
    Otherwise it will only scan the first file for variable names.
    """

    def __init__(self, 
                 ns = "http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0",
                 encoding = 'UTF-8',
                 xlink = "http://www.w3.org/1999/xlink",
                 thredds_roots = {},

                 do_file_filter = False,
                 valid_file_index = 0,
                 check_vars_in_all_files = False):
        self.ns = ns
        self.encoding = encoding
        self.xlink = xlink
        self.thredds_roots = thredds_roots
        self.thredds_roots.setdefault("esg_esacci", "/neodc")

        # options related to quirks in the data
        self.do_file_filter = do_file_filter
        self.check_vars_in_all_files = check_vars_in_all_files
        self.valid_file_index = valid_file_index

    def read(self, filename):
        ET.register_namespace("", self.ns)
        self.tree = ET.ElementTree()
        self.tree.parse(filename)
        self.root = self.tree.getroot()
        self.root.set("xmlns:xlink", self.xlink)

    def write(self, filename):
        tmpfile = filename + ".tmp"
        self.tree.write(tmpfile, encoding=self.encoding)
        os.system("xmllint --format %s > %s" % (tmpfile, filename))
        os.remove(tmpfile)

    def tag_full_name(self, tag_base_name):
        return "{%s}%s" % (self.ns, tag_base_name)

    def tag_base_name(self, tag_full_name):
        return tag_full_name[tag_full_name.index("}") + 1 :]

    def tag_base_name_is(self, element, tag_name):
        return self.tag_base_name(element.tag) == tag_name

    def insert_element_before_similar(self, parent, new_child):
        "Add a child element, if possible putting it before another child with the same tag"
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
        "As new_element, but add result as child of specified parent element"
        child = self.new_element(*args, **kwargs)
        parent.append(child)
        return child

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

    def delete_all_children_called(self, parent, tagname):
        for child in parent.getchildren():
            if self.tag_base_name_is(child, tagname):
                parent.remove(child)

    def insert_viewer_metadata(self):
        mt = self.new_element("metadata", inherited="true")
        self.new_child(mt, "serviceName", "all")
        self.new_child(mt, "authority", "pml.ac.uk:")
        self.new_child(mt, "dataType", "Grid")
        self.new_child(mt, "property", name="viewer", 
                       value="http://jasmin.eofrom.space/?wms_url={WMS},GISportal Viewer")
        self.insert_element_before_similar(self.top_level_dataset, mt)
        
    def insert_wms_service(self):
        "Add a new 'service' element."
        sv = self.new_element("service",
                              name = "wms",
                              serviceType="WMS",
                              base="/thredds/wms/")
        #self.insert_element_before_similar(self.root, sv)
        self.root.insert(0, sv)

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
        return os.path.join(self.thredds_roots[ds_root],
                            fileserver_url[pos + 1 :])

    @cached_property
    def netcdf_files(self):
        files = [self.path_on_disk(element.attrib['urlPath'])
                 for element in self.second_level_datasets
                 if element.attrib["serviceName"] == "HTTPServer"]
        if self.do_file_filter:
            files = self.filter_files(files)
        return files

    def filter_files(self, files):
        """
        Subset a file list to be only the ones whose pathnames differ from the example path 
        only by the substitution of digits.  The example filename is the first filename, unless 
        a different list index for an example valid file has been passed to the constructor.
        """
        recomp = re.compile("[0-9]")
        do_subs = lambda path: recomp.sub("0", path)
        example_path = files[self.valid_file_index]
        substituted_example = do_subs(example_path)
        matches = lambda path: (do_subs(path) == substituted_example)
        return filter(matches, files)

    def netcdf_variables_for_file(self, path):
        return set(NcFile(path).multidim_vars())

    @cached_property
    def netcdf_variables(self):
        files = self.netcdf_files
        varnames = self.netcdf_variables_for_file(files[0])
        if self.check_vars_in_all_files:
            for path in files[1:]:
                varnames = varnames.union(self.netcdf_variables_for_file(path))
                print len(varnames)
        aslist = list(varnames)
        aslist.sort()
        return aslist

    # https://www.rosettacode.org/wiki/Find_common_directory_path#Python
    def allnamesequal(self, name):
	return all(n==name[0] for n in name[1:]) 
    def commonprefix(self, paths, sep='/'):
	bydirectorylevels = zip(*[p.split(sep) for p in paths])
	return sep.join(x[0] for x in takewhile(self.allnamesequal, bydirectorylevels))

    re_date = re.compile("(.*?[^0-9])[12][0-9]{3}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])")
    def get_date_format_mark_1(self, file_path):
        base = os.path.basename(file_path)
        m = self.re_date.match(base)
        if not m:
            raise Exception("filename %s doesn't seem to contain a date" % file_path)
        return m.group(1) + "#yyyyMMdd"

    def get_date_format_mark(self, paths):
        all_date_formats = map(self.get_date_format_mark_1, paths)
        assert len(set(all_date_formats)) == 1  # if don't all give same string, need to refine
        return all_date_formats[0]
        
    def add_wms_ds(self):
        dsid = self.dataset_id
        ds = self.new_element("dataset", name=dsid, ID=dsid, urlPath=dsid)
        self.new_child(ds, "access", serviceName="wms", urlPath=dsid)
        nc = self.new_child(ds, "netcdf", xmlns="http://www.unidata.ucar.edu/namespaces/netcdf/ncml-2.2")
        agg = self.new_child(nc, "aggregation", dimName="time", type="joinNew")
        self.new_child(agg, "remove", name="time", type="variable")
        for varname in self.netcdf_variables:
            self.new_child(agg, "variableAgg", name=varname)

        common_dir = self.commonprefix(map(os.path.dirname, self.netcdf_files))
        self.new_child(agg, "scan", location=common_dir,
                       dateFormatMark=self.get_date_format_mark(self.netcdf_files))

        self.top_level_dataset.append(ds)            


    def all_changes(self):

        self.insert_viewer_metadata()
        self.strip_restrictAccess()

        # remove all services and just add the WMS one
        self.delete_all_children_called(self.root, "service")
        self.insert_wms_service()

        # ensure some cached_properties get evaluated before we delete elements
        self.netcdf_files
        self.netcdf_variables
        
        # remove all (2nd level) datasets and just add the WMS one
        self.delete_all_children_called(self.top_level_dataset, "dataset")
        self.add_wms_ds()

def main():
    tx = ThreddsXML(do_file_filter = True, valid_file_index=1)
    tx.read("input.xml")
    tx.all_changes()
    tx.write("output.xml")

if __name__ == '__main__':
    #tree = get_tree("input.xml")
    main()
