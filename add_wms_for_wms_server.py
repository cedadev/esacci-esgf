#!/usr/bin/env python2.7

"""
Script to create THREDDS xml files for use on the WMS server
based on XML files copied from the data node, and also to produce 
a top-level THREDDS catalog for the WMS server based on a template
file with the addition of the links to per-dataset catalog files.

For default filenames used, see default args to ProcessBatch.__init__()
"""

import re
import os
import netCDF4
from itertools import takewhile
from cached_property import cached_property

from addwms_base import ThreddsXMLBase, ThreddsXMLDatasetBase

class NcFile(object):

    def __init__(self, filename):
        self.ds = netCDF4.Dataset(filename)

    def multidim_vars(self):
        return [name for name, var in self.ds.variables.iteritems()
                if len(var.shape) >= 2]

class ThreddsXMLTopLevel(ThreddsXMLBase):
    """
    A class for manipulating the top-level THREDDS catalog
    (on the WMS server)
    """

    def add_ref(self, href, name, title=None):
        if not title:
            title = name
        atts = {'xlink:title' : title,
                'xlink:href' : href,
                'name' : name}
        self.new_child(self.root, "catalogRef", **atts)


class ThreddsXMLDatasetOnWMSServer(ThreddsXMLDatasetBase):
    """
    A class for processing THREDDS XML files and tweaking them to add WMS tags.

    Instantiate with check_vars_in_all_files=True to open all the netCDF files and
    add tags for variables that appear in any file (as any variables encountered on 
    scanning but not formally listed will be served with wrong time information).
    Otherwise it will only scan the first file for variable names.
    """

    def __init__(self, 
                 thredds_roots = {},
                 check_filenames_similar = False,
                 valid_file_pattern = None,
                 check_vars_in_all_files = False,
                 do_wcs = False,
                 **kwargs):

        ThreddsXMLDatasetBase.__init__(self, **kwargs)

        self.thredds_roots = thredds_roots
        self.thredds_roots.setdefault("esg_esacci", "/neodc")
        self.do_wcs = do_wcs

        # options related to quirks in the data
        self.check_filenames_similar = check_filenames_similar
        self.check_vars_in_all_files = check_vars_in_all_files
        self.valid_file_pattern = valid_file_pattern

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
        # hack to avoid path inconsistency
        m = re.match("^(/neodc/esacci/[^/]+)/", path)
        if m:
            path = os.readlink(m.group(1)) + path[m.end(1) :]
        return path

    @cached_property
    def netcdf_files(self):
        files = [self.path_on_disk(element.attrib['urlPath'])
                 for element in self.second_level_datasets
                 if element.attrib["serviceName"] == "HTTPServer"]

        if self.valid_file_pattern:
            files = self.apply_filter_verbose(self.filter_files_by_pattern,
                                              files, 
                                              self.valid_file_pattern)            
        if self.check_filenames_similar:
            files = self.apply_filter_verbose(self.filter_files_by_similarity,
                                              files)
        return files

    def apply_filter_verbose(self, func, orig_list, *args):
        filtered_list = func(orig_list, *args)
        if len(filtered_list) != len(orig_list):
            print "After calling %s" % func.__name__
            print "%s out of %s files used" % (len(filtered_list), len(orig_list))
            if filtered_list:
                print "Example file used:     %s" % filtered_list[0]
            print "Example file not used: %s" % (set(orig_list) - set(filtered_list)).pop()
        return filtered_list

    def filter_files_by_pattern(self, files, pattern):
        print "applying filtering pattern: %s" % pattern
        return filter(re.compile(pattern).search, files)

    def filter_files_by_similarity(self, files):
        """
        Subset a file list to be only the ones whose pathnames differ from the first filename 
        only by the substitution of digits.
        """
        recomp = re.compile("[0-9]")
        do_subs = lambda path: recomp.sub("0", path)
        example_path = files[0]
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
                #print len(varnames)
        aslist = list(varnames)
        aslist.sort()
        return aslist

    # https://www.rosettacode.org/wiki/Find_common_directory_path#Python
    def allnamesequal(self, name):
	return all(n==name[0] for n in name[1:]) 
    def commonprefix(self, paths, sep='/'):
	bydirectorylevels = zip(*[p.split(sep) for p in paths])
	return sep.join(x[0] for x in takewhile(self.allnamesequal, bydirectorylevels))

    re_prefix = "(.*?[^0-9]|)"
    re_yyyy = "[12][0-9]{3}"
    re_mm = "(0[1-9]|1[0-2])"
    re_dd = "(0[1-9]|[12][0-9]|3[01])"
    recomp_date_ymd = re.compile(re_prefix + re_yyyy + re_mm + re_dd)
    recomp_date_ym = re.compile(re_prefix + re_yyyy + re_mm)
    recomp_date_y = re.compile(re_prefix + re_yyyy)

    def get_date_format_mark_1(self, file_path):
        base = os.path.basename(file_path)
        m = self.recomp_date_ymd.match(base)
        if m:
            return m.group(1) + "#yyyyMMdd"
        m = self.recomp_date_ym.match(base)
        if m:
            return m.group(1) + "#yyyyMM"
        m = self.recomp_date_y.match(base)
        if m:
            return m.group(1) + "#yyyy"
        raise Exception("filename %s doesn't seem to contain a date" % file_path)

    def get_date_format_mark(self, paths):
        all_date_formats = map(self.get_date_format_mark_1, paths)
        assert len(set(all_date_formats)) == 1  # if don't all give same string, need to refine
        return all_date_formats[0]

    def add_wms_ds(self):
        dsid = self.dataset_id
        ds = self.new_element("dataset", name=dsid, ID=dsid, urlPath=dsid)
        self.new_child(ds, "access", serviceName="wms", urlPath=dsid)
        if self.do_wcs:
            self.new_child(ds, "access", serviceName="wcs", urlPath=dsid)
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
        if self.do_wcs:
            self.insert_wcs_service()            

        # ensure some cached_properties get evaluated before we delete elements
        self.netcdf_files
        self.netcdf_variables
        #print self.netcdf_files
        
        # remove all (2nd level) datasets and just add the WMS one
        self.delete_all_children_called(self.top_level_dataset, "dataset")
        self.add_wms_ds()

class ProcessBatch(object):
    def __init__(self, indir='input_catalogs', outdir='output_catalogs',
                 cat_in = 'catalog_in.xml',
                 cat_out = 'catalog.xml'):
        self.indir = indir
        self.outdir = outdir
        self.cat_in = cat_in
        self.cat_out = os.path.join(outdir, cat_out)

    def do_all(self):
        tx_cat = ThreddsXMLTopLevel()
        tx_cat.read(self.cat_in)
        for fn in self.get_all_basenames():
            try:
                print fn
                self.process_file(fn)
                print
                title = fn
                assert fn.endswith(".xml")
                name = fn[:-4]
                tx_cat.add_ref(title, name)
            except:
                print "WARNING: %s failed\n" % fn
        tx_cat.write(self.cat_out)

    def get_all_basenames(self):
        return [fn for fn in os.listdir(self.indir) if fn.endswith(".xml")]

    def get_kwargs(self, basename):
        "return argument dictionary to deal with special cases where files are heterogeneous"
        if basename.startswith("esacci.OC."):
            dirs = {'day': 'daily',
                    'mon': 'monthly',
                    'yr': 'annual',
                    '8-days': '8day'}
            freq = basename.split(".")[2]
            pattern = 'geographic.*' + dirs[freq]
            return {'valid_file_pattern' : pattern}
        elif basename == 'esacci.GHG.day.L2.CH4.TANSO-FTS.GOSAT.GOSAT.v2-3-6.r1.v20160427.xml':
            return {'valid_file_pattern' : 'SRPR'}
        elif basename.startswith("esacci.SEAICE."):
            return {'valid_file_pattern' : 'NorthernHemisphere'}
        else:
            return {}

    def process_file(self, basename):
        in_file = os.path.join(self.indir, basename)
        out_file = os.path.join(self.outdir, basename)

        kwargs = self.get_kwargs(basename)

        tx = ThreddsXMLDatasetOnWMSServer(check_filenames_similar = True, 
                                          do_wcs = True,
                                          **kwargs)
        tx.read(in_file)
        tx.all_changes()
        tx.write(out_file)
    
if __name__ == '__main__':
    pb = ProcessBatch()
    pb.do_all()
