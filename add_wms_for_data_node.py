#!/usr/bin/env python2.7

"""
Script to create THREDDS xml files for use on the data node
based on XML files copied from the data node, with the 
addition of external links for the WMS service.

For default filenames used, see default args to ProcessBatch.__init__()
"""

import sys
import os

from addwms_base import ThreddsXMLDatasetBase, ProcessBatchBase

class ThreddsXMLDatasetOnDataNode(ThreddsXMLDatasetBase):
    """
    A class for processing THREDDS XML files and tweaking them to add WMS tags
    that point to external WMS server
    """
    def __init__(self, 
                 wms_url_base = 'https://cci-odp-data.cems.rl.ac.uk/thredds/wms/',
                 wcs_url_base = 'https://cci-odp-data.cems.rl.ac.uk/thredds/wcs/',
                 do_per_file_links = False,
                 do_wcs = False,
                 **kwargs):
        ThreddsXMLDatasetBase.__init__(self, **kwargs)
        self.wms_url_base = wms_url_base
        self.wcs_url_base = wcs_url_base
        self.do_wcs = do_wcs
        self.do_per_file_links = do_per_file_links

    def insert_access_links(self):        
        dsid = self.dataset_id
        dsets = [self.top_level_dataset]
        if self.do_per_file_links:
            dsets += self.second_level_datasets
        for ds in dsets:
            access = self.new_element("access", serviceName="wms", urlPath=dsid)
            self.insert_element_before_similar(ds, access)
            if self.do_wcs:
                access = self.new_element("access", serviceName="wcs", urlPath=dsid)
                self.insert_element_before_similar(ds, access)

    def all_changes(self):
        self.insert_viewer_metadata()
        self.insert_wms_service(base = self.wms_url_base)
        if self.do_wcs:
            self.insert_wcs_service(base = self.wcs_url_base)
        self.insert_access_links()

    
class ProcessBatch(ProcessBatchBase):
    def __init__(self, args, indir='input_catalogs', outdir='output_catalogs_for_data_node'):
        self.indir = indir
        self.outdir = outdir
        self.parse_args(args)

    def do_all(self):
        for fn in self.basenames:
            print fn
            self.process_file(fn)
            print

    def process_file(self, basename):
        in_file = os.path.join(self.indir, basename)
        out_file = os.path.join(self.outdir, basename)
        tx = ThreddsXMLDatasetOnDataNode(do_wcs = True)
        tx.read(in_file)
        tx.all_changes()
        tx.write(out_file)
    
if __name__ == '__main__':
    pb = ProcessBatch(sys.argv[1:])
    pb.do_all()
