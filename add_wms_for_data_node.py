#!/usr/bin/env python2.7

"""
Script to create THREDDS xml files for use on the data node
based on XML files copied from the data node, with the 
addition of external links for the WMS service.

For default filenames used, see default args to ProcessBatch.__init__()
"""

import os

from addwms_base import ThreddsXMLDatasetBase

class ThreddsXMLDatasetOnDataNode(ThreddsXMLDatasetBase):
    """
    A class for processing THREDDS XML files and tweaking them to add WMS tags
    that point to external WMS server
    """
    def __init__(self,
                 wms_url_base = 'https://cci-odp-data.cems.rl.ac.uk/thredds/wms',
                 **kwargs):
        ThreddsXMLDatasetBase.__init__(self, **kwargs)
        self.wms_url_base = wms_url_base

    def insert_access_links(self):        
        dsid = self.dataset_id
        for ds in [self.top_level_dataset] + self.second_level_datasets:
            access = self.new_element("access", serviceName="wms", urlPath=dsid)
            self.insert_element_before_similar(ds, access)

    def all_changes(self):
        self.insert_viewer_metadata()
        self.insert_wms_service(base = self.wms_url_base)
        self.insert_access_links()

    
class ProcessBatch(object):
    def __init__(self, indir='input_catalogs', outdir='output_catalogs_for_data_node'):
        self.indir = indir
        self.outdir = outdir

    def do_all(self):
        for fn in self.get_all_basenames():
            print fn
            self.process_file(fn)
            print

    def get_all_basenames(self):
        return [fn for fn in os.listdir(self.indir) if fn.endswith(".xml")]

    def process_file(self, basename):
        in_file = os.path.join(self.indir, basename)
        out_file = os.path.join(self.outdir, basename)
        tx = ThreddsXMLDatasetOnDataNode()
        tx.read(in_file)
        tx.all_changes()
        tx.write(out_file)
    
if __name__ == '__main__':
    pb = ProcessBatch()
    pb.do_all()
