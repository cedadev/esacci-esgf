import sys
import os
import xml.etree.cElementTree as ET

from cached_property import cached_property

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

    def delete_all_children_called(self, parent, tagname):
        for child in parent.getchildren():
            if self.tag_base_name_is(child, tagname):
                parent.remove(child)

class ThreddsXMLDatasetBase(ThreddsXMLBase):
    """
    An intermediate class re THREDDS catalogs that describe datasets -
    methods in common to what we want to do on the data node
    and on the WMS server
    """

    def __init__(self, **kwargs):
        ThreddsXMLBase.__init__(self, **kwargs)

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
        "Add a new 'service' element."
        sv = self.new_element("service",
                              name = "wms",
                              serviceType="WMS",
                              base=base)
        #self.insert_element_before_similar(self.root, sv)
        self.root.insert(0, sv)

    def insert_wcs_service(self,
                           base="/thredds/wcs/"):
        "Add a new 'service' element."
        sv = self.new_element("service",
                              name = "wcs",
                              serviceType="WCS",
                              base=base)
        #self.insert_element_before_similar(self.root, sv)
        self.root.insert(0, sv)


class ProcessBatchBase(object):

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

