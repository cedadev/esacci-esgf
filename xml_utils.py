"""
Common functions for tasks to do with parsing XML documents
"""
import re
import xml.etree.cElementTree as ET


def find_by_tagname(xml_filename, tagname):
    """
    Recursively search an XML document and return elements with the given tag
    name
    """
    tree = ET.ElementTree()
    tree.parse(xml_filename)
    root = tree.getroot()

    # Regex to optionally match namspace in tag name
    tag_regex = re.compile("({[^}]+})?" + tagname)
    for el in root.iter():
        if re.fullmatch(tag_regex, el.tag):
            yield el
