#!/usr/bin/env python3
import sys
import os

import pysolr

"""
Script that modifies the WMS, WCS and OpenDAP links in Solr.
"""

DEFAULT_SOLR_NODE = 'http://esgf-index1.ceda.ac.uk:8984'


def solr_update_doc(s, doc, func):
    """
    Given a document from Solr, call the specified function
    (which should return a dictionary of field updates), and then update
    Solr if the dictionary is not empty.
    """
    id = doc['id']

    updated = func(doc)

    # if it is actually any different from before, then
    # update it in Solr
    if updated:
        print('updating:', id)
        s.add([doc])
    else:
        print('unchanged:', id)


def query_all(s, query="*.*", chunk=1000):
    """
    Get a list of documents from Solr all into memory, querying in
    chunks of 1000.  Each one will be a dictionary.
    """
    results = []
    start = 0
    while True:
        print("querying from %s" % start)
        resp = s.search(query, start=start, rows=chunk)
        these_results = resp.docs
        if not these_results:
            print("%s results found" % len(results))
            return results
        results.extend(these_results)
        start += chunk


def update_urls(doc):
    """
    Update the WMS and/or WCS URLs in the "url" list in the document so as to add the
    query parameters needed for GetCapabilities if they are not already there.
    Update any OpenDAP URLs to remove the .html suffix.

    Works equally with a dataset document and a file document.

    Returns boolean to say if updated
    """
    urls = doc['url']
    changed = False
    for i, url_with_svc in enumerate(urls):
        bits = url_with_svc.split('|')
        if bits[2] == 'WMS' and '?' not in bits[0]:
            bits[0] += '?service=WMS&version=1.3.0&request=GetCapabilities'
            changed = True
        # WCS similar but note different version
        if bits[2] == 'WCS' and '?' not in bits[0]:
            bits[0] += '?service=WCS&version=1.0.0&request=GetCapabilities'
            changed = True
        # If OpenDAP then remove .html suffix
        if bits[2] == 'OPENDAP' and bits[0].endswith('.html'):
            bits[0] = bits[0][:-5]
            changed = True

        urls[i] = '|'.join(bits)

    return changed


def usage():
    prog = os.path.basename(sys.argv[0])
    print("""Usage: %s [SOLR NODE]

Modify Solr documents to correct WMS, WCS and OpenDAP endpoints for
aggregate datasets

SOLR NODE defaults to %s""" % (prog, DEFAULT_SOLR_NODE))


if __name__ == '__main__':

    solr_node = DEFAULT_SOLR_NODE
    if len(sys.argv) > 1:
        if sys.argv[1] in ("-h", "--help"):
            usage()
            sys.exit(0)
        else:
            solr_node = sys.argv[1]

    s = pysolr.Solr("%s/solr/datasets" % solr_node)
    dsets = query_all(s, query='esacci')
    for ds in dsets:
        solr_update_doc(s, ds, update_urls)
