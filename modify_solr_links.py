#!/usr/bin/env python

import pysolr

import string

"""
Script that modifies the WMS links in Solr.
"""

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
        print 'updating:', id
        s.add([doc])
    else:
        print 'unchanged:', id

def query_all(s, query="*.*", chunk=1000):
    """
    Get a list of documents from Solr all into memory, querying in
    chunks of 1000.  Each one will be a dictionary.
    """
    results = []
    start = 0
    while True:
        print "querying from %s" % start
        resp = s.search(query, start=start, rows=chunk)
        these_results = resp.docs
        if not these_results:
            print "%s results found" % len(results)
            return results
        results.extend(these_results)
        start += chunk


def update_wms_and_wcs_urls(doc):
    """
    Update the WMS and/or WCS URLs in the "url" list in the document so as to add the
    query parameters needed for GetCapabilities if they are not already there.
    Works equally with a dataset document and a file document.

    Returns boolean to say if updated
    """
    urls = doc['url']
    changed = False
    for i, url_with_svc in enumerate(urls):
        bits = url_with_svc.split('|')
        if bits[2] == 'WMS' and '?' not in bits[0]:
            bits[0] += '?service=WMS&version=1.3.0&request=GetCapabilities'
            urls[i] = string.join(bits, '|')
            changed = True
        # WCS similar but note different version
        if bits[2] == 'WCS' and '?' not in bits[0]:
            bits[0] += '?service=WCS&version=1.0.0&request=GetCapabilities'
            urls[i] = string.join(bits, '|')
            changed = True
    return changed

if __name__ == '__main__':
    s = pysolr.Solr('http://esgf-index1.ceda.ac.uk:8984/solr/datasets')
    dsets = query_all(s, query='esacci')
    for ds in dsets:
        solr_update_doc(s, ds, update_wms_and_wcs_urls)
