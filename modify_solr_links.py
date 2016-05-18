#!/usr/bin/env python

import solr

import string

"""
Script that modifies the WMS links in Solr.
"""

def solr_update_doc(s, doc, func):
    """
    Given a document from Solr, call the specified function
    (which should edit the document in place and return a boolean to 
    say if anything was changed), and then update Solr if it is changed.
    """
    id = doc['id']

    # Set up whole new dictionary, based on the exsting one 
    # with relevant changes.
    # In the case of the elements of interest ('url' and 
    # 'access'), the values are lists of strings.
    new = doc.copy()

    changed = func(doc)

    # if it is actually any different from before, then
    # update it in Solr
    if changed:
        print 'updating:', id        

        # Solr doesn't like _version_ to be reused;
        # delete it and it will assign a fresh one.
        # (This is not the ESGF dataset version.)
        del(new['_version_'])
        s.delete(id)
        s.add(**new)
        s.commit()
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
        resp = s.select(query, start=start, rows=chunk)
        these_results = resp.results
        if not these_results:
            print "%s results found" % len(results)
            return results
        results.extend(these_results)
        start += chunk

  
def update_wms_url(doc):
    """
    Update the WMS URL in the "url" list in the document so as to add the 
    query parameters needed for GetCapabilities if they are not already there.
    Works equally with a dataset document and a file document.
    """
    urls = doc['url']
    changed = False
    #print '>>', urls
    for i, url_with_svc in enumerate(urls):
        bits = url_with_svc.split('|')
        if bits[2] == 'WMS' and '?' not in bits[0]:
            bits[0] += '?service=WMS&version=1.3.0&request=GetCapabilities'
            urls[i] = string.join(bits, '|')
            changed = True
    #print '<<', urls
    return changed
    
if __name__ == '__main__':
    s = solr.SolrConnection('http://esgf-test2.ceda.ac.uk:8984/solr/datasets')
    dsets = query_all(s, query='esacci')
    for ds in dsets:
        solr_update_doc(s, ds, update_wms_url)

    s = solr.SolrConnection('http://esgf-test2.ceda.ac.uk:8984/solr/files')
    files = query_all(s, query='esacci')
    for f in files:
        solr_update_doc(s, f, update_wms_url)
