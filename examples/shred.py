#!/usr/bin/env python
"""
Example of using the 'shred' transformation.

You will need a copy of 'zfs.owl' and specify its location at 'CHISEL_EXAMPLE_ZFS_OWL'.
"""
import os
import chisel

__dry_run__ = os.getenv('CHISEL_EXAMPLE_DRY_RUN', True)
__catalog_url__ = os.getenv('CHISEL_EXAMPLE_CATALOG_URL', 'http://localhost/ermrest/catalog/1')

catalog = chisel.connect(__catalog_url__)
print('CONNECTED')

# For this demonstration, lookup and delete the target table if it already exists
if 'zebrafish_stage_terms' in catalog.s['vocab'].tables:
    catalog.s['vocab'].t['zebrafish_stage_terms'].delete(catalog.catalog, catalog.s['vocab'])  # committed immediately

# SPARQL expression to extract the id (i.e., short identifier) and name (i.e., preferred readable name) from the graph
sparql_class_and_props = """
SELECT DISTINCT ?id (?label AS ?name)
WHERE {
  ?s oboInOwl:id ?id .
  ?s rdfs:label ?label .
}"""

# Create a new relation computed from the shredded graph
catalog.s['vocab'].t['zebrafish_stage_terms'] = chisel.shred(os.getenv('CHISEL_EXAMPLE_ZFS_OWL'), sparql_class_and_props)
catalog.commit(dry_run=__dry_run__)
print('DONE')
