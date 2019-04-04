#!/usr/bin/env python
"""
Example of using the 'shred' transformation.

You will need a copy of 'zfs.owl' and specify its location at 'CHISEL_EXAMPLE_ZFS_OWL'.
"""
import os
import chisel

__dry_run__ = os.getenv('CHISEL_EXAMPLE_DRY_RUN', True)
__catalog_url__ = os.getenv('CHISEL_EXAMPLE_CATALOG_URL', 'http://localhost/ermrest/catalog/1')

zfs_filename = os.getenv('CHISEL_EXAMPLE_ZFS_OWL')
if not zfs_filename:
    print("ERROR: env var 'CHISEL_EXAMPLE_ZFS_OWL' not defined")
    exit(1)

catalog = chisel.connect(__catalog_url__)
print('CONNECTED')

# SPARQL expression to extract the id (i.e., short identifier) and name (i.e., preferred readable name) from the graph
sparql_class_and_props = """
SELECT DISTINCT ?id (?label AS ?name)
WHERE {
  ?s oboInOwl:id ?id .
  ?s rdfs:label ?label .
}"""

# Create a new relation computed from the shredded graph
with catalog.evolve(dry_run=__dry_run__):
    catalog['vocab']['zebrafish_stage_terms'] = chisel.shred(zfs_filename, sparql_class_and_props)
print('DONE')
