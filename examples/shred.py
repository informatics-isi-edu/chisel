"""Example of using the 'shred' transformation.

You will need a copy of 'zfs.owl' and specify its location at 'CHISEL_EXAMPLES_ZFS_OWL'.
"""
import os
from deriva.core import DerivaServer
from deriva.chisel import Model
from deriva.chisel import shred

__dry_run__ = os.getenv('CHISEL_EXAMPLE_DRY_RUN', True)
__host__ = os.getenv('CHISEL_EXAMPLES_HOSTNAME', 'localhost')
__catalog_id__ = os.getenv('CHISEL_EXAMPLES_CATALOG', '1')

zfs_filename = os.getenv('CHISEL_EXAMPLES_ZFS_OWL')
if not zfs_filename:
    print("ERROR: env var 'CHISEL_EXAMPLES_ZFS_OWL' not defined")
    exit(1)

server = DerivaServer('https', __host__)
catalog = server.connect_ermrest(__catalog_id__)
model = Model.from_catalog(catalog)

# SPARQL expression to extract the id (i.e., short identifier) and name (i.e., preferred readable name) from the graph
sparql_class_and_props = """
SELECT DISTINCT ?id (?label AS ?name)
WHERE {
  ?s oboInOwl:id ?id .
  ?s rdfs:label ?label .
}"""

# Create a new relation computed from the shredded graph
with model.begin(dry_run=__dry_run__) as session:
    session.create_table_as(
        'vocab', 'zebrafish_stage_terms',
        shred(zfs_filename, sparql_class_and_props)
    )
