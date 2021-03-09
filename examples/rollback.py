"""Example of rolling back a model evolution session.
"""
import os
from deriva.core import DerivaServer
from deriva.chisel import Model

__dry_run__ = os.getenv('CHISEL_EXAMPLE_DRY_RUN', True)
__host__ = os.getenv('CHISEL_EXAMPLES_HOSTNAME', 'localhost')
__catalog_id__ = os.getenv('CHISEL_EXAMPLES_CATALOG', '1')

server = DerivaServer('https', __host__)
catalog = server.connect_ermrest(__catalog_id__)
model = Model.from_catalog(catalog)

# Begin a model evolution session and then rollback before exit
with model.begin(dry_run=__dry_run__) as session:
    session.create_table_as(
        'vocab', 'ethnicity',
        model.schemas['isa'].tables['clinical_assay'].columns['ethnicity'].to_domain(similarity_fn=None)
    )
    session.rollback()
