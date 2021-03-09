"""Example of using the 'ReifySub' transformation.
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

# Create a new (child) relation computed from a subset of column(s) of the source relation
with model.begin(dry_run=__dry_run__) as session:
    dataset = model.schemas['isa'].tables['dataset']
    session.create_table_as(
        'isa', 'dataset_study_designs',
        dataset.reify_sub(dataset.columns['study_design'])
    )
