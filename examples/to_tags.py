"""Example of using the 'tagify' (a.k.a., 'to_tags()') transformation.
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

# Create a new relation computed from the atomized source relation
with model.begin(dry_run=__dry_run__) as session:
    session.create_table_as(
        'isa', 'enhancer_anatomical_structures',
        model.schemas['isa'].tables['enhancer'].columns['list_of_anatomical_structures'].to_tags(
            model.schemas['vocab'].tables['anatomy']
        )
    )
