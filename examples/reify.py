"""Example of using the 'reify' transformation.
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

# Create a new relation by reifying a subset of attributes of an existing relation into a new relation
with model.begin(dry_run=__dry_run__) as session:
    enhancer = model.schemas['isa'].tables['enhancer']
    session.create_table_as(
        'isa', 'enhancer_assembly',
        enhancer.reify(
            # new key column(s) in new relation
            [
                enhancer.columns['id']
            ],
            # new non-key columns in new relation
            enhancer.columns['original_species_assembly'],
            enhancer.columns['original_species_chromosome'],
            enhancer.columns['original_species_start'],
            enhancer.columns['original_species_end']
        )
    )
