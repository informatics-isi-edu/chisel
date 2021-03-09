"""Example of using the 'scan' operator.
"""
import os
from deriva.core import DerivaServer
from deriva.chisel import Model
from deriva.chisel.catalog.semistructured import csv_reader

__dry_run__ = os.getenv('CHISEL_EXAMPLE_DRY_RUN', True)
__host__ = os.getenv('CHISEL_EXAMPLES_HOSTNAME', 'localhost')
__catalog_id__ = os.getenv('CHISEL_EXAMPLES_CATALOG', '1')

server = DerivaServer('https', __host__)
catalog = server.connect_ermrest(__catalog_id__)
model = Model.from_catalog(catalog)

# Create a new relation computed from the a scan of the csv file
with model.begin(dry_run=__dry_run__) as session:
    session.create_table_as(
        'isa', 'enhancer_reporter_assay',
        csv_reader(os.getenv('CHISEL_EXAMPLES_CSV'))
    )
