#!/usr/bin/env python
"""Example of using the 'scan' operator."""
import os
import chisel

__dry_run__ = os.getenv('CHISEL_EXAMPLE_DRY_RUN', True)
__catalog_url__ = os.getenv('CHISEL_EXAMPLE_CATALOG_URL', 'http://localhost/ermrest/catalog/1')

catalog = chisel.connect(__catalog_url__)
print('CONNECTED')

# Create a new relation computed from the a scan of the csv file
with catalog.evolve(dry_run=__dry_run__):
    catalog.s['isa'].t['enhancer_reporter_assay'] = chisel.csv_reader(os.getenv('CHISEL_EXAMPLE_CSV'))
print('DONE')
