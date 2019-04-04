#!/usr/bin/env python
"""
Example of using the 'ReifySub' transformation.
"""
import os
import chisel

__dry_run__ = os.getenv('CHISEL_EXAMPLE_DRY_RUN', True)
__catalog_url__ = os.getenv('CHISEL_EXAMPLE_CATALOG_URL', 'http://localhost/ermrest/catalog/1')

catalog = chisel.connect(__catalog_url__)
print('CONNECTED')

# Create a new relation computed from the reifySubed column(s) of the source relation
with catalog.evolve(dry_run=__dry_run__):
    dataset = catalog['isa']['dataset']  # assigning to local var just for readability
    catalog['isa']['dataset_jbrowse'] = dataset.reify_sub(dataset['show_in_jbrowse'])
print('DONE')
