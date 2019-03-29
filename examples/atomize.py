#!/usr/bin/env python
"""
Example of using the 'atomize' (a.k.a., 'to_atoms()') transformation.
"""
import os
import chisel

__dry_run__ = os.getenv('CHISEL_EXAMPLE_DRY_RUN', True)
__catalog_url__ = os.getenv('CHISEL_EXAMPLE_CATALOG_URL', 'http://localhost/ermrest/catalog/1')

catalog = chisel.connect(__catalog_url__)
print('CONNECTED')

# Create a new relation computed from the atomized source relation
catalog.s['isa'].t['enhancer_closest_genes'] = catalog.s['isa'].t['enhancer'].c['list_of_closest_genes'].to_atoms()
catalog.commit(dry_run=__dry_run__)
print('DONE')
