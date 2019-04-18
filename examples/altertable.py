#!/usr/bin/env python
"""
Example of using the 'reify' transformation.
"""
import os
import chisel

__dry_run__ = os.getenv('CHISEL_EXAMPLE_DRY_RUN', True)
__catalog_url__ = os.getenv('CHISEL_EXAMPLE_CATALOG_URL', 'http://localhost/ermrest/catalog/1')

catalog = chisel.connect(__catalog_url__)
print('CONNECTED')

# Create a new relation by reifying a subset of attributes of an existing relation into a new relation
with catalog.evolve(dry_run=__dry_run__):
    catalog['isa']['enhancer'] = catalog['isa']['enhancer'].select('id')
    #catalog['isa']['foobar'] = catalog['isa']['enhancer'].select('id')
print('DONE')
