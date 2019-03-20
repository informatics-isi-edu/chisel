#!/usr/bin/env python
"""
Example of using the 'tagify' (a.k.a., 'to_tags()') transformation.
"""
import os
import chisel

__dry_run__ = os.getenv('CHISEL_EXAMPLE_DRY_RUN', True)
__catalog_url__ = os.getenv('CHISEL_EXAMPLE_CATALOG_URL', 'https://localhost/ermrest/catalog/1')

catalog = chisel.connect(__catalog_url__)
print('CONNECTED')

# Create a new relation computed from the atomized source relation
domain = catalog.s['vocab'].t['anatomy_terms']
catalog.s['isa'].t['enhancer_anatomical_structures'] = catalog.s['isa'].t['enhancer'].c['list_of_anatomical_structures'].to_tags(domain)
catalog.commit(dry_run=__dry_run__)
print('DONE')
