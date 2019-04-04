#!/usr/bin/env python
"""
Example of using the 'canonicalize' transformation.
"""
import os
import chisel

__dry_run__ = os.getenv('CHISEL_EXAMPLE_DRY_RUN', True)
__catalog_url__ = os.getenv('CHISEL_EXAMPLE_CATALOG_URL', 'http://localhost/ermrest/catalog/1')

catalog = chisel.connect(__catalog_url__)
print('CONNECTED')

# Create a new 'domain' relation by extracting unique values of an attributes of an existing relation
with catalog.evolve(dry_run=__dry_run__):
    catalog['vocab']['ethnicity'] = catalog['isa']['clinical_assay']['ethnicity'].to_vocabulary()
print('DONE')
