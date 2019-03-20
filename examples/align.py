#!/usr/bin/env python
"""
Example of using the 'align' transformation.
"""
import os
import chisel

__dry_run__ = os.getenv('CHISEL_EXAMPLE_DRY_RUN', True)
__catalog_url__ = os.getenv('CHISEL_EXAMPLE_CATALOG_URL', 'http://localhost/ermrest/catalog/1')

catalog = chisel.connect(__catalog_url__)
print('CONNECTED')

# Align the 'sex' property of the clinical_assay table with the 'gender_terms' vocabulary
domain = catalog.s['vocab'].t['gender_terms']
catalog.s['isa'].t['clinical_assay_fixed'] = catalog.s['isa'].t['clinical_assay'].c['sex'].align(domain)
catalog.commit(dry_run=__dry_run__)
print('DONE')
