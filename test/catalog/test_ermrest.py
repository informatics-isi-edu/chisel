import os
import unittest

from chisel.catalog.base import ComputedRelation
from chisel.operators.base import Alter
from .utils import ERMrestHelper, BaseTestCase

ermrest_hostname = os.getenv('CHISEL_TEST_ERMREST_HOST')


@unittest.skipUnless(ermrest_hostname, 'ERMrest hostname not defined. Set "CHISEL_TEST_ERMREST_HOST" to enable test.')
class TestERMrestCatalog (BaseTestCase):
    """Units test suite for ermrest catalog functionality."""

    catalog_helper = ERMrestHelper(ermrest_hostname)

    def test_basic_setup(self):
        self.assertTrue(self._catalog is not None)
        self.assertTrue(self.catalog_helper.exists(self.catalog_helper.samples))

    def test_alter(self):
        projected_col_name = self.catalog_helper.FIELDS[1]
        with self._catalog.evolve() as ctx:
            self._catalog['public'][self.catalog_helper.samples] =\
                self._catalog['public'][self.catalog_helper.samples].select(
                projected_col_name
            )
            self.assertIsInstance(self._catalog['public'][self.catalog_helper.samples], ComputedRelation)
            self.assertIsInstance(self._catalog['public'][self.catalog_helper.samples]._physical_plan, Alter)

        # validate the schema names
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        col_defs = ermrest_schema['schemas']['public']['tables'][self.catalog_helper.samples]['column_definitions']
        col_names = [col_def['name'] for col_def in col_defs]
        self.assertIn(projected_col_name, col_names)
        self.assertTrue(
            all([field == projected_col_name or field not in col_names for field in self.catalog_helper.FIELDS]))
