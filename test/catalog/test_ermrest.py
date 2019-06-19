import os
import unittest

from chisel.catalog.base import ComputedRelation
from chisel.operators.base import Alter
from test.utils import ERMrestHelper, BaseTestCase
import chisel.optimizer as _op

ermrest_hostname = os.getenv('CHISEL_TEST_ERMREST_HOST')
ermrest_catalog_id = os.getenv('CHISEL_TEST_ERMREST_CATALOG')


@unittest.skipUnless(ermrest_hostname, 'ERMrest hostname not defined. Set "CHISEL_TEST_ERMREST_HOST" to enable test.')
class TestERMrestCatalog (BaseTestCase):
    """Units test suite for ermrest catalog functionality."""

    catalog_helper = ERMrestHelper(ermrest_hostname, ermrest_catalog_id, unit_table_names=['list_of_closest_genes'])

    def test_basic_setup(self):
        self.assertTrue(self._catalog is not None)
        self.assertTrue(self.catalog_helper.exists(self.catalog_helper.samples))

    def test_alter_select_cname(self):
        projected_col_name = self.catalog_helper.FIELDS[1]
        with self._catalog.evolve():
            self._catalog['public'][self.catalog_helper.samples] =\
                self._catalog['public'][self.catalog_helper.samples].select(
                projected_col_name
            )
            self.assertIsInstance(self._catalog['public'][self.catalog_helper.samples], ComputedRelation)
            self.assertIsInstance(
                _op.physical_planner(_op.logical_planner(self._catalog['public'][self.catalog_helper.samples].logical_plan)),
                Alter
            )

        # validate the schema names
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        col_defs = ermrest_schema['schemas']['public']['tables'][self.catalog_helper.samples]['column_definitions']
        col_names = [col_def['name'] for col_def in col_defs]
        self.assertIn(projected_col_name, col_names)
        self.assertTrue(
            all([field == projected_col_name or field not in col_names for field in self.catalog_helper.FIELDS]))

    def test_alter_select_column(self):
        projected_col_name = self.catalog_helper.FIELDS[1]
        with self._catalog.evolve():
            self._catalog['public'][self.catalog_helper.samples] =\
                self._catalog['public'][self.catalog_helper.samples].select(
                self._catalog['public'][self.catalog_helper.samples][projected_col_name]
            )
            self.assertIsInstance(self._catalog['public'][self.catalog_helper.samples], ComputedRelation)
            self.assertIsInstance(
                _op.physical_planner(_op.logical_planner(self._catalog['public'][self.catalog_helper.samples].logical_plan)),
                Alter
            )

        # validate the schema names
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        col_defs = ermrest_schema['schemas']['public']['tables'][self.catalog_helper.samples]['column_definitions']
        col_names = [col_def['name'] for col_def in col_defs]
        self.assertIn(projected_col_name, col_names)
        self.assertTrue(
            all([field == projected_col_name or field not in col_names for field in self.catalog_helper.FIELDS]),
            'Column in altered table that should have been removed.'
        )

    def test_alter_remove_column(self):
        removed_col_name = self.catalog_helper.FIELDS[1]
        with self._catalog.evolve():
            self._catalog['public'][self.catalog_helper.samples] =\
                self._catalog['public'][self.catalog_helper.samples].select(
                ~self._catalog['public'][self.catalog_helper.samples][removed_col_name]
            )
            self.assertIsInstance(self._catalog['public'][self.catalog_helper.samples], ComputedRelation)
            self.assertIsInstance(
                _op.physical_planner(_op.logical_planner(self._catalog['public'][self.catalog_helper.samples].logical_plan)),
                Alter
            )

        # validate the schema names
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        col_defs = ermrest_schema['schemas']['public']['tables'][self.catalog_helper.samples]['column_definitions']
        col_names = [col_def['name'] for col_def in col_defs]
        self.assertNotIn(removed_col_name, col_names)
        self.assertTrue(
            all([field in col_names or field == removed_col_name for field in self.catalog_helper.FIELDS]),
            'Column not in altered table, but it should not have been removed.'
        )

    def test_ermrest_atomize(self):
        cname = 'list_of_closest_genes'
        with self._catalog.evolve():
            self._catalog['public'][cname] = \
                self._catalog['public'][self.catalog_helper.samples][cname].to_atoms()

        # validate new table is in ermrest
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        self.assertIn(cname, ermrest_schema['schemas']['public']['tables'])
