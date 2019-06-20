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

    def test_alter_del_column(self):
        removed_col_name = self.catalog_helper.FIELDS[1]
        del self._catalog['public'][self.catalog_helper.samples].columns[removed_col_name]

        # validate the schema names
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        col_defs = ermrest_schema['schemas']['public']['tables'][self.catalog_helper.samples]['column_definitions']
        col_names = [col_def['name'] for col_def in col_defs]
        self.assertNotIn(removed_col_name, col_names)
        self.assertTrue(
            all([field in col_names or field == removed_col_name for field in self.catalog_helper.FIELDS]),
            'Column not in altered table, but it should not have been removed.'
        )

    def test_alter_select_alias(self):
        projected_col_name = self.catalog_helper.FIELDS[1]
        projected_col_alias = projected_col_name + ' Alias'

        # get data for later validation
        dp = self._catalog.ermrest_catalog.getPathBuilder()
        dbptable = dp.schemas['public'].tables[self.catalog_helper.samples]
        original_data = dbptable.attributes(
            dbptable.column_definitions['RID'],
            **{projected_col_alias: dbptable.column_definitions[projected_col_name]}
        ).fetch(
            sort=[dbptable.column_definitions['RID']]
        )

        # do the rename
        with self._catalog.evolve():
            self._catalog['public'][self.catalog_helper.samples] =\
                self._catalog['public'][self.catalog_helper.samples].select(
                self._catalog['public'][self.catalog_helper.samples][projected_col_name].alias(projected_col_alias)
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
        self.assertIn(projected_col_alias, col_names)
        self.assertTrue(
            all([field not in col_names for field in self.catalog_helper.FIELDS]),
            'Column in altered table that should have been removed.'
        )

        # validate the data
        dp = self._catalog.ermrest_catalog.getPathBuilder()
        dbptable = dp.schemas['public'].tables[self.catalog_helper.samples]
        revised_data = dbptable.attributes(
            dbptable.column_definitions['RID'],
            dbptable.column_definitions[projected_col_alias]
        ).fetch(
            sort=[dbptable.column_definitions['RID']]
        )
        self.assertListEqual(list(original_data), list(revised_data), 'Data does not match')

    def test_alter_col_rename(self):
        projected_col_name = self.catalog_helper.FIELDS[1]
        projected_col_alias = projected_col_name + ' Alias'

        # get data for later validation
        dp = self._catalog.ermrest_catalog.getPathBuilder()
        dbptable = dp.schemas['public'].tables[self.catalog_helper.samples]
        original_data = dbptable.attributes(
            dbptable.column_definitions['RID'],
            **{projected_col_alias: dbptable.column_definitions[projected_col_name]}
        ).fetch(
            sort=[dbptable.column_definitions['RID']]
        )

        # do the rename
        column = self._catalog['public'][self.catalog_helper.samples][projected_col_name]
        column.name = projected_col_alias

        # validate the local state object
        table_columns = self._catalog['public'][self.catalog_helper.samples].columns
        self.assertIn(projected_col_alias, table_columns, 'Alias not in table columns')
        self.assertNotIn(projected_col_name, table_columns, 'Original column name in table columns')

        # validate the schema names
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        col_defs = ermrest_schema['schemas']['public']['tables'][self.catalog_helper.samples]['column_definitions']
        col_names = [col_def['name'] for col_def in col_defs]
        self.assertIn(projected_col_alias, col_names)
        self.assertTrue(
            all([
                (field == projected_col_name and field not in col_names) or
                (field != projected_col_name and field in col_names)
                for field in self.catalog_helper.FIELDS
            ]),
            'Column in altered table that should have been removed.'
        )

        # validate the data
        dp = self._catalog.ermrest_catalog.getPathBuilder()
        dbptable = dp.schemas['public'].tables[self.catalog_helper.samples]
        revised_data = dbptable.attributes(
            dbptable.column_definitions['RID'],
            dbptable.column_definitions[projected_col_alias]
        ).fetch(
            sort=[dbptable.column_definitions['RID']]
        )
        self.assertListEqual(list(original_data), list(revised_data), 'Data does not match')

    def test_ermrest_atomize(self):
        cname = 'list_of_closest_genes'
        with self._catalog.evolve():
            self._catalog['public'][cname] = \
                self._catalog['public'][self.catalog_helper.samples][cname].to_atoms()

        # validate new table is in ermrest
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        self.assertIn(cname, ermrest_schema['schemas']['public']['tables'])
