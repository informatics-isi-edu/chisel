import os
import unittest

from chisel.catalog.base import ComputedRelation, CatalogMutationError
from chisel.operators.base import Alter
from test.utils import ERMrestHelper, BaseTestCase
import chisel.optimizer as _op
from chisel import builtin_types, Column

ermrest_hostname = os.getenv('CHISEL_TEST_ERMREST_HOST')
ermrest_catalog_id = os.getenv('CHISEL_TEST_ERMREST_CATALOG')


@unittest.skipUnless(ermrest_hostname, 'ERMrest hostname not defined. Set "CHISEL_TEST_ERMREST_HOST" to enable test.')
class TestERMrestCatalog (BaseTestCase):
    """Units test suite for ermrest catalog functionality."""

    _samples_copy_tname = "SAMPLES COPY"
    _samples_renamed_tname = "SAMPLES RENAMED"

    catalog_helper = ERMrestHelper(
        ermrest_hostname, ermrest_catalog_id,
        unit_table_names=[
            'list_of_closest_genes',
            _samples_copy_tname,
            _samples_renamed_tname
        ])

    def test_precondition_check(self):
        self.assertTrue(self._catalog is not None)
        self.assertTrue(self.catalog_helper.exists(self.catalog_helper.samples))

    def test_alter_columsn_via_select_cname(self):
        old_table_obj = self._catalog['public'][self.catalog_helper.samples]
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

        # validate table object invalidation
        with self.assertRaises(CatalogMutationError):
            old_table_obj.select()
        with self.assertRaises(CatalogMutationError):
            old_table_obj.columns[projected_col_name].to_domain()

        # validate new table model objects
        schema = self._catalog['public']
        new_table_obj = schema[self.catalog_helper.samples]
        self.assertIsNotNone(new_table_obj.select(), 'Could not select from new table object.')
        self.assertIsNotNone(new_table_obj.columns[projected_col_name], 'Could not get new column object.')

    def test_alter_columns_via_select_cols(self):
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

    def test_alter_drop_column_via_select(self):
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

    def test_alter_drop_column_via_del(self):
        removed_col_name = self.catalog_helper.FIELDS[1]
        removed_col = self._catalog['public'][self.catalog_helper.samples].columns[removed_col_name]
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
        with self.assertRaises(CatalogMutationError):
            removed_col.to_domain()

    def test_alter_rename_column_via_select(self):
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

    def test_alter_rename_column_direct(self):
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

    def test_alter_add_column(self):
        # define new column
        new_col_name = 'NEW COLUMN NAME'
        col_def = Column.define(new_col_name, builtin_types['int8'])
        self._catalog['public'][self.catalog_helper.samples].columns[new_col_name] = col_def

        # validate the schema names
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        col_defs = ermrest_schema['schemas']['public']['tables'][self.catalog_helper.samples]['column_definitions']
        col_names = [col_def['name'] for col_def in col_defs]
        self.assertIn(new_col_name, col_names)
        self.assertTrue(
            all([field in col_names for field in self.catalog_helper.FIELDS]),
            'Column not in altered table, but it should not have been removed.'
        )

    def test_drop_table(self):
        # keep handle to table model object
        original_table = self._catalog['public'].tables[self.catalog_helper.samples]

        # delete the table
        del self._catalog['public'].tables[self.catalog_helper.samples]

        # validate that it is no longer in catalog
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        self.assertNotIn(self.catalog_helper.samples, ermrest_schema['schemas']['public']['tables'],
                         'Table "%s" found in ermrest catalog schema' % self.catalog_helper.samples)

        # validate the model invalidation
        self.assertFalse(original_table.valid, 'Table object not invalidated')
        self.assertTrue(all([not c.valid for c in original_table.columns.values()]))
        self.assertNotIn(self.catalog_helper.samples, self._catalog['public'].tables,
                         'Table "%s" found in local catalog model' % self.catalog_helper.samples)

    def test_copy_table(self):
        # keep handle to table model object
        original_table = self._catalog['public'].tables[self.catalog_helper.samples]
        new_table_name = self._samples_copy_tname

        # copy the table
        with self._catalog.evolve():
            self._catalog['public'][new_table_name] = original_table.select()

        # validate that original and copy are in the catalog
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        self.assertIn(self.catalog_helper.samples, ermrest_schema['schemas']['public']['tables'],
                      'Table "%s" not found in ermrest catalog schema' % self.catalog_helper.samples)
        self.assertIn(new_table_name, ermrest_schema['schemas']['public']['tables'],
                      'Table "%s" not found in ermrest catalog schema' % new_table_name)

        # model objects should be valid
        self.assertTrue(original_table.valid, 'Table object not valid')
        self.assertIn(self.catalog_helper.samples, self._catalog['public'].tables,
                      'Table "%s" found in local catalog model' % self.catalog_helper.samples)
        self.assertIn(new_table_name, self._catalog['public'].tables,
                      'Table "%s" not found in local catalog model' % new_table_name)

    def test_rename_table(self):
        # keep handle to table model object
        original_table = self._catalog['public'].tables[self.catalog_helper.samples]
        new_table_name = self._samples_renamed_tname

        # rename the table
        original_table.name = new_table_name

        # validate that tables has been replaced in the catalog
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        self.assertNotIn(self.catalog_helper.samples, ermrest_schema['schemas']['public']['tables'],
                         'Table "%s" found in ermrest catalog schema' % self.catalog_helper.samples)
        self.assertIn(new_table_name, ermrest_schema['schemas']['public']['tables'],
                      'Table "%s" not found in ermrest catalog schema' % new_table_name)

        # validate that table model object has been invalidated and replaced in local model state
        self.assertFalse(original_table.valid, 'Table object not invalidated')
        self.assertTrue(all([not c.valid for c in original_table.columns.values()]))
        self.assertNotIn(self.catalog_helper.samples, self._catalog['public'].tables,
                         'Table "%s" found in local catalog model' % self.catalog_helper.samples)
        self.assertIn(new_table_name, self._catalog['public'].tables,
                      'Table "%s" not found in local catalog model' % new_table_name)

    def test_ermrest_atomize(self):
        cname = 'list_of_closest_genes'
        with self._catalog.evolve():
            self._catalog['public'][cname] = \
                self._catalog['public'][self.catalog_helper.samples][cname].to_atoms()

        # validate new table is in ermrest
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        self.assertIn(cname, ermrest_schema['schemas']['public']['tables'])
