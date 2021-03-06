import os
import unittest

from chisel.catalog.base import ComputedRelation, CatalogMutationError
from chisel.operators.base import Alter
from test.helpers import ERMrestHelper, BaseTestCase
import chisel.optimizer as _op
from chisel import data_types, Column, Table, ForeignKey

ermrest_hostname = os.getenv('DERIVA_PY_TEST_HOSTNAME')
ermrest_catalog_id = os.getenv('DERIVA_PY_TEST_CATALOG')


@unittest.skipUnless(ermrest_hostname, 'ERMrest hostname not defined.')
class TestERMrestCatalog (BaseTestCase):
    """Unit test suite for ermrest catalog functionality."""

    _samples_copy_tname = "SAMPLES COPY"
    _samples_renamed_tname = "SAMPLES RENAMED"
    _test_renamed_sname = "NEW SCHEMA"
    _test_create_table_tname = "NEW TABLE"
    _test_assoc_table_tname = "{}_{}".format(ERMrestHelper.samples, _test_create_table_tname)

    catalog_helper = ERMrestHelper(
        ermrest_hostname, ermrest_catalog_id,
        unit_schema_names=[
            _test_renamed_sname
        ],
        unit_table_names=[
            'list_of_closest_genes',
            _samples_copy_tname,
            _samples_renamed_tname,
            _test_create_table_tname,
            _test_assoc_table_tname,
            _test_renamed_sname + ':' + ERMrestHelper.samples
        ]
    )

    def test_precondition_check(self):
        self.assertTrue(self._catalog is not None)
        self.assertTrue(self.catalog_helper.exists(self.catalog_helper.samples))

    def _is_table_valid(self, new_tname):
        """Helper function to test if named table exists and is valid.
        """
        # is it in the ermrest schema?
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        self.assertIn(new_tname, ermrest_schema['schemas']['public']['tables'], 'New table not found in ermrest schema')
        # is it in the local model?
        self.assertIn(new_tname, self._catalog['public'].tables)
        # is the returned model object valid?
        new_table = self._catalog['public'].tables[new_tname]
        self.assertIsNotNone(new_table, 'New table model object not returned')
        self.assertTrue(isinstance(new_table, Table), 'Wrong type for new table object: %s' % type(new_table).__name__)
        self.assertTrue(new_table.valid, 'New table object is not "valid"')

    def test_create_table(self):
        # define new table
        new_tname = self._test_create_table_tname
        table_def = Table.define(new_tname)

        # create the table
        with self._catalog.evolve():
            self._catalog['public'].tables[new_tname] = table_def
        self._is_table_valid(new_tname)

    def test_create_table_w_fkey(self):
        # define new table
        new_tname = self._test_create_table_tname
        table_def = Table.define(
            new_tname,
            column_defs=[
                Column.define(
                    'samples_fk',
                    data_types.text
                )
            ],
            fkey_defs=[
                ForeignKey.define(
                    ['samples_fk'],
                    'public',
                    self.catalog_helper.samples,
                    ['RID'],
                    constraint_name=['public', 'NEW_TABLE_samples_fk_FKey'],
                    comment='This is a unit test generated fkey',
                    on_update='NO ACTION',
                    on_delete='NO ACTION'
                )
            ]
        )

        # create the table
        with self._catalog.evolve():
            self._catalog['public'].tables[new_tname] = table_def
        self._is_table_valid(new_tname)

    def test_allow_alter_err(self):
        with self.assertRaises(CatalogMutationError):
            with self._catalog.evolve():
                self._catalog['public'][self.catalog_helper.samples] = \
                    self._catalog['public'][self.catalog_helper.samples].select(self.catalog_helper.FIELDS[1])

    def test_alter_columns_via_select_cname(self):
        old_table_obj = self._catalog['public'][self.catalog_helper.samples]
        projected_col_name = self.catalog_helper.FIELDS[1]

        # evolve table
        with self._catalog.evolve(allow_alter=True):
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
        with self._catalog.evolve(allow_alter=True):
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
        with self._catalog.evolve(allow_alter=True):
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
        with self._catalog.evolve(allow_alter=True):
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
            dbptable.column_definitions[projected_col_name].alias(projected_col_alias)
        ).sort(dbptable.column_definitions['RID']).fetch()

        # do the rename
        with self._catalog.evolve(allow_alter=True):
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
        ).sort(dbptable.column_definitions['RID']).fetch()
        self.assertListEqual(list(original_data), list(revised_data), 'Data does not match')

    def test_alter_rename_column_direct_w_ctx(self):
        self._do_test_alter_rename_column_direct(True)

    def test_alter_rename_column_direct_wout_ctx(self):
        self._do_test_alter_rename_column_direct(False)

    def _do_test_alter_rename_column_direct(self, with_ctx):
        projected_col_name = self.catalog_helper.FIELDS[1]
        projected_col_alias = projected_col_name + ' Alias'

        # get data for later validation
        dp = self._catalog.ermrest_catalog.getPathBuilder()
        dbptable = dp.schemas['public'].tables[self.catalog_helper.samples]
        original_data = dbptable.attributes(
            dbptable.column_definitions['RID'],
            dbptable.column_definitions[projected_col_name].alias(projected_col_alias)
        ).sort(dbptable.column_definitions['RID']).fetch()

        # do the rename
        column = self._catalog['public'][self.catalog_helper.samples][projected_col_name]
        if with_ctx:
            with self._catalog.evolve(allow_alter=True):
                column.name = projected_col_alias
        else:
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
        ).sort(dbptable.column_definitions['RID']).fetch()
        self.assertListEqual(list(original_data), list(revised_data), 'Data does not match')

    def test_alter_add_column_w_ctx(self):
        self._do_test_alter_add_column(True)

    def test_alter_add_column_wout_ctx(self):
        self._do_test_alter_add_column(False)

    def _do_test_alter_add_column(self, with_ctx):
        # define new column
        new_col_name = 'NEW COLUMN NAME'
        col_def = Column.define(new_col_name, data_types['int8'])

        # do alter
        if with_ctx:
            with self._catalog.evolve(allow_alter=True):
              self._catalog['public'][self.catalog_helper.samples].columns[new_col_name] = col_def
        else:
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

    def test_drop_table_w_ctx(self):
        # keep handle to table model object
        self._do_test_drop_table(True)

    def test_drop_table_wout_ctx(self):
        # keep handle to table model object
        self._do_test_drop_table(False)

    def _do_test_drop_table(self, with_ctx):
        # keep handle to table model object
        original_table = self._catalog['public'].tables[self.catalog_helper.samples]

        # delete the table
        if with_ctx:
            with self._catalog.evolve(allow_drop=True):
                del self._catalog['public'].tables[self.catalog_helper.samples]
        else:
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

    def test_clone_table_as_select(self):
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

    def test_clone_table(self):
        # keep handle to table model object
        original_table = self._catalog['public'].tables[self.catalog_helper.samples]
        new_table_name = self._samples_copy_tname

        # copy the table
        with self._catalog.evolve():
            self._catalog['public'][new_table_name] = original_table.clone()

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

    def test_alter_rename_table(self):
        # keep handle to table model object
        original_table = self._catalog['public'].tables[self.catalog_helper.samples]
        new_table_name = self._samples_renamed_tname

        # rename the table
        with self._catalog.evolve(allow_alter=True):
            original_table.name = new_table_name

        # validate that tables has been replaced in the catalog
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        self.assertNotIn(self.catalog_helper.samples, ermrest_schema['schemas']['public']['tables'].keys(),
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

    def test_alter_move_table(self):
        # keep handle to table model object
        original_table = self._catalog['public'].tables[self.catalog_helper.samples]
        new_schema_name = self._test_renamed_sname

        # "move" the table to a different schema
        with self._catalog.evolve(allow_alter=True):
            original_table.schema = self._catalog[new_schema_name]

        # validate that tables has been moved in the catalog
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        self.assertNotIn(self.catalog_helper.samples, ermrest_schema['schemas']['public']['tables'].keys(),
                         'Table found in ermrest "public" schema')
        self.assertIn(self.catalog_helper.samples, ermrest_schema['schemas'][new_schema_name]['tables'],
                      'Table not found in ermrest "%s" schema' % new_schema_name)

        # validate that table model object has been invalidated and replaced in local model state
        self.assertFalse(original_table.valid, 'Table object not invalidated')
        self.assertTrue(all([not c.valid for c in original_table.columns.values()]))
        self.assertNotIn(self.catalog_helper.samples, self._catalog['public'].tables,
                         'Table found in catalog model "public" schema')
        self.assertIn(self.catalog_helper.samples, self._catalog[new_schema_name].tables,
                      'Table not found in catalog model "%s" schema' % new_schema_name)

    def test_ermrest_atomize(self):
        cname = 'list_of_closest_genes'
        with self._catalog.evolve():
            self._catalog['public'][cname] = \
                self._catalog['public'][self.catalog_helper.samples][cname].to_atoms()

        # validate new table is in ermrest
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        self.assertIn(cname, ermrest_schema['schemas']['public']['tables'])

    @unittest.skip
    def test_link_tables(self):
        dst_table = self._catalog['public'][self.catalog_helper.samples]

        # create new table for use in this test
        with self._catalog.evolve():
            self._catalog['public'].tables[self._test_create_table_tname] = Table.define(self._test_create_table_tname)
        src_table = self._catalog['public'].tables[self._test_create_table_tname]

        # link tables
        with self._catalog.evolve(allow_alter=True):
            src_table.link(dst_table)

        # validate new fkey column is in ermrest
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        column_names = {
            c['name']
            for c in ermrest_schema['schemas']['public']['tables'][self._test_create_table_tname]['column_definitions']
        }
        self.assertIn(self.catalog_helper.samples, column_names,
                      "Association table not found in ERMrest schema resource.")

    @unittest.skip
    def test_associate_tables(self):
        src_table = self._catalog['public'][self.catalog_helper.samples]

        # create new table for use in this test
        with self._catalog.evolve():
            self._catalog['public'].tables[self._test_create_table_tname] = Table.define(self._test_create_table_tname)
        dst_table = self._catalog['public'].tables[self._test_create_table_tname]

        # associate tables
        with self._catalog.evolve(allow_alter=True):
            src_table.associate(dst_table)

        # validate new table is in ermrest
        ermrest_schema = self._catalog.ermrest_catalog.getCatalogSchema()
        self.assertIn(self._test_assoc_table_tname, ermrest_schema['schemas']['public']['tables'],
                      "Association table not found in ERMrest schema resource.")
