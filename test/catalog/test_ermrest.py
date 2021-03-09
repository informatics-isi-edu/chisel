"""Unit tests against a remote ermrest catalog.
"""
import os
import unittest
from test.helpers import ERMrestHelper, BaseTestCase
from deriva.chisel import builtin_types, Column, Table, ForeignKey

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
        self.assertTrue(self.model is not None)
        self.assertTrue(self.catalog_helper.exists(self.catalog_helper.samples))

    def _is_table_valid(self, new_tname):
        """Helper function to test if named table exists and is valid.
        """
        # is it in the ermrest schema?
        ermrest_schema = self.model.catalog.getCatalogSchema()
        self.assertIn(new_tname, ermrest_schema['schemas']['public']['tables'], 'New table not found in ermrest schema')
        # is it in the local model?
        self.assertIn(new_tname, self.model.schemas['public'].tables)
        # is the returned model object valid?
        new_table = self.model.schemas['public'].tables[new_tname]
        self.assertIsNotNone(new_table, 'New table model object not returned')
        self.assertTrue(isinstance(new_table, Table), 'Wrong type for new table object: %s' % type(new_table).__name__)

    def test_create_table(self):
        # define new table
        new_tname = self._test_create_table_tname
        table_def = Table.define(new_tname)
        # create the table
        self.model.schemas['public'].create_table(table_def)
        self._is_table_valid(new_tname)

    def test_create_table_w_fkey(self):
        # define new table
        new_tname = self._test_create_table_tname
        table_def = Table.define(
            new_tname,
            column_defs=[
                Column.define(
                    'samples_fk',
                    builtin_types.text
                )
            ],
            fkey_defs=[
                ForeignKey.define(
                    ['samples_fk'],
                    'public',
                    self.catalog_helper.samples,
                    ['RID'],
                    constraint_names=[['public', 'NEW_TABLE_samples_fk_FKey']],
                    comment='This is a unit test generated fkey',
                    on_update='NO ACTION',
                    on_delete='NO ACTION'
                )
            ]
        )

        # create the table
        self.model.schemas['public'].create_table(table_def)
        self._is_table_valid(new_tname)

    def test_alter_table_alter_column_name(self):
        samples = self.model.schemas['public'].tables[self.catalog_helper.samples]
        column_name = self.catalog_helper.FIELDS[1]
        new_column_name = 'new_column_name'
        # ...alter cname
        samples.columns[column_name].alter(name=new_column_name)
        # ...validate old cname not in table
        with self.assertRaises(KeyError):
            samples.columns[column_name]
        # ...validate new cname is in table
        self.assertIsNotNone(samples.columns[new_column_name])

    def test_alter_table_drop_column(self):
        samples = self.model.schemas['public'].tables[self.catalog_helper.samples]
        column_name = self.catalog_helper.FIELDS[1]
        # ...drop column
        samples.columns[column_name].drop()
        # ...validate old cname not in table
        with self.assertRaises(KeyError):
            samples.columns[column_name]

    def test_alter_table_add_column(self):
        samples = self.model.schemas['public'].tables[self.catalog_helper.samples]
        column_name = 'NEW_COLUMN_NAME'
        samples.create_column(Column.define(column_name, builtin_types.text))
        # ...validate new cname is in table
        self.assertIsNotNone(samples.columns[column_name])

    def test_drop_table(self):
        samples = self.model.schemas['public'].tables[self.catalog_helper.samples]
        samples.drop()
        with self.assertRaises(KeyError):
            samples.columns[self.catalog_helper.samples]

    def test_clone_table(self):
        samples = self.model.schemas['public'].tables[self.catalog_helper.samples]
        cloned_table_name = self._samples_copy_tname
        self.model.schemas['public'].create_table_as(cloned_table_name, samples.clone())
        self.assertIsNotNone(self.model.schemas['public'].tables[cloned_table_name])

    def test_alter_table_rename(self):
        samples = self.model.schemas['public'].tables[self.catalog_helper.samples]
        samples.alter(table_name=self._samples_renamed_tname)
        self.assertIsNotNone(self.model.schemas['public'].tables[self._samples_renamed_tname])

    def test_alter_table_move(self):
        samples = self.model.schemas['public'].tables[self.catalog_helper.samples]
        samples.alter(schema_name=self._test_renamed_sname)
        self.assertIsNotNone(self.model.schemas[self._test_renamed_sname].tables[self.catalog_helper.samples])
        with self.assertRaises(KeyError):
            samples = self.model.schemas['public'].tables[self.catalog_helper.samples]

    def test_smo_to_atoms(self):
        samples = self.model.schemas['public'].tables[self.catalog_helper.samples]
        cname = 'list_of_closest_genes'
        self.model.schemas['public'].create_table_as(cname, samples.columns[cname].to_atoms())
        # validate new table is in ermrest
        ermrest_schema = self.model.catalog.getCatalogSchema()
        self.assertIn(cname, ermrest_schema['schemas']['public']['tables'])
