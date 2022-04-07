"""Unit tests for MMOxSMO project operation.
"""
from contextlib import suppress
import os
import logging
from deriva.chisel import mmo
from deriva.core.ermrest_model import tag

from test.mmo.base import BaseMMOTestCase

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('DERIVA_PY_TEST_LOGLEVEL', default=logging.WARNING))


class TestMMOxSMOProject (BaseMMOTestCase):

    unittest_tname = 'foo'

    def setUp(self):
        super(TestMMOxSMOProject, self).setUp()
        self.tearDown()

    def tearDown(self):
        super(TestMMOxSMOProject, self).tearDown()
        with suppress(KeyError):
            self.model.schemas['test'].tables[self.unittest_tname].drop()

    def test_project_prune_col_simple(self):
        """Prunes a column that directly appears in annotation without any fkey traversal."""
        cname = 'name'

        # verify found in source model
        matches = mmo.find(self.model, ['test', 'person', cname])
        self.assertTrue(len(matches) > 0)

        # select columns besides 'cname'
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].select('RID', 'dept')
        )

        matches = mmo.find(self.model, ['test', self.unittest_tname, cname])
        self.assertTrue(len(matches) == 0)

    def test_project_prune_col_in_path(self):
        """Prunes a column that requires traversals of outbound and inbound fkeys."""
        cname = 'RID'

        # verify found in source model
        matches = mmo.find(self.model, ['test', 'person', cname])
        self.assertTrue(len(matches) > 0)

        # select columns besides 'cname'
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].select('name', 'dept')
        )

        matches = mmo.find(self.model, ['test', self.unittest_tname, cname])
        self.assertTrue(len(matches) == 0)

    def test_project_rename_col_simple(self):
        """Renames a column that directly appears in annotation without any fkey traversal."""
        src_cname = 'name'
        new_cname = 'full_name'

        # verify found in source model
        matches = mmo.find(self.model, ['test', 'person', src_cname])
        self.assertTrue(len(matches) > 0)

        # select columns besides 'cname'
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].select(
                self.model.schemas['test'].tables['person'].columns[src_cname].alias(new_cname)
            )
        )

        matches = mmo.find(self.model, ['test', self.unittest_tname, new_cname])
        self.assertTrue(len(matches) > 0)

    def test_project_rename_col_in_path(self):
        """Renames a column that requires traversals of outbound and inbound fkeys."""
        src_cname = 'RID'
        new_cname = 'record_id'

        # verify found in source model
        matches = mmo.find(self.model, ['test', 'person', src_cname])
        self.assertTrue(len(matches) > 0)

        # select columns besides 'cname'
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].select(
                self.model.schemas['test'].tables['person'].columns[src_cname].alias(new_cname), 'dept'
            )
        )

        matches = mmo.find(self.model, ['test', self.unittest_tname, new_cname])
        self.assertTrue(len(matches) > 0)

    def test_reify_prune_key(self):
        src_key_name = ['test', 'person_RID_key']
        new_key_name = ['test', f'{self.unittest_tname}_RID_key']

        # verify found in source model
        matches = mmo.find(self.model, src_key_name)
        self.assertTrue(len(matches) > 0)

        # reify will project a subset of columns and form a new key, so the original key name mapping should be pruned
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].reify(['name'], 'last_name')
        )

        matches = mmo.find(self.model, new_key_name)
        self.assertTrue(len(matches) == 0)

    def test_project_prune_fkey(self):
        src_key_name = ['test', 'person_dept_fkey']
        new_key_name = ['test', f'{self.unittest_tname}_dept_fkey']

        # verify found in source model
        matches = mmo.find(self.model, src_key_name)
        self.assertTrue(len(matches) > 0)

        # this projection does not include the fkey's columns and so it will be pruned from the computed relation
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].select('last_name')
        )

        matches = mmo.find(self.model, new_key_name)
        self.assertTrue(len(matches) == 0)

    def test_project_replace_key(self):
        src_key_name = ['test', 'person_RID_key']
        new_key_name = ['test', f'{self.unittest_tname}_RID_key']

        # verify found in source model
        matches = mmo.find(self.model, src_key_name)
        self.assertTrue(len(matches) > 0)

        # projecting the 'RID' should also carry forward and rename the key name in the mappings
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].select('name', 'RID')
        )

        matches = mmo.find(self.model, new_key_name)
        self.assertTrue(len(matches) > 0)
        self.assertTrue(all([m.anchor == temp for m in matches]))

    def test_project_replace_fkey(self):
        src_key_name = ['test', 'person_dept_fkey']
        new_key_name = ['test', f'{self.unittest_tname}_dept_fkey']

        # verify found in source model
        matches = mmo.find(self.model, src_key_name)
        self.assertTrue(len(matches) > 0)

        # projecting the 'dept' should also carry forward and rename the fkey name in the mappings
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].select('dept')
        )

        matches = mmo.find(self.model, new_key_name)
        self.assertTrue(len(matches) > 0)
        self.assertTrue(all([m.anchor == temp for m in matches]))

    def test_project_rename_and_replace_key(self):
        old_cname, new_cname = 'RID', 'record_id'
        src_key_name = ['test', f'person_{old_cname}_key']
        new_key_name = ['test', f'{self.unittest_tname}_{new_cname}_key']

        # verify found in source model
        matches = mmo.find(self.model, src_key_name)
        self.assertTrue(len(matches) > 0)

        # projecting the 'dept' should also carry forward and rename the fkey name in the mappings
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].select(
                self.model.schemas['test'].tables['person'].columns[old_cname].alias(new_cname)
            )
        )

        matches = mmo.find(self.model, new_key_name)
        self.assertTrue(len(matches) > 0)
        self.assertTrue(all([m.anchor == temp for m in matches]))

    def test_project_rename_and_replace_fkey(self):
        old_cname, new_cname = 'dept', 'department'
        src_fkey_name = ['test', f'person_{old_cname}_fkey']
        new_fkey_name = ['test', f'{self.unittest_tname}_{new_cname}_fkey']

        # verify found in source model
        matches = mmo.find(self.model, src_fkey_name)
        self.assertTrue(len(matches) > 0)

        # projecting the 'dept' should also carry forward and rename the fkey name in the mappings
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].select(
                self.model.schemas['test'].tables['person'].columns[old_cname].alias(new_cname)
            )
        )

        matches = mmo.find(self.model, new_fkey_name)
        self.assertTrue(len(matches) > 0)
        self.assertTrue(all([m.anchor == temp for m in matches]))

    def test_domainify(self):
        old_cname, new_cname = 'dept', 'name'
        src_fkey_name = ['test', f'person_{old_cname}_fkey']
        new_fkey_name = ['test', f'{self.unittest_tname}_{new_cname}_fkey']

        # verify found in source model
        matches = mmo.find(self.model, src_fkey_name)
        self.assertTrue(len(matches) > 0)

        # domainifying 'dept' will rename it to 'name' but should preserve it as a foreign key
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].columns[old_cname].to_domain()
        )

        matches = mmo.find(self.model, new_fkey_name)
        self.assertTrue(len(matches) > 0)
        self.assertTrue(all([m.anchor == temp for m in matches]))

    def test_domainify_distinct(self):
        old_cname, new_cname = 'dept', 'name'
        src_fkey_name = ['test', f'person_{old_cname}_fkey']
        new_fkey_name = ['test', f'{self.unittest_tname}_{new_cname}_fkey']

        # verify found in source model
        matches = mmo.find(self.model, src_fkey_name)
        self.assertTrue(len(matches) > 0)

        # domainifying 'dept' will rename it to 'name' but should preserve it as a foreign key
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].columns[old_cname].to_domain(similarity_fn=None)
        )

        matches = mmo.find(self.model, new_fkey_name)
        self.assertTrue(len(matches) > 0)
        self.assertTrue(all([m.anchor == temp for m in matches]))

    def test_canonicalize(self):
        old_cname, new_cname = 'dept', 'name'
        src_fkey_name = ['test', f'person_{old_cname}_fkey']
        new_fkey_name = ['test', f'{self.unittest_tname}_{new_cname}_fkey']

        # verify found in source model
        matches = mmo.find(self.model, src_fkey_name)
        self.assertTrue(len(matches) > 0)

        # canonicalizing 'dept' will rename it to 'name' but should preserve it as a foreign key
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].columns[old_cname].to_domain()
        )

        matches = mmo.find(self.model, new_fkey_name)
        self.assertTrue(len(matches) > 0)
        self.assertTrue(all([m.anchor == temp for m in matches]))

    def test_union(self):
        src_key_name = ['test', 'person_dept_fkey']
        new_key_name = ['test', f'{self.unittest_tname}_dept_fkey']

        # verify found in source model
        matches = mmo.find(self.model, src_key_name)
        self.assertTrue(len(matches) > 0)

        # projecting the 'RID' should also carry forward and rename the key name in the mappings
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].select('name', 'dept').union(
                self.model.schemas['test'].tables['person'].select('name', 'dept')
            )
        )

        matches = mmo.find(self.model, new_key_name)
        self.assertTrue(len(matches) > 0)
        self.assertTrue(all([m.anchor == temp for m in matches]))

    def test_atomize(self):
        # though the operation will rename the column and therefore the key, the new key will get dropped and pruned
        # from the model due to the unnest involved in the atomize operation
        src_key_name = ['test', f'person_RID_key']
        new_key_name = ['test', f'{self.unittest_tname}_person_RID_key']

        # verify found in source model
        matches = mmo.find(self.model, src_key_name)
        self.assertTrue(len(matches) > 0)

        # atomizing the column will invalidate all key columns from the original relation
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].columns['name'].to_atoms()
        )

        matches = mmo.find(self.model, new_key_name)
        self.assertTrue(len(matches) == 0)
        self.assertTrue(all([m.anchor == temp for m in matches]))

    def test_join(self):
        # dept RID column and key should be found in model
        matches = mmo.find(self.model, ['test', 'dept_RID_key'])
        self.assertTrue(len(matches) > 0)
        matches = mmo.find(self.model, ['test', 'dept', 'RID'])
        self.assertTrue(len(matches) > 0)

        # join will invalidate all key columns from the original relations and rename conflicting columns
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['dept'].join(self.model.schemas['test'].tables['person'])
        )

        # now, left_RID should be found in model, but the key should not since keys are not preserved in a join
        matches = mmo.find(self.model, ['test', f'{self.unittest_tname}_left_RID_key'])
        self.assertTrue(len(matches) == 0)
        matches = mmo.find(self.model, ['test', self.unittest_tname, 'left_RID'])
        self.assertTrue(len(matches) > 0)
        self.assertTrue(all([m.anchor == temp for m in matches]))

    def test_icmo_add_key(self):
        cname = 'name'
        key_name = ['test', f'{self.unittest_tname}_{cname}_key']

        # reify will form a new key from the first set of column names
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            self.model.schemas['test'].tables['person'].reify([cname], 'last_name')
        )
        self.assertTrue(any([key_name in key.names for key in temp.keys]))

    def test_associate(self):
        person = self.model.schemas['test'].tables['person']

        # join will invalidate all key columns from the original relations and rename conflicting columns
        temp = self.model.schemas['test'].create_table_as(
            self.unittest_tname,
            person.associate(person.columns['dept'])
        )

        # minimal test, should be improved
        _ = self.model.catalog.getPathBuilder()
        path = _.schemas['test'].tables[self.unittest_tname]
        self.assertGreater(len(list(path.entities())), 0, 'expected non-empty results')
