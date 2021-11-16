"""Unit tests for MMO replace operation.
"""
import os
import logging
from deriva.chisel import mmo
from deriva.core.ermrest_model import tag

from test.mmo.base import BaseMMOTestCase

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('DERIVA_PY_TEST_LOGLEVEL', default=logging.WARNING))


class TestMMOReplace (BaseMMOTestCase):

    def _pre(self, fn):
        """Pre-condition evaluation."""
        fn(self.assertTrue, self.assertFalse)

    def _post(self, fn):
        """Post-condition evaluation"""
        fn(self.assertFalse, self.assertTrue)

    def test_replace_col_in_vizcols(self):
        def cond(before, after):
            before(len(mmo.find(self.model, ["org", "dept", "postal_code"])) == 1)
            after(len(mmo.find(self.model, ["org", "dept", "zip"])) == 1)

        self._pre(cond)
        mmo.replace(self.model, ["org", "dept", "postal_code"], ["org", "dept", "zip"])
        self._post(cond)

    def test_replace_col_in_vizcols_pseudocol_simple(self):
        def cond(before, after):
            before(len(mmo.find(self.model, ["org", "dept", "street_address"])) == 1)
            after(len(mmo.find(self.model, ["org", "dept", "number_and_street_name"])) == 1)

        self._pre(cond)
        mmo.replace(self.model, ["org", "dept", "street_address"], ["org", "dept", "number_and_street_name"])
        self._post(cond)

    def test_replace_col_in_sourcedefs_columns(self):
        def cond(before, after):
            before(len(mmo.find(self.model, ["org", "dept", "country"])) == 1)
            after(len(mmo.find(self.model, ["org", "dept", "country_code"])) == 1)

        self._pre(cond)
        mmo.replace(self.model, ["org", "dept", "country"], ["org", "dept", "country_code"])
        self._post(cond)

    def test_replace_col_in_vizcols_pseudocol(self):
        def cond(before, after):
            before(len(mmo.find(self.model, ["org", "dept", "state"])) == 1)
            after(len(mmo.find(self.model, ["org", "dept", "state_or_province"])) == 1)

        self._pre(cond)
        mmo.replace(self.model, ["org", "dept", "state"], ["org", "dept", "state_or_province"])
        self._post(cond)

    def test_replace_col_in_sourcedefs_sources(self):
        def cond(before, after):
            before(len(mmo.find(self.model, ["org", "dept", "city"])) == 1)
            after(len(mmo.find(self.model, ["org", "dept", "township"])) == 1)

        self._pre(cond)
        mmo.replace(self.model, ["org", "dept", "city"], ["org", "dept", "township"])
        self._post(cond)

    # def test_prune_key_in_vizcols(self):
    #     def cond(assertion):
    #         matches = mmo.find(self.model, ["org", "dept_RID_key"])
    #         assertion(len(matches) == 1)
    #
    #     self._pre(cond)
    #     mmo.prune(self.model, ["org", "dept_RID_key"])
    #     self._post(cond)
    #
    # def test_prune_fkey_in_vizfkeys(self):
    #     fkname = ["org", "person_dept_fkey"]
    #
    #     def cond(assertion):
    #         matches = mmo.find(self.model, fkname)
    #         assertion(any([m.tag == tag.visible_foreign_keys and m.mapping == fkname for m in matches]))
    #
    #     self._pre(cond)
    #     mmo.prune(self.model, fkname)
    #     self._post(cond)
    #
    # def test_prune_fkey_in_vizcols(self):
    #     fkname = ["org", "person_dept_fkey"]
    #
    #     def cond(assertion):
    #         matches = mmo.find(self.model, fkname)
    #         assertion(any([m.tag == tag.visible_columns and m.mapping == fkname for m in matches]))
    #
    #     self._pre(cond)
    #     mmo.prune(self.model, fkname)
    #     self._post(cond)
    #
    # def test_prune_fkey_in_sourcedefs_sources(self):
    #     fkname = ["org", "person_dept_fkey"]
    #
    #     def cond(assertion):
    #         matches = mmo.find(self.model, fkname)
    #         assertion(any([m.tag == tag.source_definitions and m.mapping == 'personnel' for m in matches]))
    #
    #     self._pre(cond)
    #     mmo.prune(self.model, fkname)
    #     self._post(cond)
    #
    # def test_prune_fkey_in_sourcedefs_fkeys(self):
    #     fkname = ["org", "person_dept_fkey"]
    #
    #     def cond(assertion):
    #         matches = mmo.find(self.model, fkname)
    #         assertion(any([m.tag == tag.source_definitions and m.mapping == fkname for m in matches]))
    #
    #     self._pre(cond)
    #     mmo.prune(self.model, fkname)
    #     self._post(cond)
    #
    # def test_prune_fkey_in_sourcedefs_recurse(self):
    #     def cond(assertion):
    #         assertion(any([
    #             isinstance(vizcol, dict) and vizcol.get("sourcekey") == "dept_size"
    #             for vizcol in self.model.schemas['org'].tables['person'].annotations[tag.visible_columns]['detailed']
    #         ]))
    #
    #     self._pre(cond)
    #     mmo.prune(self.model, ["org", "person_dept_fkey"])
    #     self._post(cond)

    # todo: test for search-box
