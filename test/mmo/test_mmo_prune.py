"""Unit tests for MMO prune operation.
"""
import os
import logging
from deriva.chisel import mmo
from deriva.core.ermrest_model import tag

from test.mmo.base import BaseMMOTestCase

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('DERIVA_PY_TEST_LOGLEVEL', default=logging.WARNING))


class TestMMOFind (BaseMMOTestCase):

    def _pre(self, fn):
        """Pre-condition evaluation."""
        fn(self.assertTrue)

    def _post(self, fn):
        """Post-condition evaluation"""
        fn(self.assertFalse)

    def test_prune_key_in_vizcols(self):
        def cond(assertion):
            matches = mmo.find(self.model, ["org", "dept_RID_key"])
            assertion(len(matches) == 1)

        self._pre(cond)
        mmo.prune(self.model, ["org", "dept_RID_key"])
        self._post(cond)

    def test_prune_col_in_vizcols(self):
        def cond(assertion):
            matches = mmo.find(self.model, ["org", "dept", "RCT"])
            assertion(len(matches) == 1)

        self._pre(cond)
        mmo.prune(self.model, ["org", "dept", "RCT"])
        self._post(cond)

    def test_prune_col_in_vizcols_pseudocol_simple(self):
        def cond(assertion):
            matches = mmo.find(self.model, ["org", "dept", "RMT"])
            assertion(len(matches) == 1)

        self._pre(cond)
        mmo.prune(self.model, ["org", "dept", "RMT"])
        self._post(cond)

    def test_prune_col_in_vizcols_pseudocol(self):
        def cond(assertion):
            matches = mmo.find(self.model, ["org", "dept", "name"])
            assertion(any([m.anchor.name == 'person' and m.tag == tag.visible_columns and isinstance(m.mapping, dict) for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, ["org", "dept", "name"])
        self._post(cond)

    def test_prune_col_in_sourcedefs_columns(self):
        def cond(assertion):
            matches = mmo.find(self.model, ["org", "person", "dept"])
            assertion(any([m.anchor.name == 'person' and m.tag == tag.source_definitions and m.mapping == 'dept' for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, ["org", "person", "dept"])
        self._post(cond)

    def test_prune_col_in_sourcedefs_sources(self):
        def cond(assertion):
            matches = mmo.find(self.model, ["org", "person", "RID"])
            assertion(any([m.tag == tag.source_definitions and m.mapping == 'dept_size' for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, ["org", "person", "RID"])
        self._post(cond)

    def test_prune_fkey_in_vizfkeys(self):
        fkname = ["org", "person_dept_fkey"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(any([m.tag == tag.visible_foreign_keys and m.mapping == fkname for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, fkname)
        self._post(cond)

    def test_prune_fkey_in_vizcols(self):
        fkname = ["org", "person_dept_fkey"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(any([m.tag == tag.visible_columns and m.mapping == fkname for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, fkname)
        self._post(cond)

    def test_prune_fkey_in_sourcedefs_sources(self):
        fkname = ["org", "person_dept_fkey"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(any([m.tag == tag.source_definitions and m.mapping == 'personnel' for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, fkname)
        self._post(cond)

    def test_prune_fkey_in_sourcedefs_fkeys(self):
        fkname = ["org", "person_dept_fkey"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(any([m.tag == tag.source_definitions and m.mapping == fkname for m in matches]))

        self._pre(cond)
        mmo.prune(self.model, fkname)
        self._post(cond)
