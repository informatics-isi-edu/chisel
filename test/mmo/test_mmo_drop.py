"""Unit tests for MMO+DDL Drop operations.
"""
import os
import logging
from deriva.chisel import mmo
from deriva.core.ermrest_model import tag

from test.mmo.base import BaseMMOTestCase

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('DERIVA_PY_TEST_LOGLEVEL', default=logging.WARNING))


class TestMMOxDDLDrop (BaseMMOTestCase):

    def _pre(self, fn):
        """Pre-condition evaluation."""
        fn(self.assertTrue)

    def _post(self, fn):
        """Post-condition evaluation"""
        fn(self.assertFalse)

    def test_drop_fkey(self):
        fkname = ["org", "person_dept_fkey"]

        def cond(assertion):
            matches = mmo.find(self.model, fkname)
            assertion(any([m.tag == tag.visible_foreign_keys and m.mapping == fkname for m in matches]))

        self._pre(cond)
        self.model.fkey(fkname).drop()
        self._post(cond)
