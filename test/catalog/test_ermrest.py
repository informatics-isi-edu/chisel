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

    def test_connect_setup(self):
        self.assertTrue(self._catalog is not None)
        self.assertTrue(self.catalog_helper.exists(self.catalog_helper.samples))

    def test_alter(self):
        with self._catalog.evolve() as ctx:
            self._catalog['public'][self.catalog_helper.samples] =\
                self._catalog['public'][self.catalog_helper.samples].select(
                self.catalog_helper.FIELDS[1]
            )
            self.assertIsInstance(self._catalog['public'][self.catalog_helper.samples], ComputedRelation)
            self.assertIsInstance(self._catalog['public'][self.catalog_helper.samples]._physical_plan, Alter)
            ctx.abort()
