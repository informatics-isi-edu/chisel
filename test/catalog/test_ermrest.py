import os
import unittest

from .utils import ERMrestHelper, BaseTestCase

ermrest_hostname = os.getenv('CHISEL_TEST_ERMREST_HOST')


@unittest.skipUnless(ermrest_hostname, 'ERMrest hostname not defined. Set "CHISEL_TEST_ERMREST_HOST" to enable test.')
class TestERMrestCatalog (BaseTestCase):
    """Units test suite for ermrest catalog functionality."""

    catalog_helper = ERMrestHelper(ermrest_hostname)

    def test_connect_setup(self):
        self.assertTrue(self._catalog is not None)
        self.assertTrue(self.catalog_helper.exists(self.catalog_helper.samples))
