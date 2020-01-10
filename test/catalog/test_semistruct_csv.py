import unittest
from test.helpers import CatalogHelper, BaseTestCase


class TestSemistructuredCsv (BaseTestCase):
    """Units test suite for CSV-based semistructured catalog functionality.
    """

    output_basename = __name__ + '.output.csv'
    catalog_helper = CatalogHelper(table_names=[output_basename])

    def test_catalog_from_csv(self):
        self.assertIsNotNone(self._catalog)
        self.assertEqual(len(self._catalog.schemas), 1)

    def test_computed_relation_from_csv(self):
        domain = self._catalog['.'][self.catalog_helper.samples]['species'].to_domain()
        self.assertIsNotNone(domain)

    def test_materialize_to_csv(self):
        with self._catalog.evolve():
            domain = self._catalog['.'][self.catalog_helper.samples]['species'].to_domain(similarity_fn=None)
            self._catalog['.'][self.output_basename] = domain
        self.assertTrue(self.catalog_helper.exists(self.output_basename))

    @unittest.skip
    def test_do_not_clobber(self):
        # This is actually a general test of the 'do not clobber' feature built into the catalog
        def clobbers():
            domain = self._catalog['.'][self.catalog_helper.samples]['species'].to_domain(similarity_fn=None)
            self._catalog['.'][self.catalog_helper.samples] = domain
        self.assertRaises(ValueError, clobbers)
