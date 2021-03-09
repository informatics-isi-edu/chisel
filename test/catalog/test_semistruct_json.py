"""Unit tests against an on disk JSON data source.
"""
from test.helpers import CatalogHelper, BaseTestCase


class TestSemistructuredJson (BaseTestCase):
    """Units test suite for JSON-based semistructured catalog functionality.
    """

    output_basename = __name__ + '.output.json'
    catalog_helper = CatalogHelper(table_names=[output_basename], file_format=CatalogHelper.JSON)

    def test_catalog_from_json(self):
        self.assertIsNotNone(self.model)
        self.assertEqual(len(self.model.schemas), 1)

    def test_computed_relation_from_json(self):
        domain = self.model.schemas['.'].tables[self.catalog_helper.samples].columns['species'].to_domain()
        self.assertIsNotNone(domain)

    def test_materialize_to_json(self):
        samples = self.model.schemas['.'].tables[self.catalog_helper.samples]
        domain = samples.columns['species'].to_domain(similarity_fn=None)
        self.model.schemas['.'].create_table_as(self.output_basename, domain)
        self.assertTrue(self.catalog_helper.exists(self.output_basename))

    def test_do_not_clobber(self):
        samples = self.model.schemas['.'].tables[self.catalog_helper.samples]
        with self.assertRaises(ValueError):
            self.model.schemas['.'].create_table_as(self.catalog_helper.samples, samples.clone())
