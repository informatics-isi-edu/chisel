"""Unit tests against an on disk CSV data source.
"""
from test.helpers import CatalogHelper, BaseTestCase


class TestSemistructuredCsv (BaseTestCase):
    """Units test suite for CSV-based semistructured catalog functionality.
    """

    output_basename = __name__ + '.output.csv'
    catalog_helper = CatalogHelper(table_names=[output_basename])

    def test_catalog_from_csv(self):
        self.assertIsNotNone(self._model)
        self.assertEqual(len(self._model.schemas), 1)

    def test_computed_relation_from_csv(self):
        domain = self._model.schemas['.'].tables[self.catalog_helper.samples].columns['species'].to_domain()
        self.assertIsNotNone(domain)

    def test_materialize_to_csv(self):
        samples = self._model.schemas['.'].tables[self.catalog_helper.samples]
        domain = samples.columns['species'].to_domain(similarity_fn=None)
        self._model.schemas['.'].create_table_as(self.output_basename, domain)
        self.assertTrue(self.catalog_helper.exists(self.output_basename))

    def test_clone(self):
        self._model.schemas['.'].create_table_as(
            self.output_basename, self._model.schemas['.'].tables[self.catalog_helper.samples].clone())
        self.assertTrue(self.catalog_helper.exists(self.output_basename))

    def test_join(self):
        samples = self._model.schemas['.'].tables[self.catalog_helper.samples]
        self._model.schemas['.'].create_table_as(self.output_basename, samples.join(samples))
        self.assertTrue(self.catalog_helper.exists(self.output_basename))

    def test_union(self):
        samples = self._model.schemas['.'].tables[self.catalog_helper.samples]
        self._model.schemas['.'].create_table_as(self.output_basename, samples.union(samples))
        self.assertTrue(self.catalog_helper.exists(self.output_basename))

    def test_union_add(self):
        samples = self._model.schemas['.'].tables[self.catalog_helper.samples]
        self._model.schemas['.'].create_table_as(self.output_basename, samples + samples)
        self.assertTrue(self.catalog_helper.exists(self.output_basename))

    def test_do_not_clobber(self):
        samples = self._model.schemas['.'].tables[self.catalog_helper.samples]
        with self.assertRaises(ValueError):
            self._model.schemas['.'].create_table_as(self.catalog_helper.samples, samples.clone())
