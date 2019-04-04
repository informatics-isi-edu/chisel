from test.catalog.utils import CatalogHelper, BaseTestCase


class TestDomainify (BaseTestCase):

    output_basename = __name__ + '.output.csv'
    catalog_helper = CatalogHelper(table_names=[output_basename])

    def test_domainify_distinct(self):
        with self._catalog.evolve():
            domain = self._catalog['.'][self.catalog_helper.samples]['species'].to_domain(similarity_fn=None)
            self._catalog['.'][self.output_basename] = domain
        self.assertTrue(self.catalog_helper.exists(self.output_basename))

    def test_domainify_dedup(self):
        with self._catalog.evolve():
            domain = self._catalog['.'][self.catalog_helper.samples]['species'].to_domain()
            self._catalog['.'][self.output_basename] = domain
        self.assertTrue(self.catalog_helper.exists(self.output_basename))
