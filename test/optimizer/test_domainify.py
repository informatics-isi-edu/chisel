from test.catalog.utils import CatalogHelper, BaseTestCase


class TestDomainify (BaseTestCase):

    output_basename = __name__ + '.output.csv'
    catalog_helper = CatalogHelper(table_names=[output_basename])

    def test_domainify_distinct(self):
        domain = self._catalog.s['.'].t[self.catalog_helper.samples].c['species'].to_domain(similarity_fn=None)
        self._catalog.s['.'].t[self.output_basename] = domain
        self._catalog.commit()
        self.assertTrue(self.catalog_helper.exists(self.output_basename))

    def test_domainify_dedup(self):
        domain = self._catalog.s['.'].t[self.catalog_helper.samples].c['species'].to_domain()
        self._catalog.s['.'].t[self.output_basename] = domain
        self._catalog.commit()
        self.assertTrue(self.catalog_helper.exists(self.output_basename))
