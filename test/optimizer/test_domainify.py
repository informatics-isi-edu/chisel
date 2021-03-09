"""A few unit tests specific to the to_domain operation.
"""
from test.helpers import CatalogHelper, BaseTestCase


class TestDomainify (BaseTestCase):

    output_basename = __name__ + '.output.csv'
    catalog_helper = CatalogHelper(table_names=[output_basename])

    def test_domainify_distinct(self):
        domain = self.model.schemas['.'].tables[self.catalog_helper.samples].columns['species'].to_domain(similarity_fn=None)
        self.model.schemas['.'].create_table_as(self.output_basename, domain)
        self.assertTrue(self.catalog_helper.exists(self.output_basename))

    def test_domainify_dedup(self):
        domain = self.model.schemas['.'].tables[self.catalog_helper.samples].columns['species'].to_domain()
        self.model.schemas['.'].create_table_as(self.output_basename, domain)
        self.assertTrue(self.catalog_helper.exists(self.output_basename))
