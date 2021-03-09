"""A few very basic tests.
"""
from deriva.chisel import util as _util
from test.helpers import CatalogHelper, BaseTestCase


class TestBaseCatalog (BaseTestCase):
    """Unit test suite for base catalog functionality.
    """

    output_basename = __name__ + '.output.csv'
    catalog_helper = CatalogHelper(table_names=[output_basename])

    def test_evolve_ctx_rollback(self):
        val = 'foo'
        with self.model.begin() as sess:
            sess.rollback()
            val = 'bar'
        self.assertEqual(val, 'foo', "catalog model evolve session did not exit on rollback")

    def test_catalog_describe(self):
        _util.describe(self.model)

    def test_schema_describe(self):
        _util.describe(self.model.schemas['.'])

    def test_table_describe(self):
        _util.describe(self.model.schemas['.'].tables[self.catalog_helper.samples])

    def test_catalog_graph(self):
        _util.graph(self.model)

    def test_schema_graph(self):
        _util.graph(self.model.schemas['.'])

    def test_table_graph(self):
        _util.graph(self.model.schemas['.'].tables[self.catalog_helper.samples])
