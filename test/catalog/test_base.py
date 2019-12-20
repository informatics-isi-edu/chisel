import chisel
from chisel.catalog.base import Table
from test.utils import CatalogHelper, BaseTestCase


class TestBaseCatalog (BaseTestCase):
    """Units test suite for base catalog functionality."""

    output_basename = __name__ + '.output.csv'
    catalog_helper = CatalogHelper(table_names=[output_basename])

    def test_double_evolve_error(self):
        def double_evolve():
            with self._catalog.evolve():
                with self._catalog.evolve():
                    pass
        self.assertRaises(chisel.CatalogMutationError, double_evolve)

    def test_evolve_ctx_abort(self):
        val = 'foo'
        with self._catalog.evolve() as ctx:
            ctx.abort()
            val = 'bar'
        self.assertEqual(val, 'foo', "catalog model mutation context not aborted")

    def test_evolve_ctx_abort_restore(self):
        with self._catalog.evolve() as ctx:
            temp = self._catalog['.'][self.catalog_helper.samples]['species'].to_domain()
            self._catalog['.'][self.catalog_helper.samples] = temp
            ctx.abort()
        # table should be restored
        self.assertIsInstance(self._catalog['.'][self.catalog_helper.samples], Table, "Failed to restore tables")

    def test_evolve_block_private_abort(self):
        with self.assertRaises(chisel.CatalogMutationError):
            self._catalog._abort()

    def test_evolve_block_private_commit(self):
        with self.assertRaises(chisel.CatalogMutationError):
            self._catalog._commit()

    def test_model_getters(self):
        self.assertEqual(self._catalog.schemas['.'].tables[self.catalog_helper.samples],
                         self._catalog['.'][self.catalog_helper.samples])

    def test_model_setters(self):
        with self._catalog.evolve() as ctx:
            temp = self._catalog['.'][self.catalog_helper.samples]['species'].to_domain()
            self._catalog.schemas['.'].tables['domain1'] = temp
            self._catalog['.']['domain2'] = temp
            # TODO: api should probably allow access of pending (temp) relations
            # self.assertEqual(self._catalog.schemas['.'].tables['domain1'], self._catalog['.']['domain2'])
            ctx.abort()

    def test_model_setter_not_in_evolve_ctx_err(self):
        with self.assertRaises(chisel.CatalogMutationError):
            temp = self._catalog['.'][self.catalog_helper.samples]['species'].to_domain()
            self._catalog.schemas['.'].tables['domain1'] = temp

    def test_destructive_setter_not_in_isolation_err(self):
        with self.assertRaises(chisel.CatalogMutationError):
            with self._catalog.evolve() as ctx:
                temp = self._catalog['.'][self.catalog_helper.samples]['species'].to_domain()
                self._catalog.schemas['.'].tables['domain1'] = temp
                self._catalog['.'][self.catalog_helper.samples] = temp
        # table should be restored
        self.assertIsInstance(self._catalog['.'][self.catalog_helper.samples], Table, "Failed to restore tables")

    def test_setter_after_desctructive_not_in_isolation_err(self):
        with self.assertRaises(chisel.CatalogMutationError):
            with self._catalog.evolve() as ctx:
                temp = self._catalog['.'][self.catalog_helper.samples]['species'].to_domain()
                self._catalog['.'][self.catalog_helper.samples] = temp
                self._catalog.schemas['.'].tables['domain1'] = temp
        # table should be restored
        self.assertIsInstance(self._catalog['.'][self.catalog_helper.samples], Table, "Failed to restore tables")

    def test_schema_describe(self):
        self._catalog['.'].describe()

    def test_table_describe(self):
        self._catalog['.'][self.catalog_helper.samples].describe()

    def test_schema_graph(self):
        self._catalog['.'].graph()

    def test_table_graph(self):
        self._catalog['.'][self.catalog_helper.samples].graph()
