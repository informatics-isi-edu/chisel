import chisel
from .utils import CatalogHelper, BaseTestCase


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
