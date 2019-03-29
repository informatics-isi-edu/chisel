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
        def block_abort():
            self._catalog._abort()
        self.assertRaises(chisel.CatalogMutationError, block_abort)

    def test_evolve_block_private_commit(self):
        def block_commit():
            self._catalog._commit()

        self.assertRaises(chisel.CatalogMutationError, block_commit)
