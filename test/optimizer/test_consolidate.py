"""Tests with and without enabling work sharing consolidation.
"""
from test.helpers import CatalogHelper, BaseTestCase


class TestConsolidate (BaseTestCase):

    _test_output_consolidate_gene = __name__ + '.gene.csv'
    _test_output_consolidate_anatomy = __name__ + '.anatomy.csv'
    catalog_helper = CatalogHelper(table_names=[_test_output_consolidate_gene, _test_output_consolidate_anatomy])

    def test_consolidate_disabled(self):

        with self.model.begin(enable_work_sharing=False) as sess:
            enhancer_anatomy = self.model.schemas['.'].tables[self.catalog_helper.samples].columns['list_of_anatomical_structures'].to_atoms()
            enhancer_genes = self.model.schemas['.'].tables[self.catalog_helper.samples].columns['list_of_closest_genes'].to_atoms()
            sess.create_table_as('.', self._test_output_consolidate_anatomy, enhancer_anatomy)
            sess.create_table_as('.', self._test_output_consolidate_gene, enhancer_genes)

        self.assertTrue(self.catalog_helper.exists(self._test_output_consolidate_anatomy))
        self.assertTrue(self.catalog_helper.exists(self._test_output_consolidate_gene))

    def test_consolidate_enabled(self):

        with self.model.begin(enable_work_sharing=True) as sess:
            enhancer_anatomy = self.model.schemas['.'].tables[self.catalog_helper.samples].columns['list_of_anatomical_structures'].to_atoms()
            enhancer_genes = self.model.schemas['.'].tables[self.catalog_helper.samples].columns['list_of_closest_genes'].to_atoms()
            sess.create_table_as('.', self._test_output_consolidate_anatomy, enhancer_anatomy)
            sess.create_table_as('.', self._test_output_consolidate_gene, enhancer_genes)

        self.assertTrue(self.catalog_helper.exists(self._test_output_consolidate_anatomy))
        self.assertTrue(self.catalog_helper.exists(self._test_output_consolidate_gene))
