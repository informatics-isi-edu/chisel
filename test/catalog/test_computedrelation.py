"""A few direct tests on computed relatoin expressions.
"""
import unittest
from deriva.chisel.catalog.semistructured import json_reader

payload = [
    {
        'RID': 1,
        'property_1': 'abc',
        'property_2': 1234,
        'property_3': 'cat, dog, mouse'
    },
    {
        'RID': 2,
        'property_1': 'def',
        'property_2': 5678,
        'property_3': 'cat, Mouse'
    }
]

domain = [
    {'name': 'cat'},
    {'name': 'dog'},
    {'name': 'mouse'}
]


class TestComputedRelation (unittest.TestCase):
    def setUp(self):
        self._rel = json_reader(object_payload=payload)

    def tearDown(self):
        self._rel = None

    def test_description(self):
        self.assertIsNotNone(self._rel.columns)
        self.assertEqual(len(self._rel.columns), len(payload[0].keys()))

    def test_reifySub(self):
        parted = self._rel.reify_sub(self._rel.columns['property_2'])
        self.assertEqual(len(parted.columns), 2)

    def test_atomize(self):
        atomized = self._rel.columns['property_3'].to_atoms()
        self.assertEqual(len(atomized.columns), 2)

    def test_tagify(self):
        tagged = self._rel.columns['property_3'].to_tags(json_reader(object_payload=domain))
        self.assertEqual(len(tagged.columns), 2)
