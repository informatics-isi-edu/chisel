"""Tests for the JSONDataExtant operator."""
import unittest
import deriva.chisel.operators as _op

payload = [
    {
        'RID': 1,
        'property_1': 'hello'
    },
    {
        'RID': 2,
        'property_1': 'world'
    }
]


class TestJSONScan (unittest.TestCase):
    """Basic tests for JSONScan operator."""
    def setUp(self):
        self._op = _op.JSONScan(object_payload=payload)

    def tearDown(self):
        self._op = None

    def test_has_description(self):
        self.assertIsNotNone(self._op.description, 'description is None')

    def test_has_key(self):
        self.assertGreaterEqual(len(self._op.description['keys']), 1, 'does not have a key definition')

    def test_can_iterate_rows(self):
        count_rows = 0
        for row in self._op:
            count_rows += 1
            self.assertIn('RID', row, 'could not find RID in row')
            self.assertIn('property_1', row, 'could not find property_1 in row')
        self.assertEqual(count_rows, len(payload), 'could not iterate all rows')


if __name__ == '__main__':
    unittest.main()
