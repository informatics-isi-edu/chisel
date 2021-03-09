"""Unit tests for the JSONDataExtant operator.
"""
import unittest
import deriva.chisel.optimizer as _opt

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
    """Basic tests for JSONDataExtant operator."""
    def setUp(self):
        self._plan = _opt.JSONDataExtant(input_filename=None, json_content=None, object_payload=payload, key_regex=None)

    def tearDown(self):
        self._plan = None

    def test_logical_planner(self):
        self.assertIsNotNone(_opt.logical_planner(self._plan))

    def test_physical_planner(self):
        lp = _opt.logical_planner(self._plan)
        self.assertIsNotNone(_opt.physical_planner(lp))

    def test_has_description(self):
        lp = _opt.logical_planner(self._plan)
        pp = _opt.physical_planner(lp)
        self.assertIsNotNone(pp.description, 'description is None')

    def test_has_key(self):
        lp = _opt.logical_planner(self._plan)
        pp = _opt.physical_planner(lp)
        self.assertGreaterEqual(len(pp.description['keys']), 1, 'does not have a key definition')

    def test_can_iterate_rows(self):
        lp = _opt.logical_planner(self._plan)
        pp = _opt.physical_planner(lp)
        count_rows = 0
        for row in pp:
            count_rows += 1
            self.assertIn('RID', row, 'could not find RID in row')
            self.assertIn('property_1', row, 'could not find property_1 in row')
        self.assertEqual(count_rows, len(payload), 'could not iterate all rows')


if __name__ == '__main__':
    unittest.main()
