"""Tests for the Rename operator."""
import unittest
import deriva.chisel.operators as _op
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


class TestRename (unittest.TestCase):
    """Basic tests for Project operator."""
    def setUp(self):
        self._child = _op.JSONScan(object_payload=payload)

    def tearDown(self):
        self._child = None

    def test_renamed_description(self):
        alias_name = 'foo'
        renames = (_opt.AttributeAlias(name='property_1', alias=alias_name),)
        oper = _op.Rename(self._child, renames)
        desc = oper.description
        self.assertIsNotNone(desc, 'description is None')
        self.assertIsNotNone(desc['column_definitions'], 'column_definitions is None')
        self.assertEqual(len(payload[0].keys()), len(desc['column_definitions']), 'incorrect number of columns in description')
        self.assertIn(alias_name, {col['name'] for col in desc['column_definitions']}, 'attribute not renamed in column definitions')

    def test_renamed_tuple_attribute(self):
        alias_name = 'foo'
        renames = (_opt.AttributeAlias(name='property_1', alias=alias_name),)
        oper = _op.Rename(self._child, renames)
        rows = list(oper)
        self.assertIn(alias_name, rows[0].keys(), 'row attribute not renamed')

    def test_rename_same_attribute_twice(self):
        projection = _op.Project(self._child, ['property_1', 'property_1'])
        renames = (
            _opt.AttributeAlias(name='property_1', alias='name'),
            _opt.AttributeAlias(name='property_1', alias='synonyms')
        )
        rename = _op.Rename(projection, renames)
        tup = list(rename)[0]
        self.assertIn('name', tup)
        self.assertIn('synonyms', tup)
        self.assertNotIn('RID', tup)


if __name__ == '__main__':
    unittest.main()
