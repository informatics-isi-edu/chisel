"""Tests for the Project operator."""
import logging
import os
import unittest
import chisel.operators as _op
import chisel.optimizer as _opt
import chisel.util as _util

logger = logging.getLogger(__name__)
if os.getenv('CHISEL_TEST_VERBOSE'):
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

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


class TestProjection (unittest.TestCase):
    """Basic tests for Project operator."""

    _child = _op.JSONScan(object_payload=payload)
    # _child.description['table_name'] = 'payload'

    def test_simple_projection_description(self):
        projection = ('property_1',)
        oper = _op.Project(self._child, projection)
        desc = oper.description
        self.assertIsNotNone(desc, 'description is None')
        self.assertIsNotNone(desc['column_definitions'], 'column_definitions is None')
        self.assertEqual(len(desc['column_definitions']), len(projection), 'incorrect number of columns in description')

    def test_simple_projection_iter(self):
        projection = ('property_1',)
        oper = _op.Project(self._child, projection)
        it = iter(oper)
        self.assertIsNotNone(it, 'must return an iterable')
        rows = list(it)
        self.assertEqual(len(rows), len(payload), 'did not return correct number of rows')
        self.assertTrue(isinstance(rows[0], dict), 'row is not a dictionary')
        self.assertEqual(len(rows[0].keys()), len(projection), 'did not project correct number of attributes')

    def test_project_all_attributes(self):
        projection = (_opt.AllAttributes(),)
        oper = _op.Project(self._child, projection)
        desc = oper.description
        self.assertEqual(len(desc['column_definitions']), len(payload[0].keys()), 'did not project all attributes')

    def test_project_and_rename_same_attribute_twice(self):
        renames = (
            _opt.AttributeAlias(name='property_1', alias='name'),
            _opt.AttributeAlias(name='property_1', alias='synonyms')
        )
        projection = _op.Project(self._child, renames)
        tup = list(projection)[0]
        self.assertIn('name', tup)
        self.assertIn('synonyms', tup)
        self.assertNotIn('RID', tup)
        cnames = [column['name'] for column in projection.description['column_definitions']]
        logger.debug(cnames)
        for expected in ['name', 'synonyms']:
            self.assertIn(expected, cnames, "column missing in projected relation's description")

    def test_project_introspect_RID(self):
        projection = [
            _opt.IntrospectionFunction(_util.introspect_key_fn),
            'property_1'
        ]
        oper = _op.Project(self._child, projection)
        renamed_rid = self._child.description['table_name'] + "_RID"
        self.assertTrue(
            any([c['name'] == renamed_rid for c in oper.description['column_definitions']]),
            "'RID' not renamed to '%s'" % renamed_rid
        )


if __name__ == '__main__':
    unittest.main()
