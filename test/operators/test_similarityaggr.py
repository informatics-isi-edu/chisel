"""Tests for the NestedLoopsSimilarityAggregation operator."""
import logging
import os
import unittest
import deriva.chisel.operators as _op
import deriva.chisel.util as _util

logger = logging.getLogger(__name__)
if os.getenv('CHISEL_TEST_VERBOSE'):
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())


class TestSimilarityAggregation (unittest.TestCase):

    def setUp(self):
        self.data = [
            {
                'id': 1,
                'name': 'Mus musculus'
            },
            {
                'id': 2,
                'name': 'mus musculus'
            },
            {
                'id': 3,
                'name': 'Mus musculi'
            },
            {
                'id': 4,
                'name': 'musmusculus'
            },
            {
                'id': 5,
                'name': 'Mus msculus'
            },
            {
                'id': 6,
                'name': 'Danio rerio'
            },
            {
                'id': 7,
                'name': 'danio rerio'
            },
            {
                'id': 8,
                'name': 'Daniorerio'
            },
            {
                'id': 9,
                'name': 'Danio reri'
            },
            {
                'id': 10,
                'name': 'DXnio rerio'
            }
        ]

    @classmethod
    def generate_duplicate_data(cls, sample_data, multiplier):
        data = []
        next_id = 1
        for datum in sample_data:
            for i in range(0, multiplier):
                duplicate = datum.copy()
                duplicate['id'] = next_id
                data.append(duplicate)
                next_id += 1
        return data

    def test_grouping_single_attr_no_nesting(self):
        child = _op.JSONScan(object_payload=self.data)
        sa = _op.NestedLoopsSimilarityAggregation(child, ('name',), tuple(), _util.edit_distance_fn, None)
        tuples = list(sa)
        logger.debug(tuples)
        logger.debug(sa.description)
        self.assertEqual(len(tuples), 2, "expected 2 groups/tuples")

    def test_grouping_and_nesting_single_attrs(self):
        for datum in self.data:  # extend raw data with synonyms
            datum['synonyms'] = datum['name']
        child = _op.JSONScan(object_payload=self.data)
        sa = _op.NestedLoopsSimilarityAggregation(child, ('name',), ('synonyms',), _util.edit_distance_fn, None)
        tuples = list(sa)
        logger.debug(tuples)
        logger.debug(sa.description)
        self.assertEqual(len(tuples), 2, "expected 2 groups/tuples")
        self.assertEqual(
            len(self.data),
            sum([len(t['synonyms']) for t in tuples]),
            "expected all synonyms to be nested"
        )

    def test_grouping_single_attr_no_nesting_w_distinct(self):
        self.data = self.generate_duplicate_data(self.data, 100)
        child = _op.JSONScan(object_payload=self.data)
        child = _op.HashDistinct(child, ('name',))  # inject a distinct
        sa = _op.NestedLoopsSimilarityAggregation(child, ('name',), tuple(), _util.edit_distance_fn, None)
        tuples = list(sa)
        self.assertEqual(len(tuples), 2, "expected 2 groups/tuples")

    def test_grouping_and_nesting_single_attrs_w_distinct(self):
        # generate test data
        for datum in self.data:  # extend raw data with synonyms
            datum['synonyms'] = datum['name']
        multiplier = 100
        self.data = self.generate_duplicate_data(self.data, multiplier)

        # create physical plan
        child = _op.JSONScan(object_payload=self.data)
        child = _op.HashDistinct(child, ('name', 'synonyms'))  # inject a distinct
        sa = _op.NestedLoopsSimilarityAggregation(child, ('name',), ('synonyms',), _util.edit_distance_fn, None)
        tuples = list(sa)

        # assertions
        self.assertEqual(len(tuples), 2, "expected 2 groups/tuples")
        self.assertEqual(
            len(self.data) / multiplier,
            sum([len(t['synonyms']) for t in tuples]),
            "expected all synonyms to be nested"
        )


if __name__ == '__main__':
    unittest.main()
