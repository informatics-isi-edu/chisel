"""Tests for the NestedLoopsSimilarityJoin operator."""
import logging
import os
import unittest
import deriva.chisel.operators as _op
import deriva.chisel.optimizer as _opt
import deriva.chisel.util as _util

logger = logging.getLogger(__name__)
if os.getenv('CHISEL_TEST_VERBOSE'):
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())


class TestSimilarityJoin (unittest.TestCase):

    def test_match_in_domain(self):
        left_data = [
            {
                'id': 1,
                'species': 'Mus musculus'
            },
            {
                'id': 2,
                'species': 'Danio rerio'
            }
        ]

        right_data = [
            {
                'ID': 1,
                'name': 'Mus musculus',
                'synonyms': ['Mouse']
            },
            {
                'ID': 2,
                'name': 'Danio rerio',
                'synonyms': ['Zebrafish']
            }
        ]
        left = _op.JSONScan(object_payload=left_data)
        right = _op.JSONScan(object_payload=right_data)

        condition = _opt.Similar('species', 'name', 'synonyms', _util.edit_distance_fn, None)

        sj = _op.NestedLoopsSimilarityJoin(left, right, condition)
        tuples = list(sj)
        logger.debug(tuples)

        self.assertSequenceEqual(
            sorted(list(left_data[0].keys()) + list(right_data[0].keys())),
            sorted(tuples[0].keys()),
            'expected join to contain all columns from both left and right tuples'
        )

        self.assertEqual(len(tuples), len(left_data),
                         "expected join to contain left's number of tuples")

    def test_one_mismatch(self):
        left_data = [
            {
                'id': 1,
                'species': 'Mus musculus'
            },
            {
                'id': 2,
                'species': 'Rattus rattus'
            }
        ]

        right_data = [
            {
                'ID': 1,
                'name': 'Mus musculus',
                'synonyms': ['Mouse']
            },
            {
                'ID': 2,
                'name': 'Danio rerio',
                'synonyms': ['Zebrafish']
            }
        ]
        left = _op.JSONScan(object_payload=left_data)
        right = _op.JSONScan(object_payload=right_data)

        condition = _opt.Similar('species', 'name', 'synonyms', _util.edit_distance_fn, None)

        sj = _op.NestedLoopsSimilarityJoin(left, right, condition)
        tuples = list(sj)
        logger.debug(tuples)

        self.assertSequenceEqual(
            sorted(list(left_data[0].keys()) + list(right_data[0].keys())),
            sorted(tuples[0].keys()),
            'expected join  to contain all columns from both left and right tuples'
        )

        self.assertEqual(len(tuples), len(left_data) - 1,
                         "expected join to contain left's number of tuples")

    def test_match_in_synonyms(self):
        left_data = [
            {
                'id': 1,
                'species': 'Mouse'
            },
            {
                'id': 2,
                'species': 'Zebrafish'
            }
        ]

        right_data = [
            {
                'ID': 1,
                'name': 'Mus musculus',
                'synonyms': ['Mouse']
            },
            {
                'ID': 2,
                'name': 'Danio rerio',
                'synonyms': ['Zebrafish']
            }
        ]
        left = _op.JSONScan(object_payload=left_data)
        right = _op.JSONScan(object_payload=right_data)

        condition = _opt.Similar('species', 'name', 'synonyms', _util.edit_distance_fn, None)

        sj = _op.NestedLoopsSimilarityJoin(left, right, condition)
        tuples = list(sj)
        logger.debug(tuples)

        self.assertSequenceEqual(
            sorted(list(left_data[0].keys()) + list(right_data[0].keys())),
            sorted(tuples[0].keys()),
            'expected join to contain all columns from both left and right tuples'
        )

        self.assertEqual(len(tuples), len(left_data),
                         "expected join to contain left's number of tuples")

    def test_near_matches(self):
        left_data = [
            {
                'id': 1,
                'species': 'Mus musculus'
            },
            {
                'id': 2,
                'species': 'Danio rerio'
            },
            {
                'id': 3,
                'species': 'mus musculus'
            },
            {
                'id': 4,
                'species': 'danio rerio'
            },
            {
                'id': 5,
                'species': 'Zebrafish'
            },
            {
                'id': 6,
                'species': 'Mouse'
            },
            {
                'id': 7,
                'species': 'Zbrafish'
            },
            {
                'id': 8,
                'species': 'Muse'
            }
        ]

        right_data = [
            {
                'ID': 1,
                'name': 'Mus musculus',
                'synonyms': ['Mouse']
            },
            {
                'ID': 2,
                'name': 'Danio rerio',
                'synonyms': ['Zebrafish']
            }
        ]
        left = _op.JSONScan(object_payload=left_data)
        right = _op.JSONScan(object_payload=right_data)

        condition = _opt.Similar('species', 'name', 'synonyms', _util.edit_distance_fn, None)

        sj = _op.NestedLoopsSimilarityJoin(left, right, condition)
        tuples = list(sj)
        logger.debug(tuples)

        self.assertSequenceEqual(
            sorted(list(left_data[0].keys()) + list(right_data[0].keys())),
            sorted(tuples[0].keys()),
            'expected join to contain all columns from both left and right tuples'
        )

        self.assertEqual(len(tuples), len(left_data),
                         "expected join to contain left's number of tuples")


if __name__ == '__main__':
    unittest.main()
