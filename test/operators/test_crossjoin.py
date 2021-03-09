"""Tests for the CrossJoin operator."""
import unittest
import deriva.chisel.operators as _op


class TestCrossJoin (unittest.TestCase):

    def test_basic_crossjoin_no_conflicts(self):
        left_data = [
            {
                'id': 1,
                'property_1': 'hello'
            },
            {
                'id': 2,
                'property_1': 'world'
            }
        ]

        right_data = [
            {
                'name': 1,
                'property_2': 'bye'
            },
            {
                'name': 2,
                'property_2': 'moon'
            }
        ]
        left = _op.JSONScan(object_payload=left_data)
        right = _op.JSONScan(object_payload=right_data)

        cj = _op.CrossJoin(left, right)
        tuples = list(cj)
        self.assertSequenceEqual(
            sorted(list(left_data[0].keys()) + list(right_data[0].keys())),
            sorted(tuples[0].keys()),
            'expected cross join tuple to contain all columns from both left and right tuples'
        )
        self.assertEqual(len(tuples), len(left_data) * len(right_data),
                         'expected cross join to contain left * right number of tuples')

    def test_basic_crossjoin_w_conflicts(self):
        left_data = [
            {
                'id': 1,
                'foo': 'a',
                'property_1': 'hello'
            },
            {
                'id': 2,
                'foo': 'b',
                'property_1': 'world'
            }
        ]

        right_data = [
            {
                'name': 1,
                'foo': 'x',
                'property_2': 'bye'
            },
            {
                'name': 2,
                'foo': 'y',
                'property_2': 'moon'
            }
        ]
        conflict = 'foo'
        left = _op.JSONScan(object_payload=left_data)
        right = _op.JSONScan(object_payload=right_data)

        cj = _op.CrossJoin(left, right)
        tuples = list(cj)

        self.assertEqual(
            len(tuples[0].keys()), len(left_data[0].keys()) + len(right_data[0].keys()),
            'expected the cross join columns to be as many of sum of left and right columns'
        )
        self.assertEqual(
            len(tuples), len(left_data) * len(right_data),
            'expected cross join to contain left * right number of tuples'
        )

        non_conflicting_columns = [
            k for k in left_data[0] if k != conflict
        ] + [
            k for k in right_data[0] if k != conflict
        ]

        for k in non_conflicting_columns:
            self.assertIn(k, tuples[0], 'expected non-conflicting column "%s" in cross join tuple' % k)

        self.assertEqual(
            len([k for k in tuples[0] if k.endswith(conflict)]),
            2,
            'expected two variants of conflicting column "%s" in cross join tuple' % conflict
        )


if __name__ == '__main__':
    unittest.main()
