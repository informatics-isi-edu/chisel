import unittest
from deriva.chisel import util


class TestEditDistance (unittest.TestCase):
    def test_empty_strings(self):
        tests = [
            (None,  None,   0.0),
            (None,  '',     1.0),
            ('',    None,   1.0),
            ('',    '',     0.0)
        ]
        for s1, s2, dist in tests:
            self.assertEqual(util.edit_distance_fn(s1, s2, threshold=1.0), dist)

    def test_empty_tuples(self):
        tests = [
            (('', ''),      ('', ''),       0.0),
            ((None, None),  (None, None),   0.0),
            (('', ''),      (None, None),   1.0),
            ((None, None),  ('', ''),       1.0),
            (('', None),    ('', ''),       0.5),
            ((None, ''),    ('', ''),       0.5),
            (('', None),    (None, None),   0.5),
            ((None, ''),    (None, None),   0.5)
        ]
        for s1, s2, dist in tests:
            self.assertEqual(util.edit_distance_fn(s1, s2, threshold=1.0), dist)

    def test_partial_empty_tuples(self):
        tests = [
            (('foo', 'bar'),    ('foo', ''),    sum([0.0 / len('foofoo'), 1.0]) / 2),
            (('foo', 'bar'),    ('foo', None),  sum([0.0 / len('foofoo'), 1.0]) / 2),
            (('foo', 'bar'),    ('', 'bar'),    sum([1.0, 0.0 / len('barbar')]) / 2),
            (('foo', 'bar'),    (None, 'bar'),  sum([1.0, 0.0 / len('barbar')]) / 2)
        ]
        for s1, s2, dist in tests:
            self.assertEqual(util.edit_distance_fn(s1, s2, threshold=1.0), dist)

    def test_simple_strings(self):
        tests = [
            ('heart',   'Heart',    1.0 / len('heartHeart')),
            ('heart',   'hart',     1.0 / len('hearthart')),
            ('foo',     'bar',      3.0 / len('foobar'))
        ]
        for s1, s2, dist in tests:
            self.assertEqual(util.edit_distance_fn(s1, s2, threshold=1.0), dist)

    def test_simple_strings_w_threshold(self):
        tests = [
            ('heart',   'Heart',    1.0 / len('heartHeart')),
            ('heart',   'hart',     1.0 / len('hearthart')),
            ('foo',     'bar',      1.0)
        ]
        for s1, s2, dist in tests:
            self.assertEqual(util.edit_distance_fn(s1, s2, threshold=0.2), dist)

    def test_simple_tuples(self):
        tests = [
            (('heart', 'lung'), ('Heart', 'Lung'),  sum([1.0 / len('heartHeart'), 1.0 / len('lungLung')]) / 2),
            (('heart', 'lung'), ('hart', 'lng'),    sum([1.0 / len('hearthart'), 1.0 / len('lunglng')]) / 2),
            (('foo', 'foo'),    ('bar', 'bar'),     sum([3.0 / len('foobar'), 3.0 / len('foobar')]) / 2)
        ]
        for s1, s2, dist in tests:
            self.assertEqual(util.edit_distance_fn(s1, s2, threshold=1.0), dist)
