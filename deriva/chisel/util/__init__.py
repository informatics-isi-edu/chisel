"""Utility functions (internal use)."""

import logging
import nltk as _nltk
import sys
import warnings

from .describe import describe
from .graph import graph

logger = logging.getLogger(__name__)

try:
    from urllib.parse import urlparse as urlparse
except ImportError:
    from urlparse import urlparse as urlparse


def splitter_fn(delim):
    """Simple string spliter function builder.

    Creates a very simple string splitter function that splits an input string on the given `delim` character, then
    strips whitespace, and yields the resultant values one at a time.

    :param delim: delimiter character (e.g., ',')
    :return: splitter function
    """
    def splitter(s):
        if s:
            for v in s.split(delim):
                yield v.strip()
    return splitter


def introspect_key_fn(rel):
    """Key introspection function.

    Specifically, this function tries to first determine which set of attributes represent the minimal super key
    (a.k.a., candidate key), then it may attempt to apply heuristics to identify a plausible primary key (if not
    explicitly specified).

    :param rel: a relation scheme
    :return: a list of attribute names
    """
    keys = rel.get('keys')
    if not keys:
        logger.warning('Relation "%s" does not have any "keys". Cannot determine minimum key.' % rel.get('table_name'))
        return []

    minkey = None
    min_key_len = sys.maxsize
    for key in keys:
        key_len = len(key['unique_columns'])
        if key_len < min_key_len:
            minkey = key
            if key_len == 1 and minkey['unique_columns'][0] == 'RID':
                break

    return minkey['unique_columns'].copy()


def edit_distance_fn(tuple1, tuple2, **kwargs):
    """A very simple edit distance similarity function.

    :param tuple1: a tuple or a single value
    :param tuple2: a tuple or a single value
    :param kwargs: a context; e.g., may include threshold and algorithm-specific parameters
    :return: measure in [0 1] where 0 is exact match and 1 is no similarity
    """
    threshold = kwargs.get('threshold', 0.2)
    assert 0.0 <= threshold <= 1.0, 'threshold not in [0.0, 1.0]'
    tuple1 = tuple1 if isinstance(tuple1, tuple) else tuple([tuple1])
    tuple2 = tuple2 if isinstance(tuple2, tuple) else tuple([tuple2])
    assert (len(tuple1) == len(tuple2)), "tuples must be of same length"

    def to_str(v):
        return v if isinstance(v, str) or v is None else str(v)

    # compute tuple distances
    distances = []
    for i, value1 in enumerate(tuple1):
        value2 = tuple2[i]
        value1, value2 = to_str(value1), to_str(value2)
        if value1 is value2 is None or value1 == value2 == '':
            # if both values are None or '', they are considered exact matches
            distances.append(0.0)
        elif not value1 or not value2:
            # if any values is None or '', they are considered non-matches
            distances.append(1.0)
        else:
            # finally, compute and quasi-normalize the distance
            distance = _nltk.edit_distance(value1, value2)
            normal_distance = distance / (len(value1) + len(value2))
            distances.append(normal_distance)

    # return the average distance of the tuples, if below threshold, else 1.0 (declare no match)
    avg = sum(distances) / len(distances)
    if avg <= threshold:
        return avg
    else:
        return 1.0


def deprecated(f):
    """A simple 'deprecated' function decorator."""
    def wrapper(*args, **kwargs):
        warnings.warn("'%s' has been deprecated" % f.__name__, DeprecationWarning, stacklevel=2)
        return f(*args, **kwargs)
    return wrapper
