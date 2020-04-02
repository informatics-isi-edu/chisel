"""Logical symbols (operators, terms, etc.) of the planner and optimizer."""

from collections import namedtuple


#
# Internal helper functions
#

def _conjunction_and_fn(left, right):
    """A helper function for the bitwise and between conjunctions and/or comparisons.

    :param left: a Comparison or Conjunction object
    :param right: a Comparison or Conjunction object
    :return: a Conjunction object
    """
    assert isinstance(left, Conjunction) or isinstance(left, Comparison)
    if isinstance(left, Conjunction):
        comparisons = left.comparisons
    else:
        comparisons = (left,)

    if isinstance(right, Conjunction):
        comparisons += right.comparisons
    elif isinstance(right, Comparison):
        comparisons += (right,)
    else:
        raise TypeError("'&' not supported between instances of '{left}' and '{right}'".format(
            left=type(left).__name__, right=type(right).__name__
        ))

    return Conjunction(comparisons)


#
# Extant definitions
#

#: Represents an extant (existing table) from an ERMrest catalog
ERMrestExtant = namedtuple('ERMrestExtant', 'catalog sname tname')

#: JSONDataExtant operator
JSONDataExtant = namedtuple('JSONDataExtant', 'input_filename json_content object_payload key_regex')

#: TabularDataExtant operator
TabularDataExtant = namedtuple('TabularDataExtant', 'filename')


#
# Primitive operator definitions
#

#: temporary variable operator, where 'var' is a computed relation
TempVar = namedtuple('TempVar', 'var')

#: assign operator
Assign = namedtuple('Assign', 'child schema table_name')

#: distinct operator
Distinct = namedtuple('Distinct', 'child attributes')

#: deduplicate operator
Deduplicate = namedtuple('Deduplicate', 'child attributes similarity_fn grouping_fn')

#: nest operator
Nest = namedtuple('Nest', 'child grouping nesting similarity_fn grouping_fn')

#: nil operator represents a nil operator
Nil = namedtuple('Nil', '')

#: join operator, takes left and right children
Join = namedtuple('Join', 'left right')

#: project operator takes a list of 'attributes'
Project = namedtuple('Project', 'child attributes')

#: rename operator
Rename = namedtuple('Rename', 'child renames')

#: select operator takes a restriction 'formula'
Select = namedtuple('Select', 'child formula')

#: shred operator
Shred = namedtuple('Shred', 'graph expression')

#: similarity join
SimilarityJoin = namedtuple('SimilarityJoin', 'left right condition')

#: unnest operator takes an arbitrary 'unnest_fn' function and a named 'attribute'
Unnest = namedtuple('Unnest', 'child unnest_fn attribute')

#
# Composite operator definitions
#

#: align operator aligns the values of a target column with a given dictionary
Align = namedtuple('Align', 'domain child attribute similarity_fn grouping_fn')

#: decomposition operator splits a relation on a set of attributes and returns the distinct entities
Reify = namedtuple('Reify', 'child keys attributes')

#: reifySub operator takes a list of 'attributes'
ReifySub = namedtuple('ReifySub', 'child attributes')

#: atomize operator takes an arbitrary 'unnest_fn' function and a named 'attribute'
Atomize = namedtuple('Atomize', 'child unnest_fn attribute')

#: domainify operator creates a distinct domain set from an existing attribute
Domainify = namedtuple('Domainify', 'child attribute similarity_fn grouping_fn')

#: canonical-ize operator creates a vocabulary-like relation form an existing attribute
Canonicalize = namedtuple('Canonicalize', 'child attribute similarity_fn grouping_fn')

#: tagify operator atomizes an attribute and aligns the values with a given dictionary
Tagify = namedtuple('Tagify', 'domain child attribute unnest_fn similarity_fn grouping_fn')

#
# Terms, operands, and parameters
#

#: all attributes marker
AllAttributes = namedtuple('AllAttributes', '')

#: attribute alias parameter
AttributeAlias = namedtuple('AttributeAlias', 'name alias')

#: attribute drop parameter, for dropping a single attribute from a projection
AttributeDrop = namedtuple('AttributeDrop', 'name')

#: attribute add parameter, for adding a new attribute in a projection
AttributeAdd = namedtuple('AttributeAdd', 'definition')

#: function parameter
IntrospectionFunction = namedtuple('IntrospectionFunction', 'fn')

#: conjunction
Conjunction = namedtuple('Conjunction', 'comparisons')
Conjunction.__and__ = _conjunction_and_fn

#: comparison
Comparison = namedtuple('Comparison', 'operand1 operator operand2')
Comparison.__and__ = _conjunction_and_fn

#: similarity operator
Similar = namedtuple('Similar', 'attribute domain synonyms similarity_fn grouping_fn')
