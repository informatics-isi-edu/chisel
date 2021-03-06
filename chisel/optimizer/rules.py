"""Logical optimization, composition, and transformation rules."""

import json
from pyfpm.matcher import Matcher
from .. import util
from .. import operators as _op
from ..operators import PhysicalOperator  # don't believe linters that tell you this isn't used in this module!
from .symbols import *


#
# Utility functions
#

def _rewrite_formula(formula, projection):
    """Rewrites a select formula based on any aliased attributes in a projection.

    :param formula: a select formula
    :param projection: a projection list
    :return: the rewritten formula
    """
    if not formula:
        return formula

    # Create a map from alias name -> original column name
    aliases = {}
    for element in projection:
        if isinstance(element, AttributeAlias):
            aliases[element.alias] = element.name
    if not aliases:
        return formula

    # Rewrite comparisons in formula
    if isinstance(formula, Comparison) and formula.operand1 in aliases:
        return Comparison(aliases[formula.operand1], formula.operator, formula.operand2)
    elif isinstance(formula, Conjunction):
        return Conjunction(
            tuple([
                Comparison(aliases.get(comp.operand1, comp.operand1), comp.operator, comp.operand2)
                for comp in formula.comparisons
            ])
        )
    else:
        return formula


#
# Planning and optimization rules
#

#: general purpose rules for optimizing logical operator expressions
logical_optimization_rules = Matcher([
    (
        'Distinct(Nil(), _)',
        lambda: Nil()
    ),
    (
        'Deduplicate(Nil(), _, _, _)',
        lambda: Nil()
    ),
    (
        'Deduplicate(child, attributes, None, _)',
        lambda child, attributes: Distinct(child, attributes)
    ),
    (
        'Project(Nil(), _)',
        lambda: Nil()
    ),
    (
        'Rename(child, dict())',  # TODO: remove this rule so it doesn't conflict with sname/tname only renames
        lambda child: child
    ),
    (
        'Rename(Project(child, attributes), renames)',
        lambda child, attributes, renames: Project(child, tuple([
            a for a in attributes if a not in [r.name for r in renames]
        ]) + renames)
    ),
    (
        'Select(Nil(), _)',
        lambda: Nil()
    ),
    (
        'Unnest(Nil(), _, _)',
        lambda: Nil()
    ),
    (
        'Select(Project(child, attributes), formula)',
        lambda child, attributes, formula:
        Project(
            Select(child, _rewrite_formula(formula, attributes)),
            attributes
        )
    )
])

#: composite operator rules defined as functional pattern matching expressions
logical_composition_rules = Matcher([
    (
        'Reify(child, keys, attributes)',
        lambda child, keys, attributes: Distinct(Project(child, keys + attributes), keys + attributes)
    ),
    (
        'ReifySub(_, tuple())',
        lambda: Nil()
    ),
    (
        'ReifySub(child, attributes)',
        lambda child, attributes: Project(child, (IntrospectionFunction(util.introspect_key_fn),) + attributes)
    ),
    (
        'Atomize(_, _, "")',
        lambda: Nil()
    ),
    (
        'Atomize(child, unnest_fn, attribute)',
        lambda child, unnest_fn, attribute: Unnest(ReifySub(child, (attribute,)), unnest_fn, attribute)
    ),
    (
        'Domainify(child, attribute, similarity_fn, grouping_fn)',
        lambda child, attribute, similarity_fn, grouping_fn:
        Deduplicate(
            Rename(
                Project(child, (attribute,)),
                (AttributeAlias(name=attribute, alias='name'),)
            ),
            ('name',), similarity_fn, grouping_fn
        )
    ),
    (
        'Canonicalize(child, attribute, similarity_fn, grouping_fn)',
        lambda child, attribute, similarity_fn, grouping_fn:
        Nest(
            Rename(
                Project(child, (attribute, attribute)),
                (AttributeAlias(name=attribute, alias='name'), AttributeAlias(name=attribute, alias='synonyms'))
            ),
            ('name',), ('synonyms',), similarity_fn, grouping_fn
        )
    ),
    (
        'Align(domain, child, attribute, similarity_fn, grouping_fn)',
        lambda domain, child, attribute, similarity_fn, grouping_fn:
        Rename(
            Project(
                SimilarityJoin(
                    child,
                    Project(domain, ('name', 'synonyms')),
                    Similar(attribute, 'name', 'synonyms', similarity_fn, grouping_fn),
                ),
                (AllAttributes(), AttributeDrop(attribute), AttributeDrop('synonyms'))
            ),
            (AttributeAlias(name='name', alias=attribute),)
        )
    ),
    (
        'Tagify(domain, child, attribute, unnest_fn, similarity_fn, grouping_fn)',
        lambda domain, child, attribute, unnest_fn, similarity_fn, grouping_fn:
        Align(domain, Atomize(child, unnest_fn, attribute), attribute, similarity_fn, grouping_fn)
    )
])

#: rules for transforming logical plans to physical plans
physical_transformation_rules = Matcher([
    (
        'Assign(child:PhysicalOperator, schema, table)',
        lambda child, schema, table: _op.Assign(child, schema, table)
    ),
    (
        'Assign(child:str, schema, table)',
        lambda child, schema, table: _op.Create(_op.Metadata(json.loads(child)), schema, table)
    ),
    (
        'Assign(Project(ERMrestExtant(catalog, src_sname, src_tname), attributes), dst_sname, dst_tname)'
        '   if (src_sname, src_tname) == (dst_sname, dst_tname)',
        lambda catalog, src_sname, src_tname, dst_sname, dst_tname, attributes:
        _op.Alter(_op.ERMrestProjectSelect(catalog, src_sname, src_tname, attributes), src_sname, src_tname, dst_sname, dst_tname, attributes)
    ),
    (
        'Assign(Rename(ERMrestExtant(catalog, src_sname, src_tname), attributes), dst_sname, dst_tname)',
        lambda catalog, src_sname, src_tname, dst_sname, dst_tname, attributes:
        _op.Alter(_op.ERMrestProjectSelect(catalog, src_sname, src_tname, attributes), src_sname, src_tname, dst_sname, dst_tname, attributes)
    ),
    (
        'Assign(Nil(), schema, table)',
        lambda schema, table: _op.Drop(_op.Metadata({'schema_name': schema, 'table_name': table}), schema, table)
    ),
    (
        'TempVar(child)',
        lambda child: _op.TempVarRef(child)
    ),
    (
        'Distinct(child:PhysicalOperator, attributes)',
        lambda child, attributes: _op.HashDistinct(child, attributes)
    ),
    (
        'Deduplicate(child:PhysicalOperator, attributes, similarity_fn, grouping_fn)',
        lambda child, attributes, similarity_fn, grouping_fn: _op.NestedLoopsSimilarityAggregation(_op.HashDistinct(child, attributes), attributes, [], similarity_fn, grouping_fn)
    ),
    (
        'Project(Select(ERMrestExtant(catalog, sname, tname), formula), attributes)',
        lambda catalog, sname, tname, formula, attributes: _op.ERMrestProjectSelect(catalog, sname, tname, attributes, formula)
    ),
    (
        'Project(ERMrestExtant(catalog, sname, tname), attributes)',
        lambda catalog, sname, tname, attributes: _op.ERMrestProjectSelect(catalog, sname, tname, attributes)
    ),
    (
        'Select(ERMrestExtant(catalog, sname, tname), formula)',
        lambda catalog, sname, tname, formula: _op.ERMrestSelect(catalog, sname, tname, formula)
    ),
    (
        'ERMrestExtant(catalog, sname, tname)',
        lambda catalog, sname, tname: _op.ERMrestSelect(catalog, sname, tname)
    ),
    (
        'JSONDataExtant(input_filename, json_content, object_payload, key_regex)',
        lambda input_filename, json_content, object_payload, key_regex: _op.JSONScan(input_filename, json_content, object_payload, key_regex)
    ),
    (
        'Project(child:PhysicalOperator, attributes)',
        lambda child, attributes: _op.Project(child, attributes)
    ),
    (
        'Unnest(child:PhysicalOperator, unnest_fn, attribute)',
        lambda child, unnest_fn, attribute: _op.Unnest(child, unnest_fn, attribute)
    ),
    (
        'Nest(child:PhysicalOperator, grouping, nesting, similarity_fn, grouping_fn)',
        lambda child, grouping, nesting, similarity_fn, grouping_fn:
            _op.NestedLoopsSimilarityAggregation(
                _op.HashDistinct(child, grouping + nesting),  # inject distinct on group/nest attributes in tuples
                grouping, nesting, similarity_fn, grouping_fn
            )
    ),
    (
        'Rename(child:PhysicalOperator, renames)',
        lambda child, renames: _op.Rename(child, renames)
    ),
    (
        'Select(child:PhysicalOperator, formula)',
        lambda child, formula: _op.Select(child, formula)
    ),
    (
        'Shred(graph, expression)',
        lambda graph, expression: _op.Shred(graph, expression)
    ),
    (
        'TabularDataExtant(filename)',
        lambda filename: _op.TabularFileScan(filename)
    ),
    (
        'SimilarityJoin(left:PhysicalOperator, right:PhysicalOperator, condition)',
        lambda left, right, condition: _op.NestedLoopsSimilarityJoin(left, right, condition)
    ),
    (
        'Join(left:PhysicalOperator, right:PhysicalOperator)',
        lambda left, right: _op.CrossJoin(left, right)
    ),
    (
        'Union(child:PhysicalOperator, right:PhysicalOperator)',
        lambda child, right: _op.Union(child, right)
    )
])
