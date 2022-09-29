"""Expression planner and optimizer."""

from pyfpm import matcher as _fpm
from . import rules as _rules
from .symbols import *
from .consolidate import consolidate


def _execute_rules_single_pass(rules, op):
    """Executes rules on an operator and its children.

    :param rules: pattern matching rules
    :param op: input operator
    :return: rewritten operator
    """
    # terminate if op is Nil or is not a symbolic operator (tuple)
    if isinstance(op, Nil) or not isinstance(op, tuple):
        return op

    # rewrite this operator
    try:
        op = rules(op)
    except _fpm.NoMatch:
        # recursively rewrite the children
        for child in ['child', 'left', 'right']:
            if hasattr(op, child):
                try:
                    op = op._replace(**{child: _execute_rules_single_pass(rules, getattr(op, child))})
                except _fpm.NoMatch:
                    pass

    # return the rewritten plan
    return op


def _execute_rules(rules, plan):
    """Executes rules on a plan, repeatedly, until a fixed point is reached.

    :param rules: pattern matching rules
    :param plan: input operator expression plan
    :return: rewritten plan
    """
    while True:
        temp = plan
        plan = _execute_rules_single_pass(rules, temp)
        if str(temp) == str(plan):
            # stop when a fixed point is reached
            break
    return plan


def logical_planner(plan):
    """Logical planner.

    The logical planner function rewrites the child logical plan by first 'composing' (transforming) a composite
    logical plan into a primitive logical plan, then 'consolidating' the primitive logical plan.

    :param plan: logical plan.
    :return The rewritten logical plan.
    """
    # rewrite according to composite rules
    plan = _execute_rules(_rules.logical_composition_rules, plan)
    # rewrite according to logical optimization rules
    plan = _execute_rules(_rules.logical_optimization_rules, plan)
    return plan


def physical_planner(plan):
    """Physical planner.

    Transforms a logical plan into a physical plan.

    :param plan: logical plan
    :return: physical plan
    """
    return _execute_rules(_rules.physical_transformation_rules, plan)


def planner(plan):
    """CHiSEL expression planner from logical plan to physical plan.

    :param plan: logical plan.
    :return: physical plan
    """
    return physical_planner(logical_planner(plan))
