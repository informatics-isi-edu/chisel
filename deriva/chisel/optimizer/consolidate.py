"""Subexpression consolidation (i.e., work sharing)."""

import collections
import logging
from ..catalog import ext
from .symbols import TempVar

logger = logging.getLogger(__name__)


def _count_plans(computed_relations):
    """Count occurrences of subexpressions across a set of computed relations.

    :param computed_relations: a set of computed relations
    :return: count of sub-plan occurrence indexed by plan
    """
    counts = collections.Counter()
    for computed_relation in computed_relations:
        # for each computed relation, begin a stack of sub-plans, starting with the top-level plan
        plans = [computed_relation.logical_plan]
        while plans:
            plan = plans.pop()
            counts[plan] += 1
            # if this sub-plan has been seen, skip counting its sub-plans
            if counts[plan] > 1:
                continue
            # if this sub-plan hasn't been seen, add its (child) sub-plans to the queue
            for child in ['child', 'left', 'right']:
                if hasattr(plan, child):
                    plans.append(getattr(plan, child))
    # return counts of plans
    return counts


def _consolidate_plan(parent, plan, counts, tempvars):
    """Consolidates the subexpressions in a plan and generates new temporary variables, as needed.

    :param parent: parent computed relation being consolidated
    :param plan: current logical sub-plan to be consolidated
    :param counts: count of sub-plan occurrences
    :param tempvars: dictionary of known temporary variables, which will be updated as needed
    :return: (rewritten logical plan, new temporary vars dictionary)
    """
    if counts[plan] > 1:
        logger.debug('Found shared work: {plan}'.format(plan=str(plan)))
        if plan in tempvars and tempvars[plan] != parent:
            logger.debug('Found existing tempvar for this sub-plan')
            # re-write the plan as a reference to the temporary var
            return TempVar(tempvars[plan]), []
        else:
            logger.debug('Temp var for this plan not found, generating a new temp var.')
            tempvars[plan] = tempvar = ext.ComputedRelation(parent.schema, plan)
            return TempVar(tempvar), [tempvar]

    # recursively rewrite the children
    new_tempvars = []
    for child in ['child', 'left', 'right']:
        if hasattr(plan, child):
            child_plan, child_vars = _consolidate_plan(parent, getattr(plan, child), counts, tempvars)
            plan = plan._replace(**{child: child_plan})
            new_tempvars.extend(child_vars)
    # return the rewritten plan
    return plan, new_tempvars


def consolidate(computed_relations):
    """Consolidates the subexpressions in a set of logical plans and returns newly generated temporary variables.

    Consolidation identifies and reuses shared work among one or more logical operator expressions. Where shared work is
    identified, a temporary variable is introduced so that common subexpressions are evaluated only once.

    :param computed_relations: a sequence of computed relations
    :return: temporary variables (i.e., shared work computed relations)
    """
    logger.debug('Consolidating {num} computed relations...'.format(num=len(computed_relations)))
    tempvars = dict()
    unconsolidated = computed_relations.copy()
    while unconsolidated:
        counts = _count_plans(unconsolidated)
        unconsolidated_tempvars = []  # keep track of new temp vars that need consolidation
        for computed_relation in unconsolidated:
            # rewrite the logical plan with tempvars where possible
            computed_relation.logical_plan, new_tempvars = \
                _consolidate_plan(computed_relation, computed_relation.logical_plan, counts, tempvars)
            # update list of unconsolidated temp vars
            unconsolidated_tempvars.extend(new_tempvars)
        # replace current set of unconsolidated work, with temp vars that will be further refined
        unconsolidated = unconsolidated_tempvars
    # return temporary variables that were accumulated during the consolidation
    logger.debug('Generated {num} reused sub-expressions.'.format(num=len(tempvars)))
    return tempvars.values()
