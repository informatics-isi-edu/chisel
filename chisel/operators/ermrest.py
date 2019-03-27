"""Physical operators specific to ERMrest data sources."""

import logging
from .base import PhysicalOperator, Metadata, Project
from .base import _op

logger = logging.getLogger(__name__)


def _filter_table(table, formula):
    """Applies filters to a datapath table.

    :param table: datapath table object
    :param formula: a comparison or conjunction of comparisons
    :return: a filtered data path
    """
    assert not formula or isinstance(formula, _op.Comparison) or isinstance(formula, _op.Conjunction)
    path = table.path

    # turn formula into list of comparisons
    if not formula:
        return path
    elif isinstance(formula, _op.Comparison):
        comparisons = [formula]
    else:
        comparisons = formula.comparisons

    # turn comparisons into path filters
    for comparison in comparisons:
        path.filter(table.column_definitions[comparison.operand1] == comparison.operand2)
    return path


class ERMrestProjectSelect (Project):
    """Fused project-scan operator for ERMrest data sources."""
    def __init__(self, table, projection, formula=None):
        super(ERMrestProjectSelect, self).__init__(Metadata(table.prejson()), projection)
        self._table = table
        self._formula = formula

    def __iter__(self):
        paths = self._table.schema.catalog.ermrest_catalog.getPathBuilder()
        table = paths.schemas[self._table.schema.name].tables[self._table.name]
        filtered_path = _filter_table(table, self._formula)
        cols = [table.column_definitions[a] for a in self._attributes if a != 'RID']
        kwargs = {alias: table.column_definitions[cname] for alias, cname in self._alias_to_cname.items()}
        rows = filtered_path.entities(*cols, **kwargs)
        logger.debug("Fetching rows from '{}'".format(rows.uri))
        return iter(rows)


class ERMrestSelect (PhysicalOperator):
    """Select operator for ERMrest data sources."""
    def __init__(self, table, formula=None):
        super(ERMrestSelect, self).__init__()
        self._description = table.prejson()
        self._table = table
        self._formula = formula

    def __iter__(self):
        paths = self._table.schema.catalog.ermrest_catalog.getPathBuilder()
        table = paths.schemas[self._table.schema.name].tables[self._table.name]
        filtered_path = _filter_table(table, self._formula)
        rows = filtered_path.entities()
        logger.debug("Fetching rows from '{}'".format(rows.uri))
        return iter(rows)
