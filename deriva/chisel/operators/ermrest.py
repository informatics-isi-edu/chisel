"""Physical operators specific to ERMrest data sources.
"""
import logging
from .base import symbols, PhysicalOperator, Metadata, Project

logger = logging.getLogger(__name__)


def _apply_filters(table, formula):
    """Applies filters to a datapath table.

    :param table: datapath table object
    :param formula: a comparison or conjunction of comparisons
    :return: a filtered data path
    """
    assert not formula or isinstance(formula, symbols.Comparison) or isinstance(formula, symbols.Conjunction)
    path = table.path

    # turn formula into list of comparisons
    if not formula:
        return path
    elif isinstance(formula, symbols.Comparison):
        comparisons = [formula]
    else:
        comparisons = formula.comparisons

    # turn comparisons into path filters
    def _to_datapath_comparison(symbolic_comparison: symbols.Comparison):
        if isinstance(symbolic_comparison, symbols.Conjunction):
            raise ValueError('Nested conjunctions not supported')
        column = table.column_definitions[symbolic_comparison.operand1]
        op = getattr(column, symbolic_comparison.operator)
        return op(symbolic_comparison.operand2)

    # apply all filters
    for comparison in comparisons:
        path.filter(_to_datapath_comparison(comparison))
    return path


class ERMrestSelectProject (Project):
    """Fused select-project operator for ERMrest data sources.
    """
    def __init__(self, model, sname, tname, projection=None, formula=None):
        """Initialize the operator.

        :param model: an ermrest Model object
        :param sname: schema name
        :param tname: table name
        :param projection: list of attributes to be returned in tuples
        :param formula: expression for filtering tuples
        """
        super(ERMrestSelectProject, self).__init__(Metadata(model.schemas[sname].tables[tname].prejson()), projection)
        self._model = model
        self._sname = sname
        self._tname = tname
        self._projection = projection
        self._formula = formula

    def __iter__(self):
        paths = self._model.catalog.getPathBuilder()
        table = paths.schemas[self._sname].tables[self._tname]
        filtered_path = _apply_filters(table, self._formula)
        cols = [
            table.column_definitions[a] for a in self._attributes
        ] + [
            table.column_definitions[cname].alias(alias) for alias, cname in self._alias_to_cname.items()
        ]
        rows = filtered_path.attributes(*cols)
        logger.debug("Fetching rows from '{}'".format(rows.uri))
        return iter(rows)


class ERMrestSelect (PhysicalOperator):
    """Select operator for ERMrest data sources.
    """
    def __init__(self, model, sname, tname, formula=None):
        """Initialize the operator.

        :param model: an ermrest Model object
        :param sname: schema name
        :param tname: table name
        :param formula: where-clause formula
        """
        super(ERMrestSelect, self).__init__()
        self._description = model.schemas[sname].tables[tname].prejson()
        self._model = model
        self._sname = sname
        self._tname = tname
        self._formula = formula

    def __iter__(self):
        paths = self._model.catalog.getPathBuilder()
        table = paths.schemas[self._sname].tables[self._tname]
        filtered_path = _apply_filters(table, self._formula)
        rows = filtered_path.entities()
        logger.debug("Fetching rows from '{}'".format(rows.uri))
        return iter(rows)
