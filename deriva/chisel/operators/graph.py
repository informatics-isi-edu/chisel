"""Physical operators specific to graphical data sources."""

from copy import deepcopy
import logging
import rdflib as _rdflib
from pyparsing import Word, CaselessKeyword, alphanums, OneOrMore, Literal, SkipTo
from deriva.core import ermrest_model as _em
from .base import PhysicalOperator

logger = logging.getLogger(__name__)

__all__ = ['Shred']


class Shred (PhysicalOperator):
    """Shred operator for graphical data sources."""

    #: a cache of the introspected schemas keyed on (repr(graph), expression, introspect)
    _schema_cache = {}

    def __init__(self, graph, expression, **kwargs):
        """Creates a shred operator.

        Use keyword argument `introspect` with value `deep` to force a "deep" introspection of the query to determine
        the names and types of variables returned from the query expression. By default, the operator only performs a
        shallow introspection by parsing the expression but not executing the query. Note that if 'deep' introspection
        is selected, the graph will be read and parsed, if given as a filename.

        :param graph: a filename of an RDF jsonld graph or a parsed rdflib.Graph instance
        :param expression: text of a SPARQL expression
        :param kwargs: keyword arguments
        """
        assert graph, "Invalid value for 'graph'"
        assert expression, "Invalid value for 'expression'"
        super(Shred, self).__init__()
        self._graph = graph
        self._expression = expression
        introspect = kwargs.get('introspect', 'shallow')
        cache_key = (repr(graph), expression, introspect)
        if cache_key in Shred._schema_cache:
            self._description = deepcopy(Shred._schema_cache[cache_key])
        else:
            if introspect == 'shallow':
                self._description = self._shallow_introspection(graph, expression)
            elif introspect == 'deep':
                self._description = self._deep_introspection(self._parsed_graph, expression)
            else:
                raise ValueError("unrecognized value for 'introspect' property: ${val}".format(val=introspect))
            # lastly, cache the schema
            Shred._schema_cache[cache_key] = self._description

    @property
    def _parsed_graph(self):
        # if graph is a string, assume its a filename, else assume its a rdflib.Graph object
        if isinstance(self._graph, type('str')) or isinstance(self._graph, type('unicode')):
            graph = _rdflib.Graph()
            graph.parse(self._graph)
            self._graph = graph
        elif not isinstance(self._graph, _rdflib.Graph):
            raise ValueError('graph object is not a rdflib.Graph instances')
        return self._graph

    def __iter__(self):
        # query the parsed graph and yield results
        results = self._parsed_graph.query(self._expression)
        for row in results:
            yield {str(var): str(row[var]) for var in results.vars}

    @classmethod
    def _deep_introspection(cls, graph, expression):
        """Queries the graph to generate a description of the projected variables.

        :param graph: rdflib.Graph instance
        :param expression: SPARQL query string
        :return: table definition
        """
        assert isinstance(graph, _rdflib.Graph)
        results = graph.query(expression)
        col_defs = [
            _em.Column.define(
                str(var), Shred._rdflib_to_ermrest_type(var)
            ) for var in results.vars
        ]
        return _em.Table.define(repr(graph), column_defs=col_defs, provide_system=False)

    @classmethod
    def _rdflib_to_ermrest_type(cls, var):
        """Translate from RDFLib variable types to ERMrest types."""
        if var.isdecimal():
            return _em.builtin_types.float8
        elif var.isnumeric():
            return _em.builtin_types.int8
        else:
            return _em.builtin_types.text

    @classmethod
    def _get_parser(cls):
        """Returns parser for introspecting column names from a SPARQL query string."""
        # Note, this parser is not intended to validate a SPARQL query string. It's purpose is only to extract
        # the list of projected variables from a given SELECT expression.
        if not hasattr(cls, '_parser'):
            # keywords
            select, distinct, where, as_ = map(CaselessKeyword, "select distinct where as".split())
            # identifier
            identifier = Word('?', alphanums + '_')
            identifier.setParseAction(lambda tokens: tokens[0][1:])  # strip the leading '?'
            # alias
            lpar = Literal('(').suppress()
            rpar = Literal(')').suppress()
            alias = \
                identifier + as_ + identifier | \
                lpar + SkipTo(as_) + as_ + identifier + rpar  # skips b/c we don't want to implement the full grammar
            alias.setParseAction(lambda tokens: tokens[2])  # get just the alias name
            # simple select statement parser, ignores everthing after the where clause
            _parser = select + distinct * (0, 1) + OneOrMore(alias| identifier)("projection") + where
            _parser.setParseAction(lambda tokens: tokens['projection'])
            setattr(cls, '_parser', _parser)
        return getattr(cls, '_parser', None)

    @classmethod
    def _shallow_introspection(cls, graph, expression):
        """Parses the expression and returns a table definition based on the projected variables.

        :param graph: text of filename or rdflib.Graph instance
        :param expression: SPARQL query string
        :return: table definition
        """
        parser = cls._get_parser()
        # results = parser.parseString(expression)
        # vars_ = results['projection']
        vars_ = parser.parseString(expression)
        logger.debug('Parsed vars from SPARQL expression: %s', str(vars_))

        col_defs = [
            _em.Column.define(
                var, _em.builtin_types.text
            ) for var in vars_
        ]
        return _em.Table.define(repr(graph), column_defs=col_defs, provide_system=False)
