"""Database catalog module."""

import collections
from collections import abc as _abc
import itertools
import logging
import pprint as pp
from graphviz import Digraph
from deriva.core import ermrest_model as _em
from .. import optimizer as _op, operators, util

logger = logging.getLogger(__name__)


# def _kwargs(**kwargs):
#     """Helper for extending module with sub-types for the whole model tree."""
#     kwargs2 = {
#         'schema_class': Schema,
#         'table_class': AbstractTable,
#         'column_class': Column
#     }
#     kwargs2.update(kwargs)
#     return kwargs2


class AbstractCatalog (object):
    """Abstract base class for catalogs."""

    # TODO: revisit this after the basic functionality is restored
    # class CatalogSchemas(collections.abc.Mapping):
    #     """Collection of catalog schema model objects."""
    #
    #     def __init__(self, catalog, backing):
    #         """Initializes the collection.
    #
    #         :param catalog: the parent catalog
    #         :param backing: the backing collection must be a Mapping
    #         """
    #         assert (isinstance(backing, collections.abc.Mapping))
    #         self._backing = backing
    #
    #     def __getitem__(self, item):
    #         return self._backing[item]
    #
    #     def __iter__(self):
    #         return iter(self._backing)
    #
    #     def __len__(self):
    #         return len(self._backing)

    def __init__(self, model_doc):
        super(AbstractCatalog, self).__init__()
        self._model_doc = model_doc
        self._schemas = {schema_name: self._new_schema_instance(model_doc['schemas'][schema_name]) for schema_name in model_doc['schemas']}
        # TODO: compute 'referenced_by' in the tables

    @property
    def schemas(self):
        """Map of schema names to schema model objects."""
        return self._schemas

    @property
    def s(self):
        return self._schemas

    def _new_schema_instance(self, schema_doc):
        """Overridable method for creating a new schema model object.

        :param schema_doc: the schema document
        :return: schema model object
        """
        return Schema(self, schema_doc)

    def __getitem__(self, item):
        """Maps a schema name to a schema model object.

        This is a short-hand for `catalog.schemas[schema_name]`.
        """
        return self.schemas[item]

    def describe(self):
        """Returns a text (markdown) description."""

        def _make_markdown_repr(quote=lambda s: s):
            data = [
                ["Name", "Comment"]
            ] + [
                [s.name, s.comment] for s in self.schemas.values()
            ]
            desc = "### List of schemas\n" + \
                   util.markdown_table(data, quote)
            return desc

        class Description:
            def _repr_markdown_(self):
                return _make_markdown_repr(quote=util.markdown_quote)

            def __repr__(self):
                return _make_markdown_repr()

        return Description()

    def graph(self, engine='fdp'):
        """Generates and returns a graphviz Digraph.

        :param engine: text name for the graphviz engine (dot, neato, circo, etc.)
        :return: a Graph object that can be rendered directly by jupyter notbook or qtconsole
        """
        dot = Digraph(name='Catalog Model', engine=engine, node_attr={'shape': 'box'})

        # add nodes
        for schema in self.schemas.values():
            with dot.subgraph(name=schema.name, node_attr={'shape': 'box'}) as subgraph:
                for table in schema.tables.values():
                    label = "%s.%s" % (schema.name, table.name)
                    subgraph.node(label, label)

        # add edges
        for schema in self.schemas.values():
            for table in schema.tables.values():
                tail_name = "%s.%s" % (schema.name, table.name)
                for fkey in table.foreign_keys:
                    refcol = fkey.referenced_columns[0]
                    head_name = "%s.%s" % (refcol['schema_name'], refcol['table_name'])
                    dot.edge(tail_name, head_name)

        return dot

    def _materialize_relation(self, schema, plan):
        """Materializes a relation from a physical plan.

        :param schema: a `Schema` in which to materialize the relation
        :param plan: a `PhysicalOperator` instance from which to materialize the relation
        :return: None
        """
        raise NotImplementedError()

    def commit(self, dry_run=False, consolidate=True):
        """Commits pending computed relation assignments to the catalog.

        :param dry_run: if set to True, the pending commits will be drained, debug output printed, but not committed.
        :param consolidate: if set to True, attempt to consolidate shared work between pending operations.
        """
        # Find all pending assignment operations
        computed_relations = []
        for schema in self.schemas.values():
            for value in schema.tables.pending:
                assert isinstance(value, ComputedRelation)
                computed_relations.append(value)
            schema.tables.reset()

        logger.info('Committing {num} pending computed relations'.format(num=len(computed_relations)))

        # Consolidate the computed relations; i.e., identify and consolidate shared work
        if consolidate:
            _op.consolidate(computed_relations)

        # Process the pending operations
        for computed_relation in computed_relations:
            # get its optimized and consolidated logical plan
            logical_plan = computed_relation.logical_plan
            # do physical planning
            physical_plan = _op.physical_planner(logical_plan)

            if dry_run:
                logger.info('Dry run: no changes to catalog will be performed.')
                print('Logical plan:')
                print(logical_plan)
                print('Physical plan:')
                print(physical_plan)
                print('Schema:')
                pp.pprint(physical_plan.description)
                print('Data:')
                pp.pprint(list(itertools.islice(physical_plan, 100)))
            else:
                # Materialize the planned relation
                logging.info('Materializing "{name}"...'.format(name=computed_relation.name))
                self._materialize_relation(self.schemas[computed_relation.sname], physical_plan)


class Schema (object):
    """Represents a 'schema' (a.k.a., a namespace) in a database catalog."""
    def __init__(self, catalog, schema_doc):
        super(Schema, self).__init__()
        self._catalog = catalog
        self.name = schema_doc['schema_name']
        self.comment = schema_doc['comment']
        self._tables = {table_name: self._new_table_instance(schema_doc['tables'][table_name]) for table_name in schema_doc['tables']}
        self.tables = SchemaTables(self, self._tables)

    def _new_table_instance(self, table_doc):
        """Overridable method for creating a new table model object.

        :param table_doc: the table document
        :return: table model object
        """
        return AbstractTable(table_doc)

    def describe(self):
        """Returns a text (markdown) description."""

        def _make_markdown_repr(quote=lambda s: s):
            data = [
                ["Schema", "Name", "Kind", "Comment"]
            ] + [
                [self.name, t.name, t.kind, t.comment] for t in self.tables.values()
            ]
            desc = "### List of Tables\n" + \
                   util.markdown_table(data, quote)
            return desc

        class Description:
            def _repr_markdown_(self):
                return _make_markdown_repr(quote=util.markdown_quote)

            def __repr__(self):
                return _make_markdown_repr()

        return Description()

    def graph(self, engine='fdp'):
        """Generates and returns a graphviz Digraph.

        :param engine: text name for the graphviz engine (dot, neato, circo, etc.)
        :return: a Graph object that can be rendered directly by jupyter notbook or qtconsole
        """
        dot = Digraph(name=self.name, engine=engine, node_attr={'shape': 'box'})

        # add nodes
        for table in self.tables.values():
            label = "%s.%s" % (self.name, table.name)
            dot.node(label, label)

        # track referenced nodes
        seen = set()

        # add edges
        for table in self.tables.values():
            # add outbound edges
            tail_name = "%s.%s" % (self.name, table.name)
            for fkey in table.foreign_keys:
                refcol = fkey.referenced_columns[0]
                head_name = "%s.%s" % (refcol['schema_name'], refcol['table_name'])
                if head_name not in seen:
                    dot.node(head_name, head_name)
                    seen.add(head_name)
                dot.edge(tail_name, head_name)

            # add inbound edges
            head_name = tail_name
            for reference in table.referenced_by:
                tail_name = "%s.%s" % (reference.sname, reference.tname)
                if tail_name not in seen:
                    dot.node(tail_name, tail_name)
                    seen.add(tail_name)
                dot.edge(tail_name, head_name)

        return dot

    @property
    def t(self):
        """Shorthand for the tables container."""
        return self.tables


class SchemaTables (collections.abc.MutableMapping):
    """Container class for schema tables (for internal use only).

    This class mostly passes through container methods to the underlying tables container. Its purpose is to facilitate
    assignment of new, computed relations to the catalog.
    """
    def __init__(self, schema, backing):
        """A collection of schema tables.

        :param schema: the parent schema
        :param backing: the backing collection, which must be a Mapping
        """
        super(SchemaTables, self).__init__()
        self._schema = schema
        self._base_tables = backing
        self._pending_assignments = {}

    def _ipython_key_completions_(self):
        return self._base_tables.keys()

    @property
    def pending(self):
        """List of 'pending' assignments to this schema."""
        return self._pending_assignments.values()

    def reset(self):
        """Resets the pending assignments to this schema."""
        self._pending_assignments = {}

    def __str__(self):
        tables = self._base_tables.copy()
        tables.update(self._pending_assignments)
        return str(tables)

    def __getitem__(self, item):
        return self._base_tables[item]

    def __setitem__(self, key, value):
        if isinstance(value, _em.Table) and not isinstance(value, ComputedRelation):
            self._base_tables[key] = value
        elif not isinstance(value, ComputedRelation):
            raise ValueError('Computed relation expected')
        elif key in self._base_tables:
            raise ValueError('Table assignment to an exiting table not allow.')
        elif key in self._pending_assignments:
            raise ValueError('Table assignment already pending.')
        else:
            self._pending_assignments[key] = ComputedRelation(_op.Assign(value.logical_plan, self._schema, key))  # TODO fixme

    def __delitem__(self, key):
        if key in self._base_tables:
            del self._base_tables[key]
        elif key in self._pending_assignments:
            del self._pending_assignments
        else:
            raise KeyError(key + " not found")

    def __iter__(self):
        return iter(self._base_tables)

    def __len__(self):
        return self._base_tables


class AbstractTable (object):
    """Abstract base class for database tables."""
    def __init__(self, table_doc):
        super(AbstractTable, self).__init__()
        self._table_doc = table_doc
        self.name = table_doc['table_name']
        self.sname = table_doc['schema_name']
        self.kind = table_doc['kind']
        self.column_definitions = collections.OrderedDict([
            (col['name'], self._new_column_instance(col)) for col in table_doc['column_definitions']
        ])
        self.foreign_keys = table_doc['foreign_keys']
        self.referenced_by = []  # TODO: need to add to the catalog a method to compute these

        # TODO: this may not be necessary for the OrderedDict
        # monkey patch the column definitions for ipython key completions
        # setattr(
        #     self.column_definitions,
        #     '_ipython_key_completions_',
        #     lambda: list(self.column_definitions.elements.keys())
        # )

    def _new_column_instance(self, column_doc):
        """Overridable method for creating a new column model object.

        :param column_doc: the column document
        :return: column model object
        """
        return Column(self, column_doc)

    def prejson(self):
        """Returns a JSON-ready representation of this table model object.

        :return: a JSON-ready representation of this table model object
        """
        return self._table_doc

    def describe(self):
        """Returns a text (markdown) description."""
        def type2str(t):
            if t.is_array:
                return t.typename + "[]"
            else:
                return t.typename

        def _make_markdown_repr(quote=lambda s: s):
            data = [
                ["Column", "Type", "Nullable", "Default", "Comment"]
            ] + [
                [col.name, type2str(col.type), str(col.nullok), col.default, col.comment] for col in self.column_definitions
            ]
            desc = "### Table \"" + self.sname + "." + self.name + "\"\n" + \
                   util.markdown_table(data, quote)
            return desc

        class Description:
            def _repr_markdown_(self):
                return _make_markdown_repr(quote=util.markdown_quote)

            def __repr__(self):
                return _make_markdown_repr()

        return Description()

    def graph(self, engine='fdp'):
        """Generates and returns a graphviz Digraph.

        :param engine: text name for the graphviz engine (dot, neato, circo, etc.)
        :return: a Graph object that can be rendered directly by jupyter notbook or qtconsole
        """
        dot = Digraph(name=self.name, engine=engine, node_attr={'shape': 'box'})

        # add node
        label = "%s.%s" % (self.sname, self.name)
        dot.node(label, label)

        # track referenced nodes
        seen = set()

        # add edges
        # add outbound edges
        tail_name = "%s.%s" % (self.sname, self.name)
        for fkey in self.foreign_keys:
            refcol = fkey.referenced_columns[0]
            head_name = "%s.%s" % (refcol['schema_name'], refcol['table_name'])
            if head_name not in seen:
                dot.node(head_name, head_name)
                seen.add(head_name)
            dot.edge(tail_name, head_name)

        # add inbound edges
        head_name = tail_name
        for reference in self.referenced_by:
            tail_name = "%s.%s" % (reference.sname, reference.tname)
            if tail_name not in seen:
                dot.node(tail_name, tail_name)
                seen.add(tail_name)
            dot.edge(tail_name, head_name)

        return dot

    @property
    def logical_plan(self):
        """The logical plan used to compute this relation; intended for internal use."""
        raise NotImplementedError()

    @property
    def c(self):
        """Shorthand for the column_definitions container."""
        return self.column_definitions

    def select(self, *columns):
        """Selects this relation and projects the columns.

        :param columns: optional positional arguments of columns to be projected, which may be given as Column objects
        of this relation, or as strings.
        :return a computed relation
        """
        if columns:
            projection = []
            for column in columns:
                if isinstance(column, Column):
                    projection.append(column.name)
                elif isinstance(column, str) or isinstance(column, _op.AttributeAlias):
                    projection.append(column)
                else:
                    raise ValueError("Unsupported projection type '{}'".format(type(column).__name__))
            return ComputedRelation(_op.Project(self.logical_plan, tuple(projection)))  # TODO: set 'table' param?
        else:
            return ComputedRelation(self.logical_plan)  # TODO: set 'table' param?

    def filter(self, formula):
        """Filters this relation according to the given formula.

        :param formula: a comparison
        :return: a computed relation
        """
        if not formula:
            raise ValueError('formula must not be None')
        elif not (isinstance(formula, _op.Comparison) or isinstance(formula, _op.Conjunction)):
            raise ValueError('formula must be an instance of Comparison or Conjunction')
        else:
            # TODO: next we want to support a conjunction of comparisons
            # TODO: allow input of comparison or conjunction of comparisons
            return ComputedRelation(_op.Select(self.logical_plan, formula))  # TODO: set 'table' param?

    def reify_sub(self, *cols):
        """Reifies a sub-concept of the relation by the specified columns. This relation is left unchanged.

        :param cols: a var arg list of Column objects
        :return a computed relation
        """
        if not all([isinstance(col, Column) for col in cols]):
            raise ValueError("All positional arguments must be of type Column")
        return ComputedRelation(_op.ReifySub(self.logical_plan, tuple([col.name for col in cols])))

    def reify(self, new_key_cols, new_other_cols):
        """Splits out a new relation based on this table, which will be comprised of the new_key_cols as its keys and
        the new_other_columns as the rest of its columns.

        The new_key_columns may not be keys in this table but they will be in the new relation. All columns in
        new_key_cols and new_other_cols must be present in this table.

        :param new_key_cols: a set of columns
        :param new_other_cols: a set of column
        :return a computed relation
        """
        if not all([isinstance(col, Column) for col in new_key_cols | new_other_cols]):
            raise ValueError("All items in the column arguments must be of type Column")
        if set(new_key_cols) & set(new_other_cols):
            raise ValueError("Key columns and Other columns must not overlap")
        return ComputedRelation(_op.Reify(self.logical_plan, tuple([col.name for col in new_key_cols]), tuple([col.name for col in new_other_cols])))


class ComputedRelation (AbstractTable):
    """Computed relation."""
    def __init__(self, logical_plan):
        # NOTE: this is a consistent way of computing the relational schema. It is expensive only if the source
        # source relations are NOT extants. That is, if the source relations are Tables from a Catalog, then the
        # expression will begin with an ErmScan physical operator which will efficiently return the description from
        # the extant table's `prejson()` method. However, if the source relation is not an extant, i.e., tabular,
        # JSON, or graph data read from a file, then this can be an expensive way of (re)computing the schema. This
        # expense could add up considerably when consider the API supported 'chaining' where the user may make several
        # transformative calls in a row. For now, this will be left alone as an unoptimized corner-case, which will be
        # revisited later. The real utility of pre-computing the schema here is only to populate the
        # `column_definitions` with `Column` objects that can be used in subsequent API calls. One potential future
        # solution here is to implement "unbound" `Column` objects and return them from the `column_definitions`. The
        # unbound columns will therefore only be fully resolved when the expression is finally executed.
        self._logical_plan = _op.logical_planner(logical_plan)
        self._physical_plan = _op.physical_planner(self._logical_plan)
        self._buffered_plan = operators.BufferedOperator(self._physical_plan)
        super(ComputedRelation, self).__init__(self._physical_plan.description)

    @property
    def logical_plan(self):
        """The logical plan used to compute this relation; intended for internal use."""
        return self._logical_plan

    @logical_plan.setter
    def logical_plan(self, value):
        self._logical_plan = value
        self._physical_plan = _op.physical_planner(self._logical_plan)
        # Don't bother to update the relation's description because it is assumed that the logical plan update is only
        # for optimization; one could assert that the current and new logical plans are 'logically' equivalent but this
        # check is not cheap to perform and therefore skipped at this time.

    @property
    def physical_plan(self):
        """The physical plan used to compute this relation; intended for internal use."""
        return self._buffered_plan


class Column (object):
    """Table column."""
    def __init__(self, table, column_doc):
        super(Column, self).__init__()
        self.table = table
        self.name = column_doc['name']
        self.type = column_doc['type']
        self.default = column_doc['default']
        self.comment = column_doc['comment']

    def __hash__(self):
        return super(Column, self).__hash__()

    def eq(self, other):
        return _op.Comparison(operand1=self.name, operator='=', operand2=str(other))

    __eq__ = eq

    def lt(self, other):
        return _op.Comparison(operand1=self.name, operator='<', operand2=str(other))

    __lt__ = lt

    def le(self, other):
        return _op.Comparison(operand1=self.name, operator='<=', operand2=str(other))

    __le__ = le

    def gt(self, other):
        return _op.Comparison(operand1=self.name, operator='>', operand2=str(other))

    __gt__ = gt

    def ge(self, other):
        return _op.Comparison(operand1=self.name, operator='>=', operand2=str(other))

    __ge__ = ge

    eq.__doc__ = lt.__doc__ = le.__doc__ = gt.__doc__ = ge.__doc__ = \
        """Creates and returns a comparison clause.

        :param other: assumes a literal; any allowed but `str(other)` will be used to cast its value to text
        :return: a symbolic comparison clause to be used in other statements
        """

    def alias(self, name):
        """Renames this column to the alias name.

        :param name: a new name for this column
        :return: a symbolic expression for the renamed column
        """
        return _op.AttributeAlias(self.name, name)

    def to_atoms(self, delim=',', unnest_fn=None):
        """Computes a new relation from the 'atomic' values of this column.

        The computed relation includes the minimal key columns and this column. The non-atomic values of this column are
        unnested either using the `unnest_fn` or it no unnest_fn is given then it creates a string unnesting function
        from the given `delim` delimiter character.

        :param delim: delimited character.
        :param unnest_fn: custom unnesting function must be callable on each value of this column in the relation.
        :return: a computed relation that can be assigned to a newly named table in the catalog.
        """
        if not unnest_fn:
            unnest_fn = util.splitter_fn(delim)
        elif not callable(unnest_fn):
            raise ValueError('unnest_fn is not callable')
        return ComputedRelation(_op.Atomize(self.table.logical_plan, unnest_fn, self.name))

    def to_domain(self, similarity_fn=util.edit_distance_fn, grouping_fn=None):
        """Computes a new 'domain' from this column.

        :param similarity_fn: a function for computing a similarity comparison between values in this column.
        :param grouping_fn: a function for computing candidate 'groups' to which the similarity function is used to
        determine the final groupings.
        :return: a computed relation that represents the new domain
        """
        return ComputedRelation(_op.Domainify(self.table.logical_plan, self.name, similarity_fn, grouping_fn))

    def to_vocabulary(self, similarity_fn=util.edit_distance_fn, grouping_fn=None):
        """Creates a canonical 'vocabulary' from this column.

        :param similarity_fn: a function for computing a similarity comparison between values in this column.
        :param grouping_fn: a function for computing candidate 'groups' to which the similarity function is used to
        determine the final groupings.
        :return: a computed relation that represents the new vocabulary
        """
        return ComputedRelation(_op.Canonicalize(self.table.logical_plan, self.name, similarity_fn, grouping_fn))

    def align(self, domain, similarity_fn=util.edit_distance_fn, grouping_fn=None):
        """Align this column with a given domain

        :param domain: a simple domain or a fully structured vocabulary
        :param similarity_fn: a function for computing a similarity comparison between values in this column.
        :param grouping_fn: a function for computing candidate 'groups' to which the similarity function is used to
        determine the final groupings.
        :return: a computed relation that represents the containing table with this attribute aligned to the domain
        """
        if not isinstance(domain, AbstractTable):
            raise ValueError("domain must be a table instance")

        return ComputedRelation(_op.Align(domain.logical_plan, self.table.logical_plan, self.name, similarity_fn, grouping_fn))

    def to_tags(self, domain, delim=',', unnest_fn=None, similarity_fn=util.edit_distance_fn, grouping_fn=None):
        """Computes a new relation from the unnested and aligned values of this column.

        :param domain: a simple domain or a fully structured vocabulary
        :param delim: delimited character.
        :param unnest_fn: custom unnesting function must be callable on each value of this column in the relation.
        :param similarity_fn: a function for computing a similarity comparison between values in this column.
        :param grouping_fn: a function for computing candidate 'groups' to which the similarity function is used to
        determine the final groupings.
        :return: a computed relation that can be assigned to a newly named table in the catalog.
        """
        if not isinstance(domain, AbstractTable):
            raise ValueError("domain must be a table instance")

        if not unnest_fn:
            unnest_fn = util.splitter_fn(delim)
        elif not callable(unnest_fn):
            raise ValueError('unnest_fn must be callable')

        return ComputedRelation(_op.Tagify(domain.logical_plan, self.table.logical_plan, self.name, unnest_fn, similarity_fn, grouping_fn))
