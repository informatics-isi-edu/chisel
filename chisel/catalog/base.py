"""Database catalog module."""

import abc
import collections
from functools import wraps
import itertools
import logging
import pprint as pp
from graphviz import Digraph
from deriva.core import ermrest_model as _em
from .. import optimizer as _op, operators, util

logger = logging.getLogger(__name__)

data_types = _em.builtin_types


def valid_model_object(fn):
    """Decorator for guarding against invocations on methods of deleted model objects."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        model_object = args[0]
        assert hasattr(model_object, 'valid'), "Decorated object does not have a valid flag"
        if not getattr(model_object, 'valid'):
            raise CatalogMutationError("The {model_type} object with name '{name}' was invalidated.".format(
                model_type=type(model_object).__name__, name=model_object.name))
        return fn(*args, **kwargs)
    return wrapper


class CatalogMutationError (Exception):
    """Indicates an error during catalog model mutation."""
    pass


class AbstractCatalog (object):
    """Abstract base class for catalogs."""

    def __init__(self, model_doc):
        super(AbstractCatalog, self).__init__()
        self._evolve_ctx = None
        self._model_doc = model_doc
        self._schemas = {sname: self._new_schema_instance(model_doc['schemas'][sname]) for sname in model_doc['schemas']}
        self._update_referenced_by()

    def _update_referenced_by(self):
        """Updates the 'referenced_by back pointers on the table model objects."""
        for schema in self.schemas.values():
            for referer in schema.tables.values():
                for fkey in referer.foreign_keys:
                    referenced = self.schemas[
                        fkey.referenced_columns[0]['schema_name']
                    ].tables[
                        fkey.referenced_columns[0]['table_name']
                    ]
                    referenced.referenced_by.append(fkey)

    @property
    def schemas(self):
        """Map of schema names to schema model objects."""
        return self._schemas

    def _new_schema_instance(self, schema_doc):
        """Overridable method for creating a new schema model object.

        :param schema_doc: the schema document
        :return: schema model object
        """
        return Schema(schema_doc, self)

    def __getitem__(self, item):
        """Maps a schema name to a schema model object.

        This is a short-hand for `catalog.schemas[schema_name]`.
        """
        return self.schemas[item]

    def _ipython_key_completions_(self):
        return self.schemas.keys()

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

    class _CatalogMutationContextManager (object):
        """A context manager (i.e., 'with' statement enter/exit) for catalog model mutation."""

        class _CatalogMutationAbort (Exception):
            pass

        def __init__(self, catalog, allow_alter, allow_drop, dry_run, consolidate):
            assert isinstance(catalog, AbstractCatalog), "'catalog' must be an 'AbstractCatalog'"
            self.parent = catalog
            self.allow_alter = allow_alter
            self.allow_drop = allow_drop
            self.dry_run = dry_run
            self.consolidate = consolidate

        def __enter__(self):
            if self.parent._evolve_ctx:
                raise CatalogMutationError('A catalog model mutation context already exists.')
            self.parent._evolve_ctx = self
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if (not self.parent._evolve_ctx) or (self.parent._evolve_ctx != self):
                raise CatalogMutationError("Attempting to exit a catalog mutation content not assigned to the parent catalog.")

            try:
                if exc_type:
                    logging.info("Exception caught during catalog mutation, cancelling the current mutation.")
                    self.parent._abort()
                    return isinstance(exc_val, self._CatalogMutationAbort)
                else:
                    logging.debug("Committing current catalog model mutation operations.")
                    self.parent._commit(self.dry_run, self.consolidate)
            finally:
                # resent the schemas
                for schema in self.parent.schemas.values():
                    schema.tables.reset()
                # remove the evolve context
                self.parent._evolve_ctx = None

        def abort(self):
            """Aborts a catalog mutation context by raising an exception to be handled on exit of current context."""
            raise self._CatalogMutationAbort()

    def evolve(self, allow_alter=False, allow_drop=False, dry_run=False, consolidate=True):
        """Begins a catalog model evolution block.

        This should be called in a `with` statement block. At the end of the block, the pending changes will be
        committed to the catalog. If an exception, any exception, is raised during the block the current mutations will
        be cancelled.

        Usage:
        ```
        # let `catalog` be a chisel catalog object
        with catalog.evolve:
            catalog['foo']['baz'] = catalog['foo']['bar'].select(...).where(...)

            ...perform other mutating operations

            # at the end of the block, the pending operations (above) will be performed
        ```

        Or optionally abort a catalog mutation:
        ```
        # let `catalog` be a chisel catalog object
        with catalog.evolve as context:
            catalog['foo']['baz'] = catalog['foo']['bar'].select(...).where(...)

            ...perform other mutating operations

            if oops_something_went_wrong_here:
                context.abort()

            ...anything else will be skipped, unless you catch the exception raised by the abort() method.

            # at the end of the block, the pending operations (above) will be aborted.
        ```

        :param allow_alter: if set to True, existing tables may be altered (default=False).
        :param allow_drop: if set to True, existing tables may be deleted (default=False).
        :param dry_run: if set to True, the pending commits will be drained, debug output printed, but not committed.
        :param consolidate: if set to True, attempt to consolidate shared work between pending operations.
        :return: a catalog model mutation context object for use in 'with' statements
        """
        if self._evolve_ctx:  # TODO: make this thread safe
            raise CatalogMutationError('A catalog mutation context already exists.')

        return self._CatalogMutationContextManager(self, allow_alter, allow_drop, dry_run, consolidate)

    @abc.abstractmethod
    def _materialize_relation(self, plan):
        """Materializes a relation from a physical plan.

        :param plan: a `PhysicalOperator` instance from which to materialize the relation
        :return: None
        """

    def _abort(self):
        """Abort pending catalog model mutations."""
        if not self._evolve_ctx:
            raise CatalogMutationError("No catalog mutation context set. This method should not be called directly")

    def _commit(self, dry_run=False, consolidate=True):
        """Commits pending computed relation assignments to the catalog.

        :param dry_run: if set to True, the pending commits will be drained, debug output printed, but not committed.
        :param consolidate: if set to True, attempt to consolidate shared work between pending operations.
        """
        if not self._evolve_ctx:
            raise CatalogMutationError("No catalog mutation context set. This method should not be called directly")

        # Find all pending assignment operations
        computed_relations = []
        for schema in self.schemas.values():
            for value in schema.tables.pending:
                assert isinstance(value, ComputedRelation)
                computed_relations.append(value)

        logger.info('Committing {num} pending computed relations'.format(num=len(computed_relations)))

        # Run the logical planner and update the computed relations
        for computed_relation in computed_relations:
            computed_relation.logical_plan = _op.logical_planner(computed_relation.logical_plan)

        # Consolidate the computed relations; i.e., identify and consolidate shared work
        if consolidate:
            _op.consolidate(computed_relations)

        # Process the pending operations
        for computed_relation in computed_relations:
            # compute the model changes
            model_changes = self._determine_model_changes(computed_relation)

            # relax model constraints, if/when necessary
            if not dry_run:
                self._relax_model_constraints(model_changes)

            # get its optimized and consolidated logical plan
            logical_plan = computed_relation.logical_plan

            # do physical planning
            physical_plan = _op.physical_planner(logical_plan)

            if dry_run:
                # TODO: change this to return an object than can be printed or displayed in ipython
                logger.info('Dry run: no changes to catalog will be performed.')
                print('Logical plan:')
                print(logical_plan)
                print('Physical plan:')
                print(physical_plan)
                print('Schema:')
                pp.pprint(physical_plan.description)
                print('Data:')
                pp.pprint(list(itertools.islice(physical_plan, 100)))
                print('Model changes:')
                pp.pprint(model_changes)
            else:
                # Materialize the planned relation
                logging.info('Materializing "{name}"...'.format(name=computed_relation.name))
                self._materialize_relation(physical_plan)

                # 'propagate' model changes
                self._apply_model_changes(model_changes)

        # revise state of catalog model objects
        self._revise_catalog_state()

    def _determine_model_changes(self, computed_relation):
        """Determines the model changes to be produced by this computed relation."""
        return dict(mappings=[], constraints=[], policies=[])

    def _relax_model_constraints(self, model_changes):
        """Relaxes model constraints in the prior conditions of the model changes."""
        pass  # Sub-classes of AbstractCatalog may implement model change methods (optional).

    def _apply_model_changes(self, model_changes):
        """Apply model changes in the post conditions of the model changes."""
        pass  # Sub-classes of AbstractCatalog may implement model change methods (optional).

    def _revise_catalog_state(self):
        """Revise the catalog model object state following model evolve commits"""
        pass  # Sub-classes of AbstractCatalog may implement model change methods (optional).


class Schema (object):
    """Represents a 'schema' (a.k.a., a namespace) in a database catalog."""

    def __init__(self, schema_doc, catalog):
        super(Schema, self).__init__()
        self._catalog = catalog
        self._name = schema_doc['schema_name']
        self._comment = schema_doc['comment']
        self._tables = TableCollection(
            self,
            {table_name: self._new_table_instance(schema_doc['tables'][table_name]) for table_name in schema_doc['tables']}
        )
        self._valid = True

    @property
    def catalog(self):
        return self._catalog

    @property
    def name(self):
        return self._name

    @property
    def comment(self):
        return self._comment

    @property
    def tables(self):
        return self._tables

    @property
    def valid(self):
        return self._valid

    @valid.setter
    def valid(self, value):
        raise NotImplementedError('Invalidation of schemas not supported')

    def __getitem__(self, item):
        """Maps a table name to a table model object.

        This is a short-hand for `schema.tables[table_name]`.
        """
        return self.tables[item]

    def __setitem__(self, key, value):
        """Maps a table name to a table model object.

        This is a short-hand for `schema.tables[table_name] = table_object`.
        """
        self.tables[key] = value

    def _ipython_key_completions_(self):
        return list(self.tables.keys())

    def _new_table_instance(self, table_doc):
        """Overridable method for creating a new table model object.

        :param table_doc: the table document
        :return: table model object
        """
        return Table(table_doc)

    @valid_model_object
    def _create_table(self, table_doc):
        """Creates a table _in the catalog_.

        This method should be implemented by subclasses that allow for creating tables in extant schemas.

        :param table_doc: a table definition dictionary as defined by `Table.define(...)`.
        :return: a Table object representing the newly created table.
        """
        # TODO: refactor this into a form of generalized projection
        raise NotImplementedError()

    @valid_model_object
    def _drop_table(self, table_name):
        """Drops the table.

        :param table_name: name of the table to be dropped.
        """
        with self.catalog.evolve(allow_drop=True):
            self._tables._pending[table_name] = ComputedRelation(_op.Assign(_op.Nil(), self._name, table_name))

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


class TableCollection (collections.abc.MutableMapping):
    """Container class for schema tables (for internal use only).

    This class mostly passes through container methods to the underlying tables container. Its purpose is to facilitate
    assignment of new, computed relations to the catalog.
    """

    def __init__(self, schema, tables):
        """A collection of schema tables.

        :param schema: the parent schema
        :param tables: the original tables collection, which must be a mapping
        """
        super(TableCollection, self).__init__()
        self._schema = schema
        self._backup = tables
        self._tables = tables.copy()
        self._pending = {}
        self._destructive_pending = False

    def _ipython_key_completions_(self):
        return self._tables.keys()

    @property
    def valid(self):
        return self._schema.valid

    @property
    def pending(self):
        """List of 'pending' assignments to this schema."""
        return self._pending.values()

    def reset(self):
        """Resets the pending assignments to this schema."""
        self._tables = self._backup.copy()
        self._pending = {}
        self._destructive_pending = False

    def __str__(self):
        return str(self._tables)

    def __getitem__(self, item):
        return self._tables[item]

    @valid_model_object
    def __setitem__(self, key, value):
        # for directly creating a table...
        if not self._schema.catalog._evolve_ctx:
            if not isinstance(value, collections.abc.Mapping):
                raise CatalogMutationError("No catalog mutation context set.")
            if 'table_name' not in value:
                raise ValueError('value must have a "table_name" key in it')
            if value['table_name'] != key:
                raise ValueError('table definition "table_name" field does not match "%s"' % key)

            table = self._schema._create_table(value)
            assert isinstance(table, Table), "invalid table return type"
            self._backup[key] = table
            self.reset()
            return table

        # for evolution based on computed relations...
        if not isinstance(value, ComputedRelation):
            raise ValueError("Value must be a computed relation.")
        if self._destructive_pending:
            raise CatalogMutationError("A destructive operation is pending.")
        if key in self._tables:
            # 'key in tables' indicates that a table is being altered or replaced - a 'destructive' operation
            if self._pending:
                raise CatalogMutationError("A destructive operation is pending.")
            self._destructive_pending = True

        # update pending and current tables and return value
        # TODO: pending should be tracked in the evolve_ctx, in order, and then processed in order
        newval = ComputedRelation(_op.Assign(value.logical_plan, self._schema.name, key))
        self._tables[key] = self._pending[key] = newval
        assert self._tables[key] == self._pending[key]
        return newval

    @valid_model_object
    def __delitem__(self, key):
        self._schema._drop_table(key)

    def __iter__(self):
        return iter(self._tables)

    def __len__(self):
        return self._tables


class Table (object):
    """Abstract base class for tables."""

    def __init__(self, table_doc, schema=None):
        super(Table, self).__init__()
        self._table_doc = table_doc
        self._schema = schema
        self._name = table_doc['table_name']
        self._comment = table_doc.get('comment', None)
        self._sname = table_doc.get('schema_name', '')  # not present in computed relation
        self._kind = table_doc.get('kind')  # not present in computed relation
        self._columns = ColumnCollection(
            self, [(col['name'], self._new_column_instance(col)) for col in table_doc.get('column_definitions', [])]
        )
        self._keys = [_em.Key(self.sname, self.name, key_doc) for key_doc in table_doc.get('keys', [])]
        self._foreign_keys = [_em.ForeignKey(self.sname, self.name, fkey_doc) for fkey_doc in table_doc.get('foreign_keys', [])]
        self._referenced_by = []
        self._valid = True

    @classmethod
    def define(cls, tname, column_defs=[], key_defs=[], fkey_defs=[], comment=None, acls={}, acl_bindings={}, annotations={}):
        """Define a table.

        Currently, this is a thin wrapper on `deriva.core.ermrest_model.Table.define`.

        :param tname: table name string
        :param column_defs: optional list of column definitions returned by `Column.define()`
        :param key_defs: optional list of key definitions returned by `Key.define()`
        :param fkey_defs: optional list of foreign key definitions returned by `ForeignKey.define()`
        :param comment: optional comment string
        :param acls: optional dictionary of Access Control Lists
        :param acl_bindings: optional dictionary of Access Control List bindings
        :param annotations: optional dictionary of model annotations
        :return: a table definition dictionary
        """
        return _em.Table.define(tname, column_defs=column_defs, key_defs=key_defs, fkey_defs=fkey_defs, comment=comment, acls={}, acl_bindings=acl_bindings, annotations=annotations, provide_system=False)

    @property
    def schema(self):
        return self._schema

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if self.name == value:
            raise ValueError('The table is already named "%s"' % value)
        self._move(self.sname, value)

    @valid_model_object
    def _move(self, dst_schema_name, dst_table_name):
        """An internal method to 'move' a table either to rename it, change its schema, or both.

        :param dst_schema_name: destination schema name, may be same
        :param dst_table_name: destination table name, may be same
        """
        assert self.sname != dst_schema_name or self.name != dst_table_name
        catalog = self.schema.catalog
        with self.schema.catalog.evolve():
            # copy table to destination
            catalog.schemas[dst_schema_name].tables[dst_table_name] = self.select()
        # drop table from origin
        del catalog.schemas[self.sname].tables[self.name]
        self.valid = False  # TODO: could attempt to repair this table object

    @property
    def comment(self):
        return self._comment

    @property
    def sname(self):
        return self._sname

    @sname.setter
    def sname(self, value):
        if self.sname == value:
            raise ValueError('The schema is already set to "%s"' % value)
        self._move(value, self.name)

    @property
    def kind(self):
        return self._kind

    @property
    def columns(self):
        return self._columns

    @property
    def keys(self):
        return self._keys

    @property
    def foreign_keys(self):
        return self._foreign_keys

    @property
    def valid(self):
        return self._valid

    @valid.setter
    def valid(self, value):
        if not isinstance(value, bool):
            raise ValueError('value must be bool')
        if self._valid and not value:
            self._valid = value
            # invalidate all child model objects
            for column in self.columns.values():
                column.valid = False

    @property
    def referenced_by(self):
        return self._referenced_by

    @property
    @abc.abstractmethod
    def logical_plan(self):
        """The logical plan used to compute this relation; intended for internal use."""

    def _refresh(self):
        """Refreshes the internal state of this table object.

        A shallow version of this is provided by the Table class, but
        it should be overridden by subclass implementations that are capable
        of a deep refresh.
        """
        self.columns._refresh()

    @valid_model_object
    def fetch(self):
        """Returns an iterator for data for this relation."""
        return _op.physical_planner(_op.logical_planner(self.logical_plan))

    def _new_column_instance(self, column_doc):
        """Overridable method for creating a new column model object.

        :param column_doc: the column document
        :return: column model object
        """
        return Column(column_doc, self)

    @valid_model_object
    def _add_column(self, column_doc):
        """Adds a column to this relation _in the catalog_.

        This method should be implemented by subclasses that allow for adding columns to extant tables.

        :param column_doc: a column definition dictionary as defined by `Column.define(...)`.
        :return: a Column object representing the newly added column definition in the relation.
        """
        # TODO: refactor this into a form of generalized projection
        raise NotImplementedError()

    @valid_model_object
    def _drop_column(self, column_name):
        """Drops a column of this relation.

        :param column_name: the name of the column to be dropped.
        """
        column = self.columns[column_name]
        with self.schema.catalog.evolve(allow_alter=True):
            self.schema[self._name] = self.select(column.inv())

    def __getitem__(self, item):
        """Maps a column name to a column model object.

        This is a short-hand for `table.columns[column_name]`.
        """
        return self.columns[item]

    def _ipython_key_completions_(self):
        return self.columns.keys()
        # return list(self.columns.keys())

    def prejson(self):
        """Returns a JSON-ready representation of this table model object.

        :return: a JSON-ready representation of this table model object
        """
        return self._table_doc

    @valid_model_object
    def describe(self):
        """Returns a text (markdown) description."""
        def type2str(t):
            return t['typename']

        def _make_markdown_repr(quote=lambda s: s):
            data = [
                ["Column", "Type", "Nullable", "Default", "Comment"]
            ] + [
                [col.name, type2str(col.type), str(col.nullok), col.default, col.comment] for col in self.columns.values()
            ]
            desc = "### Table \"" + str(self.sname) + "." + str(self.name) + "\"\n" + \
                   util.markdown_table(data, quote)
            return desc

        class Description:
            def _repr_markdown_(self):
                return _make_markdown_repr(quote=util.markdown_quote)

            def __repr__(self):
                return _make_markdown_repr()

        return Description()

    @valid_model_object
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
        for reference in self._referenced_by:
            tail_name = "%s.%s" % (reference.sname, reference.tname)
            if tail_name not in seen:
                dot.node(tail_name, tail_name)
                seen.add(tail_name)
            dot.edge(tail_name, head_name)

        return dot

    @valid_model_object
    def copy(self, table_name, schema_name=None):
        """Makes a copy of this table.

        This operation must be performed in isolation of other evolve operations. It will setup the evolve block
        internally.

        :param table_name: the table copy will be given this name
        :param schema_name: the table copy will be created in this schema; if None, then it will be copied to the same
                            schema as this table.
        """
        schema = self.schema.catalog.schemas[schema_name] if schema_name else self.schema
        with schema.catalog.evolve():
            schema.tables[table_name] = self.select()

    @valid_model_object
    def select(self, *columns):
        """Selects this relation and projects the columns.

        :param columns: optional positional arguments of columns to be projected, which may be given as Column objects
        of this relation, or as strings.
        :return a computed relation
        """
        if columns:
            projection = []

            # validation: projection may be column, column name, alias, or removal
            for column in columns:
                if isinstance(column, Column):
                    projection.append(column.name)
                elif isinstance(column, str) or isinstance(column, _op.AttributeAlias) or isinstance(column, _op.AttributeRemoval):
                    projection.append(column)
                else:
                    raise ValueError("Unsupported projection type '{}'".format(type(column).__name__))

            # validation: if any removal, all must be removals (can't mix removals with other projections)
            removals = [isinstance(o, _op.AttributeRemoval) for o in projection]
            if any(removals):
                if not all(removals):
                    raise ValueError("Attribute removal cannot be mixed with other attribute projections")
                projection = [_op.AllAttributes()] + projection

            return ComputedRelation(_op.Project(self.logical_plan, tuple(projection)))
        else:
            projection = [cname for cname in self.columns]
            return ComputedRelation(_op.Project(self.logical_plan, tuple(projection)))

    @valid_model_object
    def where(self, formula):
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
            return ComputedRelation(_op.Select(self.logical_plan, formula))

    @valid_model_object
    def reify_sub(self, *cols):
        """Reifies a sub-concept of the relation by the specified columns. This relation is left unchanged.

        :param cols: a var arg list of Column objects
        :return a computed relation
        """
        if not all([isinstance(col, Column) for col in cols]):
            raise ValueError("All positional arguments must be of type Column")
        return ComputedRelation(_op.ReifySub(self.logical_plan, tuple([col.name for col in cols])))

    @valid_model_object
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


class ColumnCollection (collections.OrderedDict):
    """An OrderedDict sub-class for managing table columns."""

    def __init__(self, table, items):
        super(ColumnCollection, self).__init__()
        assert isinstance(table, Table)
        assert items is None or hasattr(items, '__iter__')
        # bypass our overridden setter
        for item in items:
            super(ColumnCollection, self).__setitem__(item[0], item[1])
        self._table = table

    @property
    def valid(self):
        return self._table.valid

    @valid_model_object
    def __delitem__(self, key):
        # get handle to the column
        column = self[key]
        # drop column from catalog model
        self._table._drop_column(key)
        # invalidate model object
        column.valid = False
        # delete from column collection
        super(ColumnCollection, self).__delitem__(key)

    @valid_model_object
    def __setitem__(self, key, value):
        if not isinstance(value, collections.abc.Mapping):
            raise ValueError('value must be a mapping object')
        if 'name' not in value:
            raise ValueError('value must have a "name" key in it')
        if 'type' not in value:
            raise ValueError('value must have a "type" key in it')
        if value['name'] != key:
            raise ValueError('column definition "name" field does not match "%s"' % key)

        column = self._table._add_column(value)
        assert isinstance(column, Column), "invalid column return type"
        super().__setitem__(key, column)

    def _refresh(self):
        """Refreshes the collection indices to repair them after a column rename."""
        columns = list(self.values())
        self.clear()
        for col in columns:
            assert isinstance(col, Column)
            super(ColumnCollection, self).__setitem__(col.name, col)


class ComputedRelation (Table):
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
        self._logical_plan = logical_plan
        self._buffered_plan = operators.BufferedOperator(_op.physical_planner(_op.logical_planner(logical_plan)))
        super(ComputedRelation, self).__init__(self._buffered_plan.description)

    @property
    def logical_plan(self):
        """The logical plan used to compute this relation; intended for internal use."""
        return self._logical_plan

    @logical_plan.setter
    def logical_plan(self, value):
        # Don't bother to update the relation's description because it is assumed that the logical plan update is only
        # for optimization; one could assert that the current and new logical plans are 'logically' equivalent but this
        # check is not cheap to perform and therefore skipped at this time.
        self._logical_plan = value
        self._buffered_plan = None

    def fetch(self):
        """Returns an iterator for data for this relation."""
        if not self._buffered_plan:
            self._buffered_plan = operators.BufferedOperator(_op.physical_planner(_op.logical_planner(self._logical_plan)))
        return self._buffered_plan


class Column (object):
    """Table column."""

    def __init__(self, column_doc, table):
        super(Column, self).__init__()
        self.table = table
        self._name = column_doc['name']
        self._type = column_doc['type']
        self._default = column_doc['default']
        self._nullok = column_doc['nullok']
        self._comment = column_doc['comment']
        self._valid = True

    @classmethod
    def define(cls, cname, ctype, nullok=True, default=None, comment=None, acls={}, acl_bindings={}, annotations={}):
        """Define a column.

        Currently, this is a thin wrapper over `deriva.core.ermrest_model.Column.define`.

        :param cname: column name string
        :param ctype: column type object from `chisel.data_types`
        :param nullok: optional NULL OK boolean flag (default: `True`)
        :param default: optional "default" literal value of same time as `ctype` (default: `None`)
        :param comment: optional comment string
        :param acls: optional dictionary of Access Control Lists
        :param acl_bindings: optional dictionary of Access Control List bindings
        :param annotations: optional dictionary of model annotations
        :return: a column definition dictionary
        """
        return _em.Column.define(cname, ctype, nullok, default, comment, acls, acl_bindings, annotations)

    def __hash__(self):
        return super(Column, self).__hash__()

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._rename(value)

    @property
    def valid(self):
        return self._valid

    @valid.setter
    def valid(self, value):
        self._valid = value

    @property
    def type(self):
        return self._type

    @property
    def default(self):
        return self._default

    @property
    def nullok(self):
        return self._nullok

    @property
    def comment(self):
        return self._comment

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

    eq.__doc__ = \
    lt.__doc__ = \
    le.__doc__ = \
    gt.__doc__ = \
    ge.__doc__ = \
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

    def inv(self):
        """Removes an attribute when used in a projection.

        :return: a sybolic expression for the removed column
        """
        return _op.AttributeRemoval(self.name)

    __invert__ = inv

    @valid_model_object
    def _rename(self, new_name):
        """Renames the column.

        This method cannot be called within another 'evolve' block. It must be performed in isolation.

        :param new_name: new name for the column
        """
        with self.table.schema.catalog.evolve(allow_alter=True):
            projection = []
            for cname in self.table.columns:
                if cname == self.name:
                    projection.append(self.alias(new_name))
                else:
                    projection.append(cname)
            self.table.schema[self.table.name] = self.table.select(*projection)

        # update local copy of name
        self._name = new_name
        # "refresh" the containing table
        self.table._refresh()

    @valid_model_object
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

    @valid_model_object
    def to_domain(self, similarity_fn=util.edit_distance_fn, grouping_fn=None):
        """Computes a new 'domain' from this column.

        :param similarity_fn: a function for computing a similarity comparison between values in this column.
        :param grouping_fn: a function for computing candidate 'groups' to which the similarity function is used to
        determine the final groupings.
        :return: a computed relation that represents the new domain
        """
        return ComputedRelation(_op.Domainify(self.table.logical_plan, self.name, similarity_fn, grouping_fn))

    @valid_model_object
    def to_vocabulary(self, similarity_fn=util.edit_distance_fn, grouping_fn=None):
        """Creates a canonical 'vocabulary' from this column.

        :param similarity_fn: a function for computing a similarity comparison between values in this column.
        :param grouping_fn: a function for computing candidate 'groups' to which the similarity function is used to
        determine the final groupings.
        :return: a computed relation that represents the new vocabulary
        """
        return ComputedRelation(_op.Canonicalize(self.table.logical_plan, self.name, similarity_fn, grouping_fn))

    @valid_model_object
    def align(self, domain, similarity_fn=util.edit_distance_fn, grouping_fn=None):
        """Align this column with a given domain

        :param domain: a simple domain or a fully structured vocabulary
        :param similarity_fn: a function for computing a similarity comparison between values in this column.
        :param grouping_fn: a function for computing candidate 'groups' to which the similarity function is used to
        determine the final groupings.
        :return: a computed relation that represents the containing table with this attribute aligned to the domain
        """
        if not isinstance(domain, Table):
            raise ValueError("domain must be a table instance")

        return ComputedRelation(_op.Align(domain.logical_plan, self.table.logical_plan, self.name, similarity_fn, grouping_fn))

    @valid_model_object
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
        if not isinstance(domain, Table):
            raise ValueError("domain must be a table instance")

        if not unnest_fn:
            unnest_fn = util.splitter_fn(delim)
        elif not callable(unnest_fn):
            raise ValueError('unnest_fn must be callable')

        return ComputedRelation(_op.Tagify(domain.logical_plan, self.table.logical_plan, self.name, unnest_fn, similarity_fn, grouping_fn))
