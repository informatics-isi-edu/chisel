"""Database catalog module."""

import abc
import collections
from functools import wraps
import itertools
import json
import logging
import pprint as pp
from deriva.core import ermrest_model as _em
from ..util import describe, deprecated, graph

from .. import optimizer as _op, operators, util

logger = logging.getLogger(__name__)

"""Chisel data types, based on deriva.core.ermrest_model.builtin_types."""
data_types = _em.builtin_types


def valid_model_object(fn):
    """Decorator for guarding against invocations on methods of deleted model objects."""
    @wraps(fn)
    def wrapper(*args, **kwargs):
        model_object = args[0]
        assert hasattr(model_object, 'valid'), "Decorated object does not have 'valid' attribute"
        if not getattr(model_object, 'valid'):
            raise CatalogMutationError("The %s object was invalidated." % type(model_object).__name__)
        return fn(*args, **kwargs)
    return wrapper


class CatalogMutationError (Exception):
    """Indicates an error during catalog model mutation."""
    pass


class AbstractCatalog (object):
    """Abstract base class for catalogs.

    Properties `allow_alter_default` and `allow_drop_default` (defaults `True`) are passed to the
    catalog `evolve(...)` method when evolution operations are performed without first establishing
    an evolve block.
    """

    def __init__(self, model_doc):
        super(AbstractCatalog, self).__init__()
        self._evolve_ctx = None
        self.allow_alter_default = True
        self.allow_drop_default = True
        self._model_doc = model_doc
        self._schemas = {sname: self._new_schema_instance(self._model_doc['schemas'][sname]) for sname in self._model_doc['schemas']}
        self._update_referenced_by()

    def _update_referenced_by(self):
        """Updates the 'referenced_by back pointers on the table model objects."""
        for schema in self.schemas.values():
            for referer in schema.tables.values():
                for fkey in referer.foreign_keys:
                    referenced = self.schemas[
                        fkey['referenced_columns'][0]['schema_name']
                    ].tables[
                        fkey['referenced_columns'][0]['table_name']
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

    @deprecated
    def describe(self):
        """Returns a text (markdown) description."""
        return describe(self)

    @deprecated
    def graph(self, engine='fdp'):
        """Generates and returns a graphviz Digraph.

        :param engine: text name for the graphviz engine (dot, neato, circo, etc.)
        :return: a Graph object that can be rendered directly by jupyter notbook or qtconsole
        """
        return graph(self, engine=engine)

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
                # reset the schemas
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
        if self._evolve_ctx:
            raise CatalogMutationError('A catalog mutation context already exists.')

        return self._CatalogMutationContextManager(self, allow_alter, allow_drop, dry_run, consolidate)

    @abc.abstractmethod
    def _materialize_relation(self, plan):
        """Materializes a relation from a physical plan.

        :param plan: a `PhysicalOperator` instance from which to materialize the relation
        :return: None
        """

    @abc.abstractmethod
    def logical_plan(self, table):
        """Symbolic representation of the table extant; intended for internal use.

        :param table: a table instance
        :return: a symbolic logical plan representing the extant table
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
        return Table(table_doc, schema=self)

    @deprecated
    def describe(self):
        """Returns a text (markdown) description."""
        return describe(self)

    @deprecated
    def graph(self, engine='fdp'):
        """Generates and returns a graphviz Digraph.

        :param engine: text name for the graphviz engine (dot, neato, circo, etc.)
        :return: a Graph object that can be rendered directly by jupyter notbook or qtconsole
        """
        return graph(self, engine=engine)

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
        self._pending = {}  # TODO: pending should be tracked in the catalog, in order, and then processed in order
        self._destructive_pending = False  # TODO: should be tracked in the catalog

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
        if not self._schema.catalog._evolve_ctx:
            with self._schema.catalog.evolve(allow_alter=self._schema.catalog.allow_alter_default):
                t = self._do_assign(key, value)
            return t
        else:
            return self._do_assign(key, value)

    def _do_assign(self, key, value):
        if isinstance(value, collections.abc.Mapping):
            # for new table creation, we expect to get a Mapping (dict)

            # validate that table definition has minimum required field
            if 'table_name' not in value:
                raise ValueError('value must have a "table_name" key in it')

            # validate that the name has not been assigned
            if key in self._tables:
                raise ValueError('there is already a table named "%s"' % key)

            # validate that table_name and key are the same
            if value['table_name'] != key:
                raise ValueError('table definition "table_name" field does not match "%s"' % key)

            # assign new table definition to the pending operations
            newval = ComputedRelation(_op.Assign(json.dumps(value), self._schema.name, key))
            self._tables[key] = self._pending[key] = newval
            return newval

        else:
            # for evolution based on computed relations...

            # validate that value is a computed relation
            if not isinstance(value, ComputedRelation):
                raise ValueError("Value must be a computed relation or a new table definition.")

            # validate that no destructive (alter/drop) operations are pending
            if self._destructive_pending:
                raise CatalogMutationError("Cannot perform another operation while a 'mutation' of an existing table is pending.")

            if key in self._tables:
                # 'key in tables' indicates that this table is being altered or replaced - a 'destructive' operation
                if self._pending:
                    raise CatalogMutationError("Cannot perform 'mutation' of an existing table while another operation is pending.")
                self._destructive_pending = True

            # update pending and current tables and return value
            newval = ComputedRelation(_op.Assign(value.logical_plan, self._schema.name, key))
            self._tables[key] = self._pending[key] = newval
            return newval

    @valid_model_object
    def __delitem__(self, key):
        if not self._schema.catalog._evolve_ctx:
            with self._schema.catalog.evolve(allow_drop=self._schema.catalog.allow_drop_default):
                t = self._do_delete(key)
            return t
        else:
            return self._do_delete(key)

    def _do_delete(self, key):
        if self._pending:
            raise CatalogMutationError("Cannot perform 'mutation' of an existing table while another operation is pending.")
        self._destructive_pending = True
        self._schema._tables._pending[key] = ComputedRelation(_op.Assign(_op.Nil(), self._schema._name, key))

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
        # TODO: eventually these need chisel model objects
        # self._keys = [_em.Key(self.schema.name, self.name, key_doc) for key_doc in table_doc.get('keys', [])]
        self._keys = [key_doc for key_doc in table_doc.get('keys', [])]
        # TODO: eventually these need chisel model objects
        # self._foreign_keys = [_em.ForeignKey(self.schema.name, self.name, fkey_doc) for fkey_doc in table_doc.get('foreign_keys', [])]
        self._foreign_keys = [fkey_doc for fkey_doc in table_doc.get('foreign_keys', [])]
        self._referenced_by = []
        self._valid = True

    @classmethod
    def define(cls, tname, column_defs=[], key_defs=[], fkey_defs=[], comment=None, acls={}, acl_bindings={}, annotations={}, provide_system=True):
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
        :param provide_system: whether to inject standard system column definitions when missing from column_defs
        :return: a table definition dictionary
        """
        return _em.Table.define(tname, column_defs=column_defs, key_defs=key_defs, fkey_defs=fkey_defs, comment=comment, acls=acls, acl_bindings=acl_bindings, annotations=annotations, provide_system=provide_system)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if self.name == value:
            raise ValueError('The table is already named "%s"' % value)
        if value in self._schema.tables:
            raise ValueError('A table by the name "%s" already exists in the schema' % value)
        self._schema.tables[value] = ComputedRelation(_op.Rename(self.logical_plan, tuple()))

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        if self._schema == value:
            raise ValueError('The table is already in "%s" schema' % value)
        if value.catalog != self._schema.catalog:
            raise ValueError('The new schema does not belong to the same catalog')
        if self._name in value.tables:
            raise ValueError('A table by the name "%s" already exists in the "%s" schema' % (self._name, value.name))
        value.tables[self._name] = ComputedRelation(_op.Rename(self.logical_plan, tuple()))

    @property
    def comment(self):
        return self._comment

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
    def logical_plan(self):
        """The logical plan used to compute this relation; intended for internal use."""
        assert self._schema, "Cannot return logical plan extant because Table.schema was not initialized."
        return self._schema.catalog.logical_plan(self)

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

    @deprecated
    def describe(self):
        """Returns a text (markdown) description."""
        return describe(self)

    @deprecated
    def graph(self, engine='fdp'):
        """Generates and returns a graphviz Digraph.

        :param engine: text name for the graphviz engine (dot, neato, circo, etc.)
        :return: a Graph object that can be rendered directly by jupyter notbook or qtconsole
        """
        return graph(self, engine=engine)

    @valid_model_object
    def clone(self):
        """Clone this table.
        """
        return self.select()

    @valid_model_object
    def select(self, *columns):
        """Selects this relation and projects the columns.

        :param columns: optional positional arguments of columns to be projected, which may be given as Column objects
        of this relation, or as strings.
        :return a computed relation
        """
        if columns:
            projection = []

            # validation: projection may be column, column name, alias, addition or removal
            for column in columns:
                if isinstance(column, Column):
                    projection.append(column.name)
                elif isinstance(column, str) or isinstance(column, _op.AttributeAlias)\
                        or isinstance(column, _op.AttributeDrop) or isinstance(column, _op.AttributeAdd):
                    projection.append(column)
                else:
                    raise ValueError("Unsupported projection type '{}'".format(type(column).__name__))

            # validation: if any mutation (add/drop), all must be mutations (can't mix with other projections)
            for mutation in (_op.AttributeAdd, _op.AttributeDrop):
                mutations = [isinstance(o, mutation) for o in projection]
                if any(mutations):
                    if not all(mutations):
                        raise ValueError("Attribute add/drop cannot be mixed with other attribute projections")
                    projection = [_op.AllAttributes()] + projection

            return ComputedRelation(_op.Project(self.logical_plan, tuple(projection)))
        else:
            projection = [cname for cname in self.columns]
            return ComputedRelation(_op.Project(self.logical_plan, tuple(projection)))

    @valid_model_object
    def join(self, right):
        """Joins this relation and the 'right' relation.

        :param right: relation to be joined.
        :return a computed relation
        """
        if not isinstance(right, Table):
            raise ValueError('Object to the right of the join is not an instance of "Table"')

        return ComputedRelation(_op.Join(self.logical_plan, right.logical_plan))

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
    def union(self, other):
        """Unions this relation and the 'other' relation.

        :param other: relation to be combined with this relation with union semantics.
        :return a computed relation
        """
        if not isinstance(other, Table):
            raise ValueError('Other relation is not an instance of "Table"')

        return ComputedRelation(_op.Union(self.logical_plan, other.logical_plan))

    __add__ = union

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

    @valid_model_object
    def link(self, target):
        """Creates a reference from this table to the target table."""
        # TODO: may need to introduce new Link operator
        #       projection of table +column(s) needed as the foriegn key, inference of key columns from target table
        #       add'l physical operation to add the fkey reference
        raise NotImplementedError('This method is not yet supported.')

    @valid_model_object
    def associate(self, target):
        """Creates a many-to-many "association" between this table and "target" table."""
        # TODO: may need to introduce new Associate operator
        #       project of new table w/ column(s) for each foriegn key to inferred keys of target tables
        #       add'l physical operation to add the fkey reference(s)
        raise NotImplementedError('This method is not yet supported.')


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
    def __getitem__(self, key):
        return super(ColumnCollection, self).__getitem__(key)

    @valid_model_object
    def __delitem__(self, key):
        # get handle to the column
        column = self[key]
        # assign a projection of parent table without this column
        self._table.schema[self._table.name] = self._table.select(column.inv())
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
        if super().__contains__(key):
            raise ValueError('"%s" column already exists in table' % key)

        self._table.schema[self._table.name] = self._table.select(_op.AttributeAdd(definition=json.dumps(value)))
        self._table.valid = False

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

    # set type property:
    #   if type == value: ignore
    #   else: create an alter column op with diff value

    @property
    def default(self):
        return self._default

    # set default property:
    #   if default == value: ignore
    #   else: create an alter column op with diff value

    @property
    def nullok(self):
        return self._nullok

    # set nullok property:
    #   if nullok == value: ignore
    #   else: create an alter column op with diff value

    @property
    def comment(self):
        return self._comment

    # set comment property:
    #   if comment == value: ignore
    #   else: create an alter column op with diff value

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

        :return: a symbolic expression for the removed column
        """
        return _op.AttributeDrop(self.name)

    __invert__ = inv

    @valid_model_object
    def _rename(self, new_name):
        """Renames the column.

        :param new_name: new name for the column
        """
        # project out all columns with this column aliased
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
        self.table._refresh()  # TODO: this should work as a temporary measure until the evolve block is committed or aborted

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


class Key (object):
    """Key constraint class."""

    @classmethod
    def define(cls, colnames, constrain_name=None, comment=None, annotations={}):
        """Define a key.

        Currently, this is a thin wrapper around `deriva.core.ermrest_model.Key.define`.

        :param colnames: list of column name strings (must exist in the table on which the key is defined)
        :param constrain_name: optional constraint name string, i.e., name of this key (default: system generated)
        :param comment: optional comment
        :param annotations: optional model annotations
        :return: a key definition dictionary
        """
        return _em.Key.define(
            colnames,
            constraint_names=[constrain_name] if constrain_name else [],
            comment=comment,
            annotations=annotations
        )


class ForeignKey (object):
    """Foreign Key class."""

    NO_ACTION = 'NO ACTION'
    RESTRICT = 'RESTRICT'
    CASCADE = 'CASCADE'
    SET_NULL = 'SET NULL'
    SET_DEFAULT = 'SET DEFAULT'

    @classmethod
    def define(cls, fk_colnames, pk_sname, pk_tname, pk_colnames, constraint_name=None, comment=None,
               on_update=NO_ACTION, on_delete=NO_ACTION, acls={}, acl_bindings={}, annotations={}):
        """Define a foreign key.

        Currently, this is a thin wrapper around `deriva.core.ermrest_model.ForeignKey.define`.

        :param fk_colnames: list of foreign key column name strings (must exist in the table on which the fkey is defined)
        :param pk_sname: schema name of the referenced table
        :param pk_tname: table name of the referenced table
        :param pk_colnames: list of key column name strings (i.e., columns of the key being referenced)
        :param constraint_name: optional constraint name string, i.e., name of this foreign key (default: system generated)
        :param comment: optional comment
        :param on_update: optional update action (default:`ForeignKey.NO_ACTION`)
        :param on_delete: optional delete action (default:`ForeignKey.NO_ACTION`)
        :param acls: optional dictionary of Access Control Lists
        :param acl_bindings: optional dictionary of Access Control List bindings
        :param annotations: optional dictionary of model annotations
        :return: a foreign key definition dictionary
        """
        return _em.ForeignKey.define(
            fk_colnames,
            pk_sname, pk_tname, pk_colnames,
            on_update=on_update, on_delete=on_delete,
            constraint_names=[constraint_name] if constraint_name else [],
            comment=comment,
            acls=acls, acl_bindings=acl_bindings,
            annotations=annotations
        )
