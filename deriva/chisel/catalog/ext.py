"""Extended catalog model classes.
"""
import itertools
import logging
from pprint import pformat, pprint
from deriva.core import ermrest_model as _erm
from . import model
from .stubs import CatalogStub
from ..optimizer import symbols, planner, logical_planner, physical_planner, consolidate
from ..operators import Assign
from .. import util

logger = logging.getLogger(__name__)


class Model (model.Model):
    """Catalog model.
    """

    provide_system = True
    update_default_vizfkeys_on_commit = False  # experimental feature

    def __init__(self, catalog):
        """Initializes the model.

        :param catalog: ErmrestCatalog or catalog-like object
        """
        super(Model, self).__init__(catalog)
        self._new_schema = lambda obj: Schema(self, obj)
        self._new_fkey = lambda obj: ForeignKey(self.schemas[obj.table.schema.name].tables[obj.table.name], obj)

    def make_extant_symbol(self, schema_name, table_name):
        """Makes a symbol for representing an extant relation.

        :param schema_name: schema name
        :param table_name: table name
        """
        return symbols.TableExtant(self, schema_name, table_name)

    class ModelEvolutionContextManager (object):
        """Represents a model evolution session.
        """

        class Rollback (Exception):
            """Exception that triggers an immediate rollback and exit from a model evolution session."""
            pass

        def __init__(self, parent, dry_run=False, enable_work_sharing=False):
            """Initializes the model evolution session.

            :param parent: catalog model
            :param dry_run: run operations, but do not materialize to the remote catalog
            :param enable_work_sharing: enable the (experimental) work sharing algorithm
            """
            assert isinstance(parent, Model), 'Parameter "model" must be an instance of Model'
            self.model = parent
            self.dry_run = dry_run
            self.enable_work_sharing = enable_work_sharing
            self.computed_relations = []

        def create_table_as(self, schema_name, table_name, expression):
            """Create table as defined by an expression, on exit from model evolution session.

            For example, to create a new relation 'bar' from the normalized values of a column 'bar' in table 'foo':
            ```
            with model.begin() as session:
                session.create_table_as('public', 'bar', foo.columns['bar'].to_atoms())
            ```

            :param schema_name: schema name
            :param table_name: table name
            :param expression: expression producing a table definition
            """
            if not schema_name or not isinstance(schema_name, str):
                raise ValueError('"schema_name" must be a non-empty string')
            if not table_name or not isinstance(table_name, str):
                raise ValueError('"table_name" must be a non-empty string')
            if not isinstance(expression, ComputedRelation):
                raise ValueError('"expression" must be instance of ComputedRelation')

            self.computed_relations.append(
                ComputedRelation(self.model.schemas[schema_name], symbols.Assign(expression._logical_plan, schema_name, table_name))
            )

        def __enter__(self):
            assert len(self.computed_relations) == 0, 'Field "computed_relations" must be empty'
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type == Model.ModelEvolutionContextManager.Rollback:
                return True
            elif exc_type:
                return False
            elif not self.computed_relations:
                return

            self.model._commit(self.computed_relations, dry_run=self.dry_run, enable_work_sharing=self.enable_work_sharing)

        def rollback(self):
            """Rollback the model evolution session without committing any changes to the remote catalog."""
            raise Model.ModelEvolutionContextManager.Rollback()

    def begin(self, dry_run=False, enable_work_sharing=False):
        """Begins a model evolution session for use as a context manager in `with` blocks.

        This should be called in a `with` statement block. At the end of the block, the pending changes will be
        committed to the catalog. If an exception, any exception, is raised during the block the current mutations will
        be cancelled.

        Usage:
        ```
        with model.begin() as session:
            session.create_table_as('foo', 'baz', model.schemas['foo'].tables['bar'].where(...).select(...))
            session.create_table_as('foo', 'qux', model.schemas['foo'].tables['bar'].columns['qux'].to_atoms())
        ```

        The `session` maybe rolled back using the `session.rollback()` method at any time in the block and the block
        will immediately terminate without any change to the remote catalog.

        :param dry_run: run operations, but do not materialize to the remote catalog
        :param enable_work_sharing: enable the (experimental) work sharing algorithm
        :return: model evolution context manager for use in `with` statements
        """
        return Model.ModelEvolutionContextManager(self, dry_run=dry_run, enable_work_sharing=enable_work_sharing)

    def _commit(self, computed_relations, dry_run=False, enable_work_sharing=False):
        """Commits a set of computed relations to the remote catalog.

        :param computed_relations: sequence of computed relations to be committed to the model
        :param dry_run: run operations, but do not materialize to the remote catalog
        :param enable_work_sharing: enable the (experimental) work sharing algorithm
        """

        # decompose, optimize and rewrite the logical plans for each computed relation
        for computed_relation in computed_relations:
            computed_relation._logical_plan = logical_planner(computed_relation._logical_plan)

        # look for work sharing (consolidation) of computed relations
        if enable_work_sharing:
            consolidate(computed_relations)

        # materialize the computed relations
        for computed_relation in computed_relations:

            # rewrite logical to physical plan
            physical_plan = physical_planner(computed_relation._logical_plan)

            if dry_run:
                # log details of the evaluated operation without committing to remote catalog
                logging.debug('LOGICAL PLAN')
                logging.debug(pformat(computed_relation._logical_plan))
                logging.debug('PHYSICAL PLAN')
                logging.debug(pformat(physical_plan))
                logging.debug('SCHEMA')
                logging.debug(pformat(physical_plan.description))
                logging.debug('DATA')
                logging.debug(pformat(list(itertools.islice(physical_plan, 100))))

            elif isinstance(physical_plan, Assign):
                # commit to the remove catalog
                logger.debug('Creating table "%s"' % physical_plan.description['table_name'])
                schema_name = physical_plan.description['schema_name']
                table_name = physical_plan.description['table_name']

                # re-define table from plan description (allows us to provide system columns)
                desc = physical_plan.description
                table_doc = Table.define(
                    desc['table_name'],
                    column_defs=desc['column_definitions'],
                    key_defs=desc['keys'],
                    fkey_defs=desc['foreign_keys'],
                    comment=desc['comment'],
                    acls=desc.get('acls', {}),
                    acl_bindings=desc.get('acl_bindings', {}),
                    annotations=desc.get('annotations', {}),
                    provide_system=Model.provide_system
                )

                # create table in remote catalog
                self.schemas[schema_name].create_table(table_doc)

                # populate new_table from physical plan for this relation
                paths = self.catalog.getPathBuilder()
                new_table = paths.schemas[schema_name].tables[table_name]

                # ...determine the nondefaults for the insert
                planned_column_names = set([col['name'] for col in desc['column_definitions']])
                nondefaults = {'RID', 'RCB', 'RCT'} & planned_column_names  # write syscol values if defined in plan

                # ...stream tuples from the physical operator to the remote catalog
                try:
                    new_table.insert(physical_plan, nondefaults=nondefaults)
                except IndexError:
                    pass  # indicates that the expression resulted in 0 tuples, not an error in itself

                # update default vizfkeys of referred pk tables (*experimental*)
                if self.update_default_vizfkeys_on_commit:
                    table = self.schemas[schema_name].tables[table_name]
                    for fkey in table.foreign_keys:
                        vizfkeys = fkey.pk_table.annotations.get(_erm.tag.visible_foreign_keys, {}).get('*')
                        if isinstance(vizfkeys, list):
                            fkey_name = [fkey.table.schema.name, fkey.constraint_name]
                            if fkey_name not in vizfkeys:
                                vizfkeys.append(fkey_name)

            else:
                raise ValueError('Computed relation evaluated to "%s" object cannot be materialized' % type(physical_plan).__name__)


class Schema (model.Schema):
    """Schema within a catalog model.
    """
    def __init__(self, parent, schema):
        """Initializes the schema.

        :param parent: the parent of this model object.
        :param schema: underlying ermrest_model.Schema instance.
        """
        super(Schema, self).__init__(parent, schema)
        self._new_table = lambda obj: Table(self, obj)

    def create_table_as(self, table_name, expression, dry_run=False):
        """Create table as defined by an expression.

        For example, to create a new relation 'bar' from the normalized values of a column 'bar' in table 'foo':
        ```
        schema.create_table_as('bar', foo.columns['bar'].to_atoms())
        ```

        :param table_name: table name
        :param expression: expression producing a table definition
        :param dry_run: evaluate expression but do not create new table in the remote catalog
        :return: a Table instance representing the newly created table in the remote catalog, or None if dry_run
        """
        if not table_name or not isinstance(table_name, str):
            raise ValueError('"table_name" must be a non-empty string')
        if not isinstance(expression, ComputedRelation):
            raise ValueError('"expression" must be instance of ComputedRelation')

        self.model._commit([
            ComputedRelation(self, symbols.Assign(expression._logical_plan, self.name, table_name))
        ], dry_run=dry_run)

        return None if dry_run else self.tables[table_name]


class Table (model.Table):
    """Table within a schema.
    """
    def __init__(self, parent, table, logical_plan=None):
        """Initializes the table.

        :param parent: the parent of this model object.
        :param table: the underlying ermrest_model.Table instance.
        :param logical_plan: logical plan to use instead of an 'extant' representation
        """
        super(Table, self).__init__(parent, table)
        self._new_column = lambda obj: Column(self, obj)
        self._new_key = lambda obj: Key(self, obj)
        self._new_fkey = lambda obj: ForeignKey(self, obj)
        self._logical_plan = logical_plan or self.schema.model.make_extant_symbol(self.schema.name, self.name)

    def _columns_to_symbols(self, *columns):
        """Validates and returns cleaned up column list.
        """
        def _column_to_symbol(column):
            if isinstance(column, Column):
                return column.name
            elif isinstance(column, str):
                return column
            elif any(isinstance(column, t) for t in (symbols.AttributeAlias, symbols.AttributeDrop, symbols.AttributeAdd)):
                return column
            else:
                raise ValueError("Unsupported type '%s' in column list" % type(column).__name__)

        return tuple([_column_to_symbol(c) for c in columns])

    def alter(self, **kwargs):
        # Wraps the underlying object's `alter` method, updates logical plan (just in case) and copies its documentation
        self._wrapped_obj.alter(**kwargs)
        self._logical_plan = self.schema.model.make_extant_symbol(self.schema.name, self.name)
        return self

    alter.__doc__ = _erm.Table.alter.__doc__

    def clone(self):
        """Clone this table.

        :return: computed relation
        """
        return self.select()

    def select(self, *columns):
        """Selects a subset of columns.

        :param columns: positional argument list of columns or column names.
        :return: computed relation
        """
        if columns:
            projection = self._columns_to_symbols(*columns)

            # validation: if any mutation (add/drop), all must be mutations (can't mix with other projections)
            for mutation in (symbols.AttributeAdd, symbols.AttributeDrop):
                mutations = [isinstance(o, mutation) for o in projection]
                if any(mutations):
                    if not all(mutations):
                        raise ValueError("Attribute add/drop cannot be mixed with other attribute projections")
                    projection = (symbols.AllAttributes()) + projection

        else:
            projection = tuple([c.name for c in self.columns])

        return ComputedRelation(self.schema, symbols.Project(self._logical_plan, projection))

    def join(self, right):
        """Joins with right-hand relation.

        :param right: right-hand relation to be joined.
        :return: computed relation
        """
        if not isinstance(right, Table):
            raise ValueError('Right-hand object must be an instance of "Table"')

        return ComputedRelation(self.schema, symbols.Join(self._logical_plan, right._logical_plan))

    def where(self, expression):
        """Filters the rows of this table according to the where-clause expression.

        :param expression: where-clause expression (instance of Comparison, Conjunction, or Disjunction)
        :return: table instance
        """
        if not (isinstance(expression, symbols.Comparison) or isinstance(expression, symbols.Conjunction) or
                isinstance(expression, symbols.Disjunction)):
            raise ValueError('expression of type "%s" not supported' % type(expression).__name__)

        return ComputedRelation(self.schema, symbols.Select(self._logical_plan, expression))

    def union(self, other):
        """Produce a union with another relation.

        :param other: a relation; must have matching column definitions with this relation.
        :return: computed relation
        """
        if not isinstance(other, Table):
            raise ValueError('Parameter "other" must be a Table instance')

        return ComputedRelation(self.schema, symbols.Union(self._logical_plan, other._logical_plan))

    __add__ = union

    def reify_sub(self, *columns):
        """Forms a new 'child' relation from a subset of columns within this relation.

        :param columns: positional arguments of columns or column names
        :return: computed relation
        """
        return ComputedRelation(self.schema, symbols.ReifySub(self._logical_plan, self._columns_to_symbols(*columns)))

    def associate(self, *fk_columns):
        """Forms a new 'child' relation from a subset of foreign key columns within this relation.

        :param fk_columns: positional arguments of columns or column names belonging to a foreign key
        :return: computed relation
        """
        return ComputedRelation(self.schema, symbols.Associate(self._logical_plan, self._columns_to_symbols(*fk_columns)))

    def reify(self, unique_columns, *columns):
        """Forms a new relation from the specified columns.

        The `unique_columns` will be used as the columns of the computed relation's key. They need not be unique in the
        source relation. The remaining `*columns` will form the rest of the columns of the computed relation.

        :param unique_columns: a collection of columns or column names
        :param *columns: positional arguments of columns or column names
        :return: computed relation
        """
        unique_columns = self._columns_to_symbols(*unique_columns)
        nonkey_columns = self._columns_to_symbols(*columns)
        if set(unique_columns) & set(nonkey_columns):
            raise ValueError('"key_columns" and "nonkey_columns" must be disjoint sets')

        return ComputedRelation(self.schema, symbols.Reify(self._logical_plan, unique_columns, nonkey_columns))


class ComputedRelation (Table):
    """Table (i.e., relation) object computed from a chisel expression.
    """

    def __init__(self, parent, logical_plan):
        """Initializes the computed relation.

        :param parent: the parent of this model object.
        :param logical_plan: chisel logical plan expression used to define this table
        """

        # invoke the expression planner to generate a physical operator plan
        plan = planner(logical_plan)

        # get the whole model doc and graft this computed relation into it
        computed_model_doc = parent.model.prejson()
        computed_model_doc['schemas'][parent.name]['tables'][plan.description['table_name']] = plan.description

        # instantiate a stubbed out model object
        computed_model = Model(CatalogStub(model_doc=computed_model_doc))

        # instantiate this object's super class (i.e., Table object)
        super(ComputedRelation, self).__init__(
            computed_model.schemas[parent.name],
            computed_model.schemas[parent.name].tables[plan.description['table_name']],
            logical_plan=logical_plan
        )

    @property
    def logical_plan(self):
        return self._logical_plan

    @logical_plan.setter
    def logical_plan(self, value):
        self._logical_plan = value

    def fetch(self):
        """Returns an iterator over the rows of this relation.
        """
        return planner(self._logical_plan)


class Column (model.Column):
    """Column within a table.
    """
    def __init__(self, parent, column):
        """Initializes the column.

        :param parent: the parent of this model object.
        :param column: the underlying ermrest_model.Column
        """
        super(Column, self).__init__(parent, column)

    def eq(self, other):
        return symbols.Comparison(operand1=self.name, operator='eq', operand2=other)

    __eq__ = eq

    def lt(self, other):
        return symbols.Comparison(operand1=self.name, operator='lt', operand2=other)

    __lt__ = lt

    def le(self, other):
        return symbols.Comparison(operand1=self.name, operator='le', operand2=other)

    __le__ = le

    def gt(self, other):
        return symbols.Comparison(operand1=self.name, operator='gt', operand2=other)

    __gt__ = gt

    def ge(self, other):
        return symbols.Comparison(operand1=self.name, operator='ge', operand2=other)

    __ge__ = ge

    eq.__doc__ = \
    lt.__doc__ = \
    le.__doc__ = \
    gt.__doc__ = \
    ge.__doc__ = \
        """Creates and returns a comparison clause.

        :param other: assumes a literal value; column references not allows
        :return: a symbolic comparison clause to be used in other statements
        """

    def alias(self, name):
        """Returns a 'column alias' clause that may be used in 'select' operations.

        :param name: name to use as an alias for this column
        :return: column alias symbol for use in expressions
        """
        return symbols.AttributeAlias(self.name, name)

    def inv(self):
        """Returns a 'remove column' clause that may be used in 'select' operations to remove this column.

        :return: remove column clause
        """
        return symbols.AttributeDrop(self.name)

    __invert__ = inv
    
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
            raise ValueError('Parameter "unnest_fn" must be callable')

        return ComputedRelation(self.table.schema, symbols.Atomize(self.table._logical_plan, unnest_fn, self.name))

    def to_domain(self, similarity_fn=util.edit_distance_fn):
        """Computes a new 'domain' from this column.

        :param similarity_fn: a function for computing a similarity comparison between values in this column.
        :return: a computed relation that represents the new domain
        """
        return ComputedRelation(self.table.schema, symbols.Domainify(self.table._logical_plan, self.name, similarity_fn, None))

    def to_vocabulary(self, similarity_fn=util.edit_distance_fn, grouping_fn=None):
        """Creates a canonical 'vocabulary' from this column.

        :param similarity_fn: a function for computing a similarity comparison between values in this column.
        :param grouping_fn: a function for computing candidate 'groups' to which the similarity function is used to
        determine the final groupings.
        :return: a computed relation that represents the new vocabulary
        """
        return ComputedRelation(self.table.schema, symbols.Canonicalize(self.table._logical_plan, self.name, similarity_fn, grouping_fn))

    def align(self, domain, similarity_fn=util.edit_distance_fn):
        """Align this column with a given domain

        :param domain: a simple domain or a fully structured vocabulary
        :param similarity_fn: a function for computing a similarity comparison between values in this column.
        :return: a computed relation that represents the containing table with this attribute aligned to the domain
        """
        if not isinstance(domain, Table):
            raise ValueError("domain must be a table instance")

        return ComputedRelation(self.table.schema, symbols.Align(domain._logical_plan, self.table._logical_plan, self.name, similarity_fn, None))

    def to_tags(self, domain, delim=',', unnest_fn=None, similarity_fn=util.edit_distance_fn):
        """Computes a new relation from the unnested and aligned values of this column.

        :param domain: a simple domain or a fully structured vocabulary
        :param delim: delimited character.
        :param unnest_fn: custom unnesting function must be callable on each value of this column in the relation.
        :param similarity_fn: a function for computing a similarity comparison between values in this column.
        :return: a computed relation that can be assigned to a newly named table in the catalog.
        """
        if not isinstance(domain, Table):
            raise ValueError("domain must be a table instance")

        if not unnest_fn:
            unnest_fn = util.splitter_fn(delim)
        elif not callable(unnest_fn):
            raise ValueError('unnest_fn must be callable')

        return ComputedRelation(self.table.schema, symbols.Tagify(domain._logical_plan, self.table._logical_plan, self.name, unnest_fn, similarity_fn, None))


class Key (model.Key):
    """Key within a table.
    """
    def __init__(self, parent, constraint):
        """Initializes the constraint.

        :param parent: the parent of this model object.
        :param constraint: the underlying ermrest_model.{Key|ForeignKey}
        """
        super(Key, self).__init__(parent, constraint)
        self._new_schema = lambda obj: Schema(parent.schema.model, obj)
        self._new_table = lambda obj: Table(parent.schema, obj)
        self._new_column = lambda obj: Column(parent.schema.model.schemas[obj.table.schema.name].tables[obj.table.name], obj)


class ForeignKey (model.ForeignKey):
    """ForeignKey within a table.
    """
    def __init__(self, parent, constraint):
        """Initializes the constraint.

        :param parent: the parent of this model object.
        :param constraint: the underlying ermrest_model.{Key|ForeignKey}
        """
        super(ForeignKey, self).__init__(parent, constraint)
        self._new_schema = lambda obj: Schema(obj.model, obj)
        self._new_table = lambda obj: Table(parent.schema, obj)
        self._new_column = lambda obj: Column(parent.schema.model.schemas[obj.table.schema.name].tables[obj.table.name], obj)
