"""Extended catalog model classes.
"""
from . import model
from .stubs import CatalogStub, ModelStub
from ..optimizer import symbols, planner, ERMrestExtant
from .. import util


class Model (model.Model):
    """Catalog model.
    """
    def __init__(self, catalog):
        """Initializes the model.

        :param catalog: ErmrestCatalog object
        """
        super(Model, self).__init__(catalog)
        self._new_schema = lambda obj: Schema(self, obj)

        self.ermrest_catalog = self._wrapped_catalog  # NOTE: this should be reworked when refactoring complete
        self.make_extant_symbol = lambda sname, tname: ERMrestExtant(self, sname, tname)  # NOTE: revisit this too; can be param of constructor


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


class Table (model.Table):
    """Table within a schema.
    """
    def __init__(self, parent, table):
        """Initializes the table.

        :param parent: the parent of this model object.
        :param table: the underlying ermrest_model.Table instance.
        """
        super(Table, self).__init__(parent, table)
        self._new_column = lambda obj: Column(self, obj)
        self._new_key = lambda obj: Key(self, obj)
        self._new_fkey = lambda obj: ForeignKey(self, obj)
        self._logical_plan = self.schema.model.make_extant_symbol(self.schema.name, self.name)

    def clone(self):
        """Clone this table.

        :return: computed relation
        """
        return self.select()

    def select(self, *columns):
        """Selects a subset of columns.

        :param columns: positional argument list of Column objects or string column names.
        :return: computed relation
        """
        if columns:
            projection = []

            # validation: projection may be column, column name, alias, addition or removal
            for column in columns:
                if isinstance(column, Column):
                    projection.append(column.name)
                elif isinstance(column, str) or isinstance(column, symbols.AttributeAlias)\
                        or isinstance(column, symbols.AttributeDrop) or isinstance(column, symbols.AttributeAdd):
                    projection.append(column)
                else:
                    raise ValueError("Unsupported projection type '{}'".format(type(column).__name__))

            # validation: if any mutation (add/drop), all must be mutations (can't mix with other projections)
            for mutation in (symbols.AttributeAdd, symbols.AttributeDrop):
                mutations = [isinstance(o, mutation) for o in projection]
                if any(mutations):
                    if not all(mutations):
                        raise ValueError("Attribute add/drop cannot be mixed with other attribute projections")
                    projection = [symbols.AllAttributes()] + projection

        else:
            projection = [cname for cname in self.columns]

        return ComputedRelation(self.schema, symbols.Project(self._logical_plan, tuple(projection)))

    def join(self, right):
        """Joins with right-hand relation.

        :param right: right-hand relation to be joined.
        :return: computed relation
        """
        if not isinstance(right, Table):
            raise ValueError('Object to the right of the join is not an instance of "Table"')

        return ComputedRelation(self.schema, symbols.Join(self._logical_plan, right._logical_plan))

    def where(self, expression):
        """Filters the rows of this table according to the where-clause expression.

        :param expression: where-clause expression (comparison or conjunction of comparisons)
        :return: table instance
        """
        if not any(isinstance(expression, symbol) for symbol in [symbols.Comparison, symbols.Conjunction]):
            raise ValueError('invalid expression')

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

        :param columns: positional arguments of type Column
        :return: computed relation
        """
        if not all(isinstance(col, Column) for col in columns):
            raise ValueError("All parameters must be instances of Column")

        return ComputedRelation(self.schema, symbols.ReifySub(self._logical_plan, tuple([col.name for col in columns])))

    def reify(self, key_columns, nonkey_columns):
        """Forms a new relation from the set of key and non-key columns out of this relation.

        The 'key_columns' do not have to be key columns or even hold unique values in the source relation. This
        operation will apply a unique constraint on 'key_columns' in the newly computed relation. The sets of
        'key_columns' and 'nonkey_columns' must be disjoint.

        :param key_columns: a set of Column objects from the source relation
        :param nonkey_columns: a set of Column objects from the source relation
        :return: computed relation
        """
        if not all(isinstance(col, Column) for col in key_columns | nonkey_columns):
            raise ValueError("All column arguments must be instances of Column")
        if set(key_columns) & set(nonkey_columns):
            raise ValueError('"key_columns" and "nonkey_columns" must be disjoin sets')

        return ComputedRelation(self.schema, symbols.Reify(self._logical_plan, tuple([col.name for col in key_columns]), tuple([col.name for col in nonkey_columns])))


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

        # create a model doc to represent the computed partial model for the table
        computed_model_doc = {
            'schemas': {
                parent.name: {
                    'tables': {
                        plan.description['table_name']: plan.description
                    }
                }
            }
        }

        # instantiate a stubbed out model object
        computed_model = ModelStub(CatalogStub(), computed_model_doc)

        # instantiate this objects super class (i.e., Table object)
        super(ComputedRelation, self).__init__(parent, computed_model.schemas[parent.name].tables[plan.description['table_name']])

        # overwrite the extant expression with the actual computed relation's logical plan
        self._logical_plan = logical_plan


class Column (model.Column):
    """Column within a table.
    """
    def __init__(self, parent, column):
        """Initializes the column.

        :param parent: the parent of this model object.
        :param column: the underlying ermrest_model.Column
        """
        super(Column, self).__init__(parent, column)

    def __hash__(self):
        return super(Column, self).__hash__()

    def eq(self, other):
        return symbols.Comparison(operand1=self.name, operator='=', operand2=str(other))

    __eq__ = eq

    def lt(self, other):
        return symbols.Comparison(operand1=self.name, operator='<', operand2=str(other))

    __lt__ = lt

    def le(self, other):
        return symbols.Comparison(operand1=self.name, operator='<=', operand2=str(other))

    __le__ = le

    def gt(self, other):
        return symbols.Comparison(operand1=self.name, operator='>', operand2=str(other))

    __gt__ = gt

    def ge(self, other):
        return symbols.Comparison(operand1=self.name, operator='>=', operand2=str(other))

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
        self._new_schema = lambda obj: Schema(self, obj)
        self._new_column = lambda obj: Column(self, obj)


class ForeignKey (model.ForeignKey):
    """ForeignKey within a table.
    """
    def __init__(self, parent, constraint):
        """Initializes the constraint.

        :param parent: the parent of this model object.
        :param constraint: the underlying ermrest_model.{Key|ForeignKey}
        """
        super(ForeignKey, self).__init__(parent, constraint)
        self._new_schema = lambda obj: Schema(self, obj)
        self._new_column = lambda obj: Column(self, obj)
