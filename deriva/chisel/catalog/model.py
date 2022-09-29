"""Catalog model classes.
"""
import logging
from deriva.core import ermrest_model as _erm
from .wrapper import MappingWrapper, SequenceWrapper, ModelObjectWrapper
from .. import mmo

logger = logging.getLogger(__name__)


class Model (object):
    """Catalog model.
    """
    def __init__(self, catalog):
        """Initializes the model.

        :param catalog: ErmrestCatalog object
        """
        super(Model, self).__init__()
        self._catalog = catalog
        self._wrapped_model = catalog.getCatalogModel()
        self._new_schema = lambda obj: Schema(self, obj)
        self._new_fkey = lambda obj: ForeignKey(self.schemas[obj.table.schema.name].tables[obj.table.name], obj)
        self.acls = self._wrapped_model.acls
        self.annotations = self._wrapped_model.annotations
        self.apply = self._wrapped_model.apply
        self.prejson = self._wrapped_model.prejson

    @classmethod
    def from_catalog(cls, catalog):
        """Retrieve catalog Model management object.
        """
        return cls(catalog)

    @property
    def catalog(self):
        return self._catalog

    @property
    def schemas(self):
        return MappingWrapper(self._new_schema, self._wrapped_model.schemas)

    def fkey(self, constraint_name_pair):
        """Return foreign key with given name pair.

        This method wraps the `deriva.core.ermrest_model.Model.fkey` method:
        > Accepts (schema_name, constraint_name) pairs as found in many
        > faceting annotations and (schema_obj, constraint_name) pairs
        > as found in fkey.name fields.
        """
        return self._new_fkey(self._wrapped_model.fkey(constraint_name_pair))

    def create_schema(self, schema_def):
        """Add a new schema to this model in the remote database based on schema_def.

           Returns a new Schema instance based on the server-supplied
           representation of the newly created schema.

           The returned Schema is also added to self.schemas.
        """
        return self._new_schema(self._wrapped_model.create_schema(schema_def))


class Schema (ModelObjectWrapper):
    """Schema within a catalog model.
    """

    define = _erm.Schema.define

    def __init__(self, parent, schema):
        """Initializes the schema.

        :param parent: the parent of this model object.
        :param schema: underlying ermrest_model.Schema instance.
        """
        super(Schema, self).__init__(schema)
        self.model = parent
        self._new_table = lambda obj: Table(self, obj)

    @property
    def tables(self):
        return MappingWrapper(self._new_table, self._wrapped_obj.tables)

    def create_table(self, table_def, add_visible_columns=False):
        """Add a new table to this schema in the remote database based on table_def.

           Returns a new Table instance based on the server-supplied
           representation of the newly created table.

           The returned Table is also added to self.tables.

           Param 'add_visible_columns' enables an experimental feature that adds
           a visible-columns '*' context to the annotations of the table based
           on a heuristic of adding all key names, fkey names, and columns not in
           a key or fkey.
        """
        table = self._new_table(self._wrapped_obj.create_table(table_def))
        if add_visible_columns:
            constrained_columns = []
            vizcols_default = []
            # add keys to visible list
            for key in table.keys:
                vizcols_default.append([key.name[0].name, key.name[1]])
                for column in key.unique_columns:
                    constrained_columns.append(column.name)
            # add fkeys to visible list
            for fkey in table.foreign_keys:
                vizcols_default.append([fkey.name[0].name, fkey.name[1]])
                for column in fkey.foreign_key_columns:
                    constrained_columns.append(column.name)
            # add (remaining) columns to visible list
            for column in table.columns:
                if column.name not in constrained_columns:
                    vizcols_default.append(column.name)
            # populate default vizcols
            table.annotations[_erm.tag.visible_columns] = {'*': vizcols_default}
            # populate default vizfkeys
            table.annotations[_erm.tag.visible_foreign_keys] = {'*': []}
            # apply annotation changes
            table.apply()
        return table

    def create_association(self, table1, table2):  # todo: need unit tests
        """Creates an association table that references the given tables.

        This method requires that both tables have a `RID` primary key. It
        creates a table named `{table1.name}_{table2.name}` with foreign
        keys to each of the given tables. It also creates a key spanning
        the columns of the foreign key.

        :param table1: a table to be referenced by this table
        :param table2: a table to be referenced by this table
        :return: a new Table instance based on the server-supplied
        representation of the table.
        """
        return self.create_table(
            Table.define(
                f'{table1.name}_{table2.name}',
                column_defs=[
                    Column.define(table1.name, _erm.builtin_types['text'], nullok=False),
                    Column.define(table2.name, _erm.builtin_types['text'], nullok=False)
                ],
                key_defs=[
                    Key.define([table1.name, table2.name])
                ],
                fkey_defs=[
                    ForeignKey.define(
                        [table1.name],
                        table1.schema.name, table1.name, [table1.columns['RID'].name],
                        on_update='CASCADE'
                    ),
                    ForeignKey.define(
                        [table2.name],
                        table2.schema.name, table2.name, [table2.columns['RID'].name],
                        on_update='CASCADE'
                    )
                ]
            )
        )

    def drop(self, cascade=False):
        """Remove this schema from the remote database.

        :param cascade: drop dependent objects.
        """
        logging.debug('Dropping %s cascade %s' % (self.name, str(cascade)))
        if cascade:
            # drop dependent objects
            for table in list(self.tables.values()):
                table.drop(cascade=True)

        self._wrapped_obj.drop()


class Table (ModelObjectWrapper):
    """Table within a schema.
    """

    define = _erm.Table.define
    define_vocabulary = _erm.Table.define_vocabulary
    define_asset = _erm.Table.define_asset

    def __init__(self, parent, table):
        """Initializes the table.

        :param parent: the parent of this model object.
        :param table: the underlying ermrest_model.Table instance.
        """
        super(Table, self).__init__(table)
        self.schema = parent
        self._new_column = lambda obj: Column(self, obj)
        self._new_key = lambda obj: Key(self, obj)
        self._new_fkey = lambda obj: ForeignKey(parent.model.schemas[obj.table.schema.name].tables[obj.table.name], obj)

    @property
    def kind(self):
        return self._wrapped_obj.kind

    @property
    def column_definitions(self):
        return SequenceWrapper(self._new_column, self._wrapped_obj.columns)

    @property
    def columns(self):
        return self.column_definitions

    @property
    def keys(self):
        return SequenceWrapper(self._new_key, self._wrapped_obj.keys)

    @property
    def foreign_keys(self):
        return SequenceWrapper(self._new_fkey, self._wrapped_obj.foreign_keys)

    @property
    def referenced_by(self):
        return SequenceWrapper(self._new_fkey, self._wrapped_obj.referenced_by)

    def create_column(self, column_def, update_visible_columns=False):
        """Add a new column to this table in the remote database based on column_def.

           Returns a new Column instance based on the server-supplied
           representation of the new column, and adds it to
           self.column_definitions too.

           Param 'update_visible_columns' enables an experimental feature to update
           the default (*) visible columns with the newly added column.
        """
        column = self._new_column(self._wrapped_obj.create_column(column_def))
        if update_visible_columns:
            vizcols = self.annotations.get(_erm.tag.visible_columns, {}).get('*', [])
            if vizcols:
                vizcols.append(column.name)
                self.apply()
        return column

    def create_key(self, key_def):
        """Add a new key to this table in the remote database based on key_def.

           Returns a new Key instance based on the server-supplied
           representation of the new key, and adds it to self.keys
           too.

        """
        return self._new_key(self._wrapped_obj.create_key(key_def))

    def create_fkey(self, fkey_def):
        """Add a new foreign key to this table in the remote database based on fkey_def.

           Returns a new ForeignKey instance based on the
           server-supplied representation of the new foreign key, and
           adds it to self.fkeys too.

        """
        return self._new_fkey(self._wrapped_obj.create_fkey(fkey_def))

    def add_reference(self, table):  # todo: unit test needed
        """Adds a foriegn key reference to `table`.

        This method requires that `table` has a `RID` primary key. It creates a
        compatible foreign key column in `self` and creates a foreign key
        constraint to `table` (`RID`).

        :param table: the table to be referenced by this table
        :return: a new ForeignKey instance based on the server-supplied
        representation of the foreign key.
        """

        fkeydef = ForeignKey.define(
            [table.name],
            table.schema.name, table.name, [table.columns['RID'].name],
            on_update='CASCADE'
        )

        self.create_column(Column.define(table.name, _erm.builtin_types['text']))
        return self.create_fkey(fkeydef)

    def drop(self, cascade=False):
        """Remove this table from the remote database.

        :param cascade: drop dependent objects.
        """
        logging.debug('Dropping %s cascade %s' % (self.name, str(cascade)))
        if cascade:
            # drop dependent objects
            for fkey in list(self.referenced_by):
                fkey.drop()

        self._wrapped_obj.drop()


class Column (ModelObjectWrapper):
    """Column within a table.
    """

    define = _erm.Column.define

    def __init__(self, parent, column):
        """Initializes the column.

        :param parent: the parent of this model object.
        :param column: the underlying ermrest_model.Column
        """
        super(Column, self).__init__(column)
        self.table = parent

    @property
    def type(self):
        return self._wrapped_obj.type

    @property
    def nullok(self):
        return self._wrapped_obj.nullok

    @property
    def default(self):
        return self._wrapped_obj.default

    def alter(self, **kwargs):
        # Wraps the underlying object's `alter` method, adds mmo functionality, and copies its documentation

        # ...remember name, if needed later
        oldname = self.name
        newname = kwargs.get('name', _erm.nochange)

        # ...do the wrapped alter
        self._wrapped_obj.alter(**kwargs)

        # ...if name change, do mmo replace
        if newname != _erm.nochange:
            basename = [self.table.schema.name, self.table.name]
            mmo.replace(self.table.schema.model, basename + [oldname], basename + [newname])

        return self

    alter.__doc__ = _erm.Column.alter.__doc__

    def drop(self, cascade=False):
        """Remove this column from the remote database.

        :param cascade: drop dependent objects.
        """
        logging.debug('Dropping %s cascade %s' % (self.name, str(cascade)))
        if cascade:
            # drop dependent objects
            for key in list(self.table.keys):
                if self in key.unique_columns:
                    logging.debug('Found dependent object %s' % key)
                    key.drop(cascade=True)

        self._wrapped_obj.drop()
        mmo.prune(self.table.schema.model, [self.table.schema.name, self.table.name, self.name])


class Constraint (ModelObjectWrapper):
    """Constraint within a table.
    """
    def __init__(self, parent, constraint):
        """Initializes the constraint.

        :param parent: the parent of this model object.
        :param constraint: the underlying ermrest_model.{Key|ForeignKey}
        """
        super(Constraint, self).__init__(constraint)
        self._new_schema = lambda obj: Schema(parent.schema.model, obj)
        self._new_table = lambda obj: Table(parent.schema.model.schemas[obj.table.schema.name], obj)
        self._new_column = lambda obj: Column(parent.schema.model.schemas[obj.table.schema.name].tables[obj.table.name], obj)

    @property
    def table(self):
        return self._new_table(self._wrapped_obj.table)

    @property
    def name(self):
        """Constraint name (schemaobj, name_str) used in API dictionaries."""
        constraint_schema, constraint_name = self._wrapped_obj.name
        return self._new_schema(constraint_schema), constraint_name

    def alter(self, **kwargs):
        # Wraps the underlying object's `alter` method, adds mmo functionality, and copies its documentation

        # ...remember name, if needed later
        oldname = self.constraint_name
        newname = kwargs.get('constraint_name', _erm.nochange)

        # ...do the wrapped alter
        self._wrapped_obj.alter(**kwargs)

        # ...if name change, do mmo replace
        if newname != _erm.nochange:
            basename = [self.table.schema.name]
            mmo.replace(self.table.schema.model, basename + [oldname], basename + [newname])

        return self


class Key (Constraint):
    """Key within a table.
    """

    define = _erm.Key.define

    @property
    def unique_columns(self):
        return SequenceWrapper(self._new_column, self._wrapped_obj.unique_columns)

    @property
    def columns(self):
        return self.unique_columns

    def __str__(self):
        return '"%s" UNIQUE CONSTRAINT (%s)' % (self.constraint_name, ', '.join(['"%s"' % c.name for c in self.unique_columns]))

    def alter(self, **kwargs):
        super().alter(**kwargs)

    alter.__doc__ = _erm.Key.alter.__doc__

    def drop(self, cascade=False):
        """Remove this key from the remote database.

        :param cascade: drop dependent objects.
        """
        logging.debug('Dropping %s cascade %s' % (self.name, str(cascade)))
        if cascade:
            # drop dependent objects
            for fkey in list(self.table.referenced_by):
                assert self.table == fkey.pk_table, "Expected key.table and foreign_key.pk_table to match"
                if self.unique_columns == fkey.referenced_columns:
                    logging.debug('Found dependent object %s' % fkey)
                    fkey.drop()

        self._wrapped_obj.drop()
        mmo.prune(self.table.schema.model, [self._wrapped_obj.constraint_schema.name, self.constraint_name])


class ForeignKey (Constraint):
    """ForeignKey within a table.
    """

    define = _erm.ForeignKey.define

    @property
    def on_update(self):
        return self._wrapped_obj.on_update

    @property
    def on_delete(self):
        return self._wrapped_obj.on_delete

    @property
    def foreign_key_columns(self):
        return SequenceWrapper(self._new_column, self._wrapped_obj.foreign_key_columns)

    @property
    def pk_table(self):
        return self._new_table(self._wrapped_obj.pk_table)

    @property
    def referenced_columns(self):
        return SequenceWrapper(self._new_column, self._wrapped_obj.referenced_columns)

    def __str__(self):
        return '"%s" FOREIGN KEY (%s) --> "%s:%s" (%s)' % (
            self.constraint_name,
            ', '.join(['"%s"' % c.name for c in self.foreign_key_columns]),
            self._wrapped_obj.pk_table.schema.name,
            self._wrapped_obj.pk_table.name,
            ', '.join(['"%s"' % c.name for c in self.referenced_columns])
        )

    def alter(self, **kwargs):
        super().alter(**kwargs)

    alter.__doc__ = _erm.ForeignKey.alter.__doc__

    def drop(self):
        """Remove this foreign key from the remote database.
        """
        self._wrapped_obj.drop()
        mmo.prune(self.table.schema.model, [self._wrapped_obj.constraint_schema.name, self.constraint_name])
