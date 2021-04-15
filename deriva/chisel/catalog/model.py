"""Catalog model classes.
"""
import logging
from deriva.core import ermrest_model as _erm
from .wrapper import MappingWrapper, SequenceWrapper, ModelObjectWrapper

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
        self.acls = self._wrapped_model.acls
        self.annotations = self._wrapped_model.annotations
        self.apply = self._wrapped_model.apply

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

    def create_table(self, table_def):
        """Add a new table to this schema in the remote database based on table_def.

           Returns a new Table instance based on the server-supplied
           representation of the newly created table.

           The returned Table is also added to self.tables.
        """
        return self._new_table(self._wrapped_obj.create_table(table_def))

    def drop(self, cascade=False):
        """Remove this schema from the remote database.

        :param cascade: automatically drop objects (namely tables) that are contained in the schema. Note that this will
                        only drop object managed by ERMrest (i.e., tables, keys, and foreign keys).
        """
        logging.debug('Dropping %s cascade %s' % (self.name, str(cascade)))
        if cascade:
            # drop dependent objects
            for table in list(self.tables.values()):
                table.drop(cascade=True)

        super(Schema, self).drop()


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
        self._new_fkey = lambda obj: ForeignKey(self, obj)

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

    def create_column(self, column_def):
        """Add a new column to this table in the remote database based on column_def.

           Returns a new Column instance based on the server-supplied
           representation of the new column, and adds it to
           self.column_definitions too.
        """
        return self._new_column(self._wrapped_obj.create_column(column_def))

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

    def drop(self, cascade=False):
        """Remove this table from the remote database.

        :param cascade: automatically drop objects (namely foreign keys) that depend on this table. Note that this will
                        only drop object managed by ERMrest (i.e., foreign keys).
        """
        logging.debug('Dropping %s cascade %s' % (self.name, str(cascade)))
        if cascade:
            # drop dependent objects
            for fkey in list(self.referenced_by):
                fkey.drop()
        super(Table, self).drop()


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


class Constraint (ModelObjectWrapper):
    """Constraint within a table.
    """
    def __init__(self, parent, constraint):
        """Initializes the constraint.

        :param parent: the parent of this model object.
        :param constraint: the underlying ermrest_model.{Key|ForeignKey}
        """
        super(Constraint, self).__init__(constraint)
        self.table = parent
        self._new_schema = lambda obj: Schema(self, obj)
        self._new_column = lambda obj: Column(parent.schema.model.schemas[obj.table.schema.name].tables[obj.table.name], obj)

    @property
    def name(self):
        """Constraint name (schemaobj, name_str) used in API dictionaries."""
        constraint_schema, constraint_name = self._wrapped_obj.name
        return self._new_schema(constraint_schema), constraint_name


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
    def referenced_columns(self):
        return SequenceWrapper(self._new_column, self._wrapped_obj.referenced_columns)

    def __str__(self):
        return '"%s" FOREIGN KEY (%s) REFERENCES "%s":"%s"(%s)' % (
            self.constraint_name,
            ', '.join(['"%s"' % c.name for c in self.foreign_key_columns]),
            self._wrapped_obj.pk_table.schema.name,
            self._wrapped_obj.pk_table.name,
            ', '.join(['"%s"' % c.name for c in self.referenced_columns])
        )
