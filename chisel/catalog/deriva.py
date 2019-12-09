"""Catalog model for ERMrest catalogs based on Deriva Catalog Manage library."""

import collections
import logging
from deriva.utils.catalog.components import deriva_model as _dm
from .. import optimizer
from .ermrest import ERMrestCatalog, ERMrestTable

logger = logging.getLogger(__name__)


class DerivaCatalog (ERMrestCatalog):
    """ERMrest catalog with implementation using the deriva-catalog-manage package."""

    # Prototype of the `DerivaTable` where a real instance is not needed. It mimics the interface when needed as a
    # parameter to deriva-catalog-manage APIs.
    _DerivaTablePrototype = collections.namedtuple('DerivaTablePrototype', ['schema', 'name'])

    def _do_create_table(self, schema_name, table_doc):
        deriva_catalog = _dm.DerivaCatalog(None, None, None, ermrest_catalog=self.ermrest_catalog, validate=False)
        deriva_schema = deriva_catalog.schema(schema_name)

        deriva_schema.create_table(
            table_doc['table_name'],
            column_defs=[self._deriva_column_from_column_doc(col_doc) for col_doc in table_doc['column_definitions']],
            key_defs=[self._deriva_key_from_key_doc(deriva_catalog, key_doc) for key_doc in table_doc['keys']],
            fkey_defs=[self._deriva_foreign_key_from_foreign_key_doc(deriva_catalog, fkey_doc) for fkey_doc in table_doc['foreign_keys']],
            comment=table_doc['comment'],
            acls=table_doc.get('acls', {}),
            acl_bindings=table_doc.get('acl_bindings', {}),
            annotations=table_doc.get('annotations', {})
        )

    def _do_copy_table(self, src_schema_name, src_table_name, dst_schema_name, dst_table_name):
        deriva_catalog = _dm.DerivaCatalog(None, None, None, ermrest_catalog=self.ermrest_catalog, validate=False)
        deriva_schema = deriva_catalog.schema(src_schema_name)
        deriva_table = deriva_schema.table(src_table_name)
        deriva_table.copy_table(dst_schema_name, dst_table_name)

        #  repair local model state
        model_doc = self.ermrest_catalog.getCatalogSchema()
        table_doc = model_doc['schemas'][dst_schema_name]['tables'][dst_table_name]
        schema = self.schemas[dst_schema_name]
        table = ERMrestTable(table_doc, schema=schema)
        schema.tables._backup[dst_table_name] = table  # TODO: this part is kludgy and needs to be revised

    def _do_move_table(self, src_schema_name, src_table_name, dst_schema_name, dst_table_name):
        deriva_catalog = _dm.DerivaCatalog(None, None, None, ermrest_catalog=self.ermrest_catalog, validate=False)
        deriva_schema = deriva_catalog.schema(src_schema_name)
        deriva_table = deriva_schema.table(src_table_name)
        with self.evolve():  # TODO: should remove this guard when other rename function is refactored
            deriva_table.move_table(dst_schema_name, dst_table_name)

        #  repair local model state
        model_doc = self.ermrest_catalog.getCatalogSchema()
        table_doc = model_doc['schemas'][dst_schema_name]['tables'][dst_table_name]
        dst_schema = self.schemas[dst_schema_name]
        table = ERMrestTable(table_doc, schema=dst_schema)
        dst_schema.tables._backup[dst_table_name] = table  # TODO: this part is kludgy and needs to be revised
        src_schema = self.schemas[src_schema_name]
        del src_schema.tables._backup[src_table_name]  # TODO: this is kludgy should revise
        src_schema.tables.reset()

    def _do_alter_table(self, schema_name, table_name, projection):
        """Alter table (general) in the catalog."""
        deriva_catalog = _dm.DerivaCatalog(None, None, None, ermrest_catalog=self.ermrest_catalog, validate=False)
        deriva_schema = deriva_catalog.schema(schema_name)
        deriva_table = deriva_schema.table(table_name)
        original_columns = {column.name: column for column in deriva_table.columns}

        # Notes: currently, there are two distinct scenarios in a projection,
        #  1) 'general' case: the projection is an improper subset of the relation's columns, and may include some
        #     aliased columns from the original columns. Also, columns may be aliased more than once.
        #  2) 'special' case for deletes only: as a syntactic sugar, many formulations of project support the
        #     notation of "-foo,-bar,..." meaning that the operator will project all _except_ those '-name' columns.
        #     We support that by first including the special symbol 'AllAttributes' followed by 'AttributeRemoval'
        #     symbols.

        if projection[0] == optimizer.AllAttributes():  # 'special' case for deletes only
            logger.debug("Dropping columns that were explicitly removed.")
            for removal in projection[1:]:
                assert isinstance(removal, optimizer.AttributeRemoval)
                logger.debug("Deleting column '{cname}'.".format(cname=removal.name))
                original_columns[removal.name].delete()

        else:  # 'general' case
            logger.debug("Copying 'aliased' columns in the projection")

            # step 1: get all the unaliased column names
            nonaliased_column_names = {column_name for column_name in projection if isinstance(column_name, str)}
            renamed_columns = []

            # step 2: COPY or RENAME the aliased columns in the projection
            for projected in projection:
                if isinstance(projected, optimizer.AttributeAlias):
                    original_column = original_columns[projected.name]
                    if projected.name not in nonaliased_column_names:  # RENAME
                        logger.debug('Renaming column "%s"' % original_column.name)
                        column_map = {
                            original_column: _dm.DerivaColumn(table=deriva_table, name=projected.alias,
                                                              type=original_column.type, nullok=original_column.nullok,
                                                              default=original_column.default, define=True)
                        }
                        deriva_table.rename_columns(column_map)
                        renamed_columns.append(original_column.name)
                    else:  # COPY
                        logger.debug('Copying column "%s"' % original_column.name)
                        column_map = {
                            original_column: _dm.DerivaColumn(table=deriva_table, name=projected.alias,
                                                              type=original_column.type, nullok=original_column.nullok,
                                                              default=original_column.default, define=True)
                        }
                        deriva_table.copy_columns(column_map)

            # step 3: remove columns that were not projected unless they were already renamed
            logger.debug("Dropping columns not in the projection.")
            for column in original_columns.values():
                if (column.name not in renamed_columns) and (column.name not in nonaliased_column_names | self.syscols):
                    logger.debug('Deleting column "%s"' % column.name)
                    column.delete()

    def _do_alter_table_add_column(self, schema_name, table_name, column_doc):
        """Alter table Add column in the catalog"""
        deriva_catalog = _dm.DerivaCatalog(None, None, None, ermrest_catalog=self.ermrest_catalog, validate=False)
        deriva_table = deriva_catalog.schema(schema_name).table(table_name)
        deriva_table.create_columns(self._deriva_column_from_column_doc(column_doc))

    def _do_drop_table(self, schema_name, table_name):
        deriva_catalog = _dm.DerivaCatalog(None, None, None, ermrest_catalog=self.ermrest_catalog, validate=False)
        deriva_schema = deriva_catalog.schema(schema_name)
        deriva_table = deriva_schema.table(table_name)
        deriva_table.delete()

    def _do_link_tables(self, schema_name, table_name, target_schema_name, target_table_name):
        """Link tables in the catalog."""
        deriva_catalog = _dm.DerivaCatalog(None, None, None, ermrest_catalog=self.ermrest_catalog, validate=False)
        source_table = deriva_catalog.schema(schema_name).table(table_name)
        target_table = deriva_catalog.schema(target_schema_name).table(target_table_name)
        source_table.link_tables(target_table)

    def _do_associate_tables(self, schema_name, table_name, target_schema_name, target_table_name):
        """Associate tables in the catalog."""
        deriva_catalog = _dm.DerivaCatalog(None, None, None, ermrest_catalog=self.ermrest_catalog, validate=False)
        source_table = deriva_catalog.schema(schema_name).table(table_name)
        target_table = deriva_catalog.schema(target_schema_name).table(target_table_name)
        source_table.associate_tables(target_table)

    @classmethod
    def _deriva_column_from_column_doc(cls, column_doc):
        """Converts a column doc into a DerivaColumn object."""
        return _dm.DerivaColumn.define(
            column_doc['name'],
            column_doc['type']['typename'],
            nullok=column_doc['nullok'],
            default=column_doc['default'],
            acls=column_doc['acls'],
            acl_bindings=column_doc['acl_bindings'],
            annotations=column_doc['annotations']
        )

    @classmethod
    def _deriva_key_from_key_doc(cls, deriva_catalog, key_doc):
        """Converts a key doc into a DerivaKey object."""
        return _dm.DerivaKey(
            None,
            key_doc['unique_columns'],
            name=tuple(key_doc['names'][0]) if len(key_doc['names']) > 0 else None,
            comment=key_doc['comment'],
            annotations=key_doc['annotations'],
            define=True
        )

    @classmethod
    def _deriva_foreign_key_from_foreign_key_doc(cls, deriva_catalog, fkey_doc):
        assert fkey_doc['referenced_columns'], 'No referenced columns specified'

        dest_table = cls._DerivaTablePrototype(
            deriva_catalog.schema(fkey_doc['referenced_columns'][0]['schema_name']),
            fkey_doc['referenced_columns'][0]['table_name']
        )

        return _dm.DerivaForeignKey.define(
            [fkey_col['column_name'] for fkey_col in fkey_doc['foreign_key_columns']],
            dest_table,  # destination table
            [fkey_col['column_name'] for fkey_col in fkey_doc['referenced_columns']],  # destination column names
            name=fkey_doc['names'][0][1] if len(fkey_doc['names']) > 0 else None,
            comment=fkey_doc.get('comment', None),
            on_update=fkey_doc.get('on_update', 'NO ACTION'),
            on_delete=fkey_doc.get('on_delete', 'NO ACTION'),
            acls=fkey_doc.get('acls', {}),
            acl_bindings=fkey_doc.get('acl_bindings', {}),
            annotations=fkey_doc.get('annotations', {})
        )
