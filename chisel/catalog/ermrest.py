"""Catalog model for remote ERMrest catalog services."""

import collections
import logging
from deriva import core as _deriva_core
from deriva.core import ermrest_model as _em
from deriva.utils.catalog.components import deriva_model as _dm
from .. import optimizer
from .. import operators
from .. import util
from . import base

logger = logging.getLogger(__name__)


def connect(url, credentials=None, use_deriva_catalog_manage=True):
    """Connect to an ERMrest data source.

    :param url: connection string url
    :param credentials: user credentials
    :param use_deriva_catalog_manage: flag to use deriva catalog manage rather than deriva core only (default: `True`)
    :return: catalog for data source
    """
    parsed_url = util.urlparse(url)
    if not credentials:
        credentials = _deriva_core.get_credential(parsed_url.netloc)
    ec = _deriva_core.ErmrestCatalog(parsed_url.scheme, parsed_url.netloc, parsed_url.path.split('/')[-1], credentials)
    if use_deriva_catalog_manage:
        return DerivaCatalog(ec)
    else:
        return ERMrestCatalog(ec)


class ERMrestCatalog (base.AbstractCatalog):
    """Database catalog backed by a remote ERMrest catalog service."""

    """instance wide setting for providing system columns when creating new tables (default: True)"""
    provide_system = True

    """The set of system columns."""
    syscols = {'RID', 'RCB', 'RMB', 'RCT', 'RMT'}

    def __init__(self, ermrest_catalog):
        super(ERMrestCatalog, self).__init__(ermrest_catalog.getCatalogSchema())
        self.ermrest_catalog = ermrest_catalog

    def _new_schema_instance(self, schema_doc):
        return ERMrestSchema(schema_doc, self)

    def _materialize_relation(self, plan):
        """Materializes a relation from a physical plan.

        :param plan: a `PhysicalOperator` instance from which to materialize the relation
        :return: None
        """
        if isinstance(plan, operators.Alter):
            logger.debug("Altering table '{tname}'.".format(tname=plan.description['table_name']))
            if not self._evolve_ctx.allow_alter:
                raise base.CatalogMutationError('"allow_alter" flag is not True')

            altered_schema_name, altered_table_name = plan.description['schema_name'], plan.description['table_name']
            self._do_alter_table(altered_schema_name, altered_table_name, plan.projection)

            # Note: repair the model following the alter table
            #  invalidate the altered table model object
            schema = self.schemas[altered_schema_name]
            invalidated_table = self[altered_schema_name].tables._backup[altered_table_name]  # get the original table
            invalidated_table.valid = False
            #  introspect the schema on the revised table
            model_doc = self.ermrest_catalog.getCatalogSchema()
            table_doc = model_doc['schemas'][altered_schema_name]['tables'][altered_table_name]
            table = ERMrestTable(table_doc, schema=schema)
            schema.tables._backup[altered_table_name] = table  # TODO: this part is kludgy and needs to be revised
            # TODO: refresh the referenced_by of the catalog

        elif isinstance(plan, operators.Drop):
            logger.debug("Dropping table '{tname}'.".format(tname=plan.description['table_name']))
            if not self._evolve_ctx.allow_drop:
                raise base.CatalogMutationError('"allow_drop" flag is not True')

            dropped_schema_name, dropped_table_name = plan.description['schema_name'], plan.description['table_name']
            self._do_drop_table(dropped_schema_name, dropped_table_name)

            # Note: repair the model following the drop table
            #  invalidate the dropped table model object
            schema = self.schemas[dropped_schema_name]
            dropped_table = schema.tables[dropped_table_name]
            dropped_table.valid = False
            #  remove dropped table model object from schema
            del schema.tables._backup[dropped_table_name]  # TODO: this part is kludgy and needs to be revised
            # TODO: refresh the referenced_by of the catalog

        elif isinstance(plan, operators.Assign):
            logger.debug("Creating table '{tname}'.".format(tname=plan.description['table_name']))
            assigned_schema_name, assigned_table_name = plan.description['schema_name'], plan.description['table_name']

            # Redefine table from plan description (allows us to provide system columns)
            desc = plan.description
            table_doc = _em.Table.define(
                desc['table_name'],
                column_defs=desc['column_definitions'],
                key_defs=desc['keys'],
                fkey_defs=desc['foreign_keys'],
                comment=desc['comment'],
                acls=desc.get('acls', {}),
                acl_bindings=desc.get('acl_bindings', {}),
                annotations=desc.get('annotations', {}),
                provide_system=ERMrestCatalog.provide_system
            )

            # Create table
            self._do_create_table(plan.description['schema_name'], table_doc)

            # Insert tuples in new table
            paths = self.ermrest_catalog.getPathBuilder()
            new_table = paths.schemas[assigned_schema_name].tables[assigned_table_name]
            # ...Determine the nondefaults for the insert
            new_table_cnames = set([col['name'] for col in desc['column_definitions']])
            nondefaults = {'RID', 'RCB', 'RCT'} & new_table_cnames
            new_table.insert(plan, nondefaults=nondefaults)

            # Repair catalog model
            #  ...introspect the schema on the revised table
            model_doc = self.ermrest_catalog.getCatalogSchema()
            table_doc = model_doc['schemas'][assigned_schema_name]['tables'][assigned_table_name]
            schema = self.schemas[assigned_schema_name]
            table = schema._new_table_instance(table_doc)
            schema.tables._backup[assigned_table_name] = table  # TODO: this part is kludgy and needs to be revised
            # TODO: refresh the referenced_by of the catalog

        else:
            raise ValueError('Plan cannot be materialized.')

    def _do_create_table(self, schema_name, table_doc):
        """Create table in the catalog."""
        schema = self.ermrest_catalog.getCatalogModel().schemas[schema_name]
        schema.create_table(self.ermrest_catalog, table_doc)

    def _do_copy_table(self, src_schema_name, src_table_name, dst_schema_name, dst_table_name):
        """Copy table in the catalog."""
        src_schema = self.schemas[src_schema_name]
        dst_schema = self.schemas[dst_schema_name]
        src_table = src_schema.tables[src_table_name]
        dst_schema.tables[dst_table_name] = src_table.select()  # requires that this be performed w/in evolve block

    def _do_move_table(self, src_schema_name, src_table_name, dst_schema_name, dst_table_name):
        """Rename table in the catalog."""
        src_schema = self.schemas[src_schema_name]
        src_table = src_schema.tables[src_table_name]
        dst_schema = self.schemas[dst_schema_name]
        with self.evolve():  # TODO: should refactor this so that it doesn't have to be performed in a evolve block
            dst_schema.tables[dst_table_name] = src_table.select()
        del src_schema.tables[src_table_name]

    def _do_alter_table_add_column(self, schema_name, table_name, column_doc):
        """Alter table Add column in the catalog"""
        model = self.ermrest_catalog.getCatalogModel()
        ermrest_table = model.schemas[schema_name].tables[table_name]
        ermrest_table.create_column(self.ermrest_catalog, column_doc)

    def _do_alter_table(self, schema_name, table_name, projection):
        """Alter table (general) in the catalog."""
        model = self.ermrest_catalog.getCatalogModel()
        schema = model.schemas[schema_name]
        table = schema.tables[table_name]
        original_columns = {c.name: c for c in table.column_definitions}

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
                original_columns[removal.name].delete(self.ermrest_catalog)

        else:  # 'general' case

            # step 1: copy aliased columns, and record nonaliased column names
            logger.debug("Copying 'aliased' columns in the projection")
            nonaliased_column_names = set()
            for projected in projection:
                if isinstance(projected, optimizer.AttributeAlias):
                    original_column = original_columns[projected.name]
                    # 1.a: clone the column
                    cloned_def = original_column.prejson()
                    cloned_def['name'] = projected.alias
                    table.create_column(self.ermrest_catalog, cloned_def)
                    # 1.b: get the datapath table for column
                    pb = self.ermrest_catalog.getPathBuilder()
                    dp_table = pb.schemas[table.sname].tables[table.name]
                    # 1.c: read the RID,column values
                    data = dp_table.attributes(
                        dp_table.column_definitions['RID'],
                        **{projected.alias: dp_table.column_definitions[projected.name]}
                    )
                    # 1.d: write the RID,alias values
                    dp_table.update(data)
                else:
                    assert isinstance(projected, str)
                    nonaliased_column_names.add(projected)

            # step 2: remove columns that were not projected
            logger.debug("Dropping columns not in the projection.")
            for column in original_columns.values():
                if column.name not in nonaliased_column_names | self.syscols:
                    logger.debug("Deleting column '{cname}'.".format(cname=column.name))
                    column.delete(self.ermrest_catalog)

    def _do_drop_table(self, schema_name, table_name):
        """Drop table in the catalog."""
        # Delete table from the ermrest catalog
        model = self.ermrest_catalog.getCatalogModel()
        schema = model.schemas[schema_name]
        table = schema.tables[table_name]
        table.delete(self.ermrest_catalog)

    def _do_link_tables(self, schema_name, table_name, target_schema_name, target_table_name):
        """Link tables in the catalog."""
        raise NotImplementedError('Not supported by %s.' % type(self).__name__)

    def _do_associate_tables(self, schema_name, table_name, target_schema_name, target_table_name):
        """Associate tables in the catalog."""
        raise NotImplementedError('Not supported by %s.' % type(self).__name__)

    def _determine_model_changes(self, computed_relation):
        """Determines the model changes to be produced by this computed relation."""
        return dict(mappings=[], constraints=[], policies=[])

    def _relax_model_constraints(self, model_changes):
        """Relaxes model constraints in the prior conditions of the model changes."""
        pass

    def _apply_model_changes(self, model_changes):
        """Apply model changes in the post conditions of the model changes."""
        pass


class ERMrestSchema (base.Schema):
    """Represents a 'schema' (a.k.a., a namespace) in a database catalog."""

    def _new_table_instance(self, table_doc):
        return ERMrestTable(table_doc, self)

    @base.valid_model_object
    def _create_table(self, table_doc):
        """ERMrest specific implementation of create table function."""

        # Revise table doc to include sys columns, per static flag
        table_doc_w_syscols = _em.Table.define(
            table_doc['table_name'],
            column_defs=table_doc.get('column_definitions', []),
            key_defs=table_doc.get('keys', []),
            fkey_defs=table_doc.get('foreign_keys', []),
            comment=table_doc.get('comment', None),
            acls=table_doc.get('acls', {}),
            acl_bindings=table_doc.get('acl_bindings', {}),
            annotations=table_doc.get('annotations', {}),
            provide_system=ERMrestCatalog.provide_system
        )

        # Create the table using evolve block for isolation
        with self.catalog.evolve():
            self.catalog._do_create_table(self.name, table_doc_w_syscols)
            return self._new_table_instance(table_doc_w_syscols)


class ERMrestTable (base.Table):
    """Extant table in an ERMrest catalog."""

    @base.valid_model_object
    def _add_column(self, column_doc):
        """ERMrest specific implementation of add column function."""
        with self.schema.catalog.evolve():
            self.schema.catalog._do_alter_table_add_column(self.schema.name, self.name, column_doc)
            return self._new_column_instance(column_doc)

    @property
    def logical_plan(self):
        """The logical plan used to compute this relation; intended for internal use."""
        return optimizer.ERMrestExtant(self.schema.catalog, self.schema.name, self.name)

    @base.valid_model_object
    def _move(self, dst_schema_name, dst_table_name):
        """An internal method to 'move' a table either to rename it, change its schema, or both.

        :param dst_schema_name: destination schema name, may be same
        :param dst_table_name: destination table name, may be same
        """
        assert self.sname != dst_schema_name or self.name != dst_table_name
        # TODO: should be refactored so that this can be done in an evolve block
        self.schema.catalog._do_move_table(self.schema.name, self.name, dst_schema_name, dst_table_name)
        # repair local model state
        self.valid = False
        if self.name in self.schema.tables._backup:
            del self.schema.tables._backup[self.name]  # TODO: this is kludgy should revise
            self.schema.tables.reset()

    @base.valid_model_object
    def copy(self, table_name, schema_name=None):
        """ERMrest catalog specific implementation of 'copy' method."""
        with self.schema.catalog.evolve():
            self.schema.catalog._do_copy_table(self.schema.name, self.name, schema_name or self.schema.name, table_name)

    @base.valid_model_object
    def link(self, target):
        """Creates a reference from this table to the target table."""
        with self.schema.catalog.evolve():
            self.schema.catalog._do_link_tables(self.schema.name, self.name, target.schema.name, target.name)

    @base.valid_model_object
    def associate(self, target):
        """Creates a many-to-many "association" between this table and "target" table."""
        with self.schema.catalog.evolve():
            self.schema.catalog._do_associate_tables(self.schema.name, self.name, target.schema.name, target.name)


class DerivaCatalog (ERMrestCatalog):
    """ERMrest catalog with implementation using the deriva-catalog-manage package."""

    # Prototype of the `DerivaTable` where a real instance is not needed. It mimics the interface when needed as a
    # parameter to deriva-catalog-manage APIs.
    _DerivaTablePrototype = collections.namedtuple('DerivaTablePrototype', ['schema_name', 'name'])

    def _do_create_table(self, schema_name, table_doc):
        deriva_catalog = _dm.DerivaCatalog(None, None, None, ermrest_catalog=self.ermrest_catalog, validate=False)
        deriva_schema = deriva_catalog.schema(schema_name)

        deriva_schema.create_table(
            table_doc['table_name'],
            column_defs=[self._deriva_column_from_column_doc(col_doc) for col_doc in table_doc['column_definitions']],
            key_defs=[self._deriva_key_from_key_doc(key_doc) for key_doc in table_doc['keys']],
            fkey_defs=[self._deriva_foreign_key_from_foreign_key_doc(fkey_doc) for fkey_doc in table_doc['foreign_keys']],
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
    def _deriva_key_from_key_doc(cls, key_doc):
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
    def _deriva_foreign_key_from_foreign_key_doc(cls, fkey_doc):
        dest_table = cls._DerivaTablePrototype(fkey_doc['referenced_columns'][0][0], fkey_doc['referenced_columns'][0][1])
        return _dm.DerivaForeignKey.define(
            [fkey_col[2] for fkey_col in fkey_doc['foreign_key_columns']],
            dest_table,  # destination table
            [fkey_col[2] for fkey_col in fkey_doc['referenced_columns']],  # destination column names
            name=fkey_doc['names'][0][1] if len(fkey_doc['names']) > 0 else None,
            comment=fkey_doc.get('comment', None),
            on_update=fkey_doc.get('on_update', 'NO ACTION'),
            on_delete=fkey_doc.get('on_delete', 'NO ACTION'),
            acls=fkey_doc('acls', {}),
            acl_bindings=fkey_doc('acl_bindings', {}),
            annotations=fkey_doc('annotations', {})
        )