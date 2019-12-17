"""Catalog model for ERMrest based on Deriva Core library."""

import logging
from deriva import core as _deriva_core
from deriva.core import ermrest_model as _em
from .. import optimizer
from .. import operators
from .. import util
from . import base

logger = logging.getLogger(__name__)


def connect(url, credentials=None, use_deriva_catalog_manage=False):
    """Connect to an ERMrest data source.

    :param url: connection string url
    :param credentials: user credentials
    :param use_deriva_catalog_manage: flag to use deriva catalog manage rather than deriva core only (default: `False`)
    :return: catalog for data source
    """
    parsed_url = util.urlparse(url)
    if not credentials:
        credentials = _deriva_core.get_credential(parsed_url.netloc)
    ec = _deriva_core.ErmrestCatalog(parsed_url.scheme, parsed_url.netloc, parsed_url.path.split('/')[-1], credentials)
    if use_deriva_catalog_manage:
        from .deriva import DerivaCatalog
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

            # TODO: need to get -- orig_sname, orig_tname = plan.child.description['...'], plan.child.description['...']
            altered_schema_name, altered_table_name = plan.description['schema_name'], plan.description['table_name']
            self._do_alter_table(altered_schema_name, altered_table_name, plan.projection)  # TODO: orig_sname/tname, ...

            # Note: repair the model following the alter table
            #  invalidate the altered table model object
            schema = self.schemas[altered_schema_name]
            invalidated_table = self[altered_schema_name].tables._backup[altered_table_name]  # TODO: get the original table
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
        schema.create_table(table_doc)

    def _do_move_table(self, src_schema_name, src_table_name, dst_schema_name, dst_table_name):
        """Rename table in the catalog."""
        src_schema = self.schemas[src_schema_name]
        src_table = src_schema.tables[src_table_name]
        dst_schema = self.schemas[dst_schema_name]
        with self.evolve():  # TODO: should refactor this so that it doesn't have to be performed in a evolve block
            dst_schema.tables[dst_table_name] = src_table.select()
        with self.evolve(allow_drop=True):  # TODO: remove undo block here
          del src_schema.tables[src_table_name]

    # TODO: should not need this if handled via projection and rules to infer an 'alter' mode
    def _do_alter_table_add_column(self, schema_name, table_name, column_doc):
        """Alter table Add column in the catalog"""
        model = self.ermrest_catalog.getCatalogModel()
        ermrest_table = model.schemas[schema_name].tables[table_name]
        ermrest_table.create_column(column_doc)

    def _do_alter_table(self, schema_name, table_name, projection):  # TODO: sname, tname, new_sname, new_tname, projection
        """Alter table (general) in the catalog."""
        model = self.ermrest_catalog.getCatalogModel()
        schema = model.schemas[schema_name]  # TODO: sname
        table = schema.tables[table_name]    # TODO: tname
        original_columns = {c.name: c for c in table.column_definitions}

        # Notes: currently, there are two distinct scenarios in a projection,
        #  1) 'general' case: the projection is an improper subset of the relation's columns, and may include some
        #     aliased columns from the original columns. Also, columns may be aliased more than once.
        #  2) 'special' case for deletes only: as a syntactic sugar, many formulations of project support the
        #     notation of "-foo,-bar,..." meaning that the operator will project all _except_ those '-name' columns.
        #     We support that by first including the special symbol 'AllAttributes' followed by 'AttributeRemoval'
        #     symbols.

        # TODO:
        #  3) change schema name
        #  4) change table name

        # if sname != new_sname:  # rename schema name
        #     # ...
        #
        # elif tname != new_tname:  # rename table name
        #     # ...

        # elif ...  TODO: will probably be mutually exclusive with column renames
        if projection[0] == optimizer.AllAttributes():  # 'special' case for deletes only
            logger.debug("Dropping columns that were explicitly removed.")
            for removal in projection[1:]:
                assert isinstance(removal, optimizer.AttributeRemoval)
                logger.debug("Deleting column '{cname}'.".format(cname=removal.name))
                original_columns[removal.name].drop()

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
                    table.create_column(cloned_def)
                    # 1.b: get the datapath table for column
                    pb = self.ermrest_catalog.getPathBuilder()
                    dp_table = pb.schemas[table.schema.name].tables[table.name]
                    # 1.c: read the RID,column values
                    data = dp_table.attributes(
                        dp_table.column_definitions['RID'],
                        dp_table.column_definitions[projected.name].alias(projected.alias)
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
                    column.drop()

    def _do_drop_table(self, schema_name, table_name):
        """Drop table in the catalog."""
        # Delete table from the ermrest catalog
        model = self.ermrest_catalog.getCatalogModel()
        schema = model.schemas[schema_name]
        table = schema.tables[table_name]
        table.drop()

    def _do_link_tables(self, schema_name, table_name, target_schema_name, target_table_name):
        """Link tables in the catalog."""
        # TODO: may need to introduce new Link operator
        #       projection of table +column(s) needed as the foriegn key, inference of key columns from target table
        #       add'l physical operation to add the fkey reference
        raise NotImplementedError('Not supported by %s.' % type(self).__name__)

    def _do_associate_tables(self, schema_name, table_name, target_schema_name, target_table_name):
        """Associate tables in the catalog."""
        # TODO: may need to introduce new Associate operator
        #       project of new table w/ column(s) for each foriegn key to inferred keys of target tables
        #       add'l physical operation to add the fkey reference(s)
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

    # TODO: will not need this when abstract table is updated
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
    def link(self, target):
        """Creates a reference from this table to the target table."""
        with self.schema.catalog.evolve():  # TODO: get rid of this evolve block
            # TODO: eventually the _do_... statements should just be moved here
            self.schema.catalog._do_link_tables(self.schema.name, self.name, target.schema.name, target.name)

    @base.valid_model_object
    def associate(self, target):
        """Creates a many-to-many "association" between this table and "target" table."""
        with self.schema.catalog.evolve():  # TODO: get rid of this evolve block
            # TODO: eventually the _do_... statements should just be moved here
            self.schema.catalog._do_associate_tables(self.schema.name, self.name, target.schema.name, target.name)
