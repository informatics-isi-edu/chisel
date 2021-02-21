"""Catalog model for ERMrest based on Deriva Core library."""

import json
import logging
from deriva import core as _deriva_core
from deriva.core import ermrest_model as _em
from .. import optimizer
from .. import operators
from .. import util
from . import base

logger = logging.getLogger(__name__)


class ERMrestCatalog (base.AbstractCatalog):
    """Database catalog backed by a remote ERMrest catalog service."""

    """instance wide setting for providing system columns when creating new tables (default: True)"""
    provide_system = True

    """The set of system columns."""
    syscols = {'RID', 'RCB', 'RMB', 'RCT', 'RMT'}

    def __init__(self, url, credentials):
        # establish connection to ermrest catalog and call super init
        parsed_url = util.urlparse(url)
        if not credentials:
            credentials = _deriva_core.get_credential(parsed_url.netloc)
        self.ermrest_catalog = _deriva_core.ErmrestCatalog(
            parsed_url.scheme, parsed_url.netloc, parsed_url.path.split('/')[-1], credentials
        )
        self.ermrest_catalog.dcctx['cid'] = "api/chisel"
        super(ERMrestCatalog, self).__init__(self.ermrest_catalog.getCatalogSchema())

    def _repair_model(self, schema_name, table_name):
        """Repair catalog model for recently created, assigned, or altered table

        :param schema_name: schema name
        :param table_name: table name
        :return: new table instance
        """
        # get table from catalog schema
        model_doc = self.ermrest_catalog.getCatalogSchema()
        table_doc = model_doc['schemas'][schema_name]['tables'][table_name]
        schema = self.schemas[schema_name]
        # instantiate new table model object
        table = schema._new_table_instance(table_doc)
        # add to schema tables backing collection
        schema.tables._backup[table_name] = table  # TODO: this part is kludgy and needs to be revised
        # TODO: refresh the referenced_by of the catalog
        return table

    def _materialize_relation(self, plan):
        """Materializes a relation from a physical plan.

        :param plan: a `PhysicalOperator` instance from which to materialize the relation
        :return: None
        """
        if isinstance(plan, operators.Create):
            schema_name, table_name = plan.description['schema_name'], plan.description['table_name']
            logger.debug(("Creating table '%s.%s'" % (schema_name, table_name)))

            # Create table
            self._do_create_table(schema_name, plan.description)
            # TODO: mmo to introduce table into mappings (ie, viz-fkeys) per its relationships

            # Repair catalog model
            self._repair_model(schema_name, table_name)

        elif isinstance(plan, operators.Alter):
            logger.debug("Altering table '{tname}'.".format(tname=plan.description['table_name']))
            if not self._evolve_ctx.allow_alter:
                raise base.CatalogMutationError('"allow_alter" flag is not True')

            orig_sname, orig_tname = plan.src_sname, plan.src_tname
            altered_schema_name, altered_table_name = plan.dst_sname, plan.dst_tname
            self._do_alter_table(orig_sname, orig_tname, altered_schema_name, altered_table_name, plan.projection)
            # TODO: mmo to rename paths in mappings and acls, per table rename ("rename" or "move")

            #  invalidate the original table model object
            invalidated_table = self.schemas[orig_sname].tables._backup[orig_tname]
            invalidated_table.valid = False  # TODO: ideally, repair rather than invalidate in the 'Alter' path
            del self.schemas[orig_sname].tables._backup[orig_tname]

            # Repair catalog model
            self._repair_model(altered_schema_name, altered_table_name)

        elif isinstance(plan, operators.Drop):
            logger.debug("Dropping table '{tname}'.".format(tname=plan.description['table_name']))
            if not self._evolve_ctx.allow_drop:
                raise base.CatalogMutationError('"allow_drop" flag is not True')

            dropped_schema_name, dropped_table_name = plan.description['schema_name'], plan.description['table_name']
            self._do_drop_table(dropped_schema_name, dropped_table_name)
            # TODO: mmo to remove mappings and acls that depend on the table

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
            self._repair_model(assigned_schema_name, assigned_table_name)

        else:
            raise ValueError('Plan cannot be materialized.')

    def _do_create_table(self, schema_name, table_doc):
        """Create table in the catalog."""
        schema = self.ermrest_catalog.getCatalogModel().schemas[schema_name]
        schema.create_table(table_doc)

    def _do_alter_table(self, src_schema_name, src_table_name, dst_schema_name, dst_table_name, projection):
        """Alter table (general) in the catalog."""
        model = self.ermrest_catalog.getCatalogModel()
        schema = model.schemas[src_schema_name]
        table = schema.tables[src_table_name]
        original_columns = {c.name: c for c in table.column_definitions}

        # Note: currently, there are distinct scenarios in an alter,
        #  - schema change
        #  - table name change
        #  - 'special' case for add/drop only projections
        #  - 'general' case for arbitrary attribute projections

        if src_schema_name != dst_schema_name:
            logger.debug("Altering table name from schema '{old}' to '{new}'".format(old=src_schema_name, new=dst_schema_name))
            table.alter(schema_name=dst_schema_name)
            # TODO: mmo fix mappings and acls, per schema name change

        elif src_table_name != dst_table_name:
            logger.debug("Altering table name from '{old}' to '{new}'".format(old=src_table_name, new=dst_table_name))
            table.alter(table_name=dst_table_name)
            # TODO: mmo fix mappings and acls, per table name change

        elif projection[0] == optimizer.AllAttributes():  # 'special' case for drops or adds only
            logger.debug("Dropping columns that were explicitly removed.")
            for item in projection[1:]:
                if isinstance(item, optimizer.AttributeDrop):
                    logger.debug("Dropping column '{cname}'.".format(cname=item.name))
                    original_columns[item.name].drop()
                    # TODO: mmo to remove mappings and acls that depend on dropped column
                elif isinstance(item, optimizer.AttributeAdd):
                    col_doc = json.loads(item.definition)
                    logger.debug("Adding column '{cname}'.".format(cname=col_doc['name']))
                    table.create_column(col_doc)
                    # TODO: mmo to add column to mappings' projects, as defined
                else:
                    raise AssertionError("Unexpected '%s' in alter operation" % type(item).__name__)

        else:  # 'general' case

            # step 1: copy aliased columns, and record nonaliased column names
            logger.debug("Copying 'aliased' columns in the projection")
            projected_column_names = set()
            for projected in projection:
                if isinstance(projected, optimizer.AttributeAlias):
                    original_column = original_columns[projected.name]
                    original_column.alter(name=projected.alias)
                    projected_column_names.add(projected.alias)
                    # TODO: mmo to fix mappings and acls per column name change
                else:
                    assert isinstance(projected, str)
                    projected_column_names.add(projected)

            # step 2: remove columns that were not projected
            logger.debug("Dropping columns not in the projection.")
            for column in original_columns.values():
                if column.name not in projected_column_names | self.syscols:
                    logger.debug("Dropping column '{cname}'.".format(cname=column.name))
                    column.drop()
                    # TODO: mmo to remove mappings and acls that depend on dropped column

    def _do_drop_table(self, schema_name, table_name):
        """Drop table in the catalog."""
        # Delete table from the ermrest catalog
        model = self.ermrest_catalog.getCatalogModel()
        schema = model.schemas[schema_name]
        table = schema.tables[table_name]
        table.drop()

    def _determine_model_changes(self, computed_relation):
        """Determines the model changes to be produced by this computed relation."""
        return dict(mappings=[], constraints=[], policies=[])

    def _relax_model_constraints(self, model_changes):
        """Relaxes model constraints in the prior conditions of the model changes."""
        pass

    def _apply_model_changes(self, model_changes):
        """Apply model changes in the post conditions of the model changes."""
        pass

    def logical_plan(self, table):
        """Symbolic representation of the table extant; intended for internal use."""
        return optimizer.ERMrestExtant(self, table.schema.name, table.name)
