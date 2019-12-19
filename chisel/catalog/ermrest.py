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

            orig_sname, orig_tname = plan.src_sname, plan.src_tname
            altered_schema_name, altered_table_name = plan.dst_sname, plan.dst_tname
            self._do_alter_table(orig_sname, orig_tname, altered_schema_name, altered_table_name, plan.projection)

            #  invalidate the original table model object
            invalidated_table = self.schemas[orig_sname].tables._backup[orig_tname]
            invalidated_table.valid = False  # TODO: ideally, repair rather than invalidate in the 'Alter' path
            del self.schemas[orig_sname].tables._backup[orig_tname]

            #  introspect the schema on the revised table
            model_doc = self.ermrest_catalog.getCatalogSchema()
            table_doc = model_doc['schemas'][altered_schema_name]['tables'][altered_table_name]
            schema = self.schemas[altered_schema_name]
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

        elif src_table_name != dst_table_name:
            logger.debug("Altering table name from '{old}' to '{new}'".format(old=src_table_name, new=dst_table_name))
            table.alter(table_name=dst_table_name)

        elif projection[0] == optimizer.AllAttributes():  # 'special' case for drops or adds only
            logger.debug("Dropping columns that were explicitly removed.")
            for item in projection[1:]:
                if isinstance(item, optimizer.AttributeDrop):
                    logger.debug("Dropping column '{cname}'.".format(cname=item.name))
                    original_columns[item.name].drop()
                elif isinstance(item, optimizer.AttributeAdd):
                    col_doc = json.loads(item.definition)
                    logger.debug("Adding column '{cname}'.".format(cname=col_doc['name']))
                    table.create_column(col_doc)
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
                else:
                    assert isinstance(projected, str)
                    projected_column_names.add(projected)

            # step 2: remove columns that were not projected
            logger.debug("Dropping columns not in the projection.")
            for column in original_columns.values():
                if column.name not in projected_column_names | self.syscols:
                    logger.debug("Dropping column '{cname}'.".format(cname=column.name))
                    column.drop()

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

    @property
    def logical_plan(self):
        """The logical plan used to compute this relation; intended for internal use."""
        return optimizer.ERMrestExtant(self.schema.catalog, self.schema.name, self.name)
