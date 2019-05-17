"""Catalog model for remote ERMrest catalog services."""

import logging
from deriva import core as _deriva_core
from deriva.core import ermrest_model as _em
from .. import optimizer
from .. import operators
from .. import util
from . import base

logger = logging.getLogger(__name__)


def connect(url, credentials=None):
    """Connect to an ERMrest data source.

    :param url: connection string url
    :param credentials: user credentials
    :return: catalog for data source
    """
    parsed_url = util.urlparse(url)
    if not credentials:
        credentials = _deriva_core.get_credential(parsed_url.netloc)
    ec = _deriva_core.ErmrestCatalog(parsed_url.scheme, parsed_url.netloc, parsed_url.path.split('/')[-1], credentials)
    return ERMrestCatalog(ec)


class ERMrestCatalog (base.AbstractCatalog):
    """Database catalog backed by a remote ERMrest catalog service."""

    """instance wide setting for providing system columns when creating new tables (default: True)"""
    provide_system = True

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
            model = self.ermrest_catalog.getCatalogModel()
            schema = model.schemas[plan.description['schema_name']]
            table = schema.tables[plan.description['table_name']]
            columns = table.column_definitions
            if plan.projection[0] != optimizer.AllAttributes():
                logger.debug("Dropping columns not in the projection.")
                for column in columns:
                    if column.name not in plan.projection + ('RID', 'RCB', 'RMB', 'RCT', 'RMT'):
                        logger.debug("Deleting column '{cname}'.".format(cname=column.name))
                        column.delete(self.ermrest_catalog)
            else:
                logger.debug("Dropping columns that were explicitly removed.")
                for removal in plan.projection[1:]:
                    assert isinstance(removal, optimizer.AttributeRemoval)
                    logger.debug("Deleting column '{cname}'.".format(cname=removal.name))
                    columns[removal.name].delete(self.ermrest_catalog)

        elif isinstance(plan, operators.Assign):
            # Redefine table from plan description (allows us to provide system columns)
            desc = plan.description
            tab_def = _em.Table.define(
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
            # TODO: it should be possible to only refresh the model and paths each evolve context since destructive
            #  operations must be performed in isolation
            schema = self.ermrest_catalog.getCatalogModel().schemas[plan.description['schema_name']]
            schema.create_table(self.ermrest_catalog, tab_def)
            paths = self.ermrest_catalog.getPathBuilder()
            new_table = paths.schemas[schema.name].tables[plan.description['table_name']]
            # Insert data
            new_table.insert(plan)
        else:
            raise ValueError('Plan cannot be materialized.')

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


class ERMrestTable (base.AbstractTable):
    """Extant table in an ERMrest catalog."""

    @property
    def logical_plan(self):
        """The logical plan used to compute this relation; intended for internal use."""
        return optimizer.ERMrestExtant(self.schema.catalog, self.schema.name, self.name)
