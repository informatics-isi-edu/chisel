"""Catalog model for remote ERMrest catalog services."""

from deriva import core as _deriva_core
from deriva.core import ermrest_model as _em
from .. import optimizer
from .. import util
from . import base


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
        # Redefine table from plan description (allows us to provide system columns)
        desc = plan.description
        tab_def = _em.Table.define(
            desc['table_name'],
            column_defs=desc['column_definitions'],
            key_defs=desc['keys'],
            fkey_defs=desc['foreign_keys'],
            comment=desc['comment'],
            acls=desc['acls'] if 'acls' in desc else {},
            acl_bindings=desc['acl_bindings'] if 'acl_bindings' in desc else {},
            annotations=desc['annotations'] if 'annotations' in desc else {},
            provide_system=ERMrestCatalog.provide_system
        )
        # Create table
        # TODO: the following needs testing after changes :: also should improve efficiency here
        schema = self.ermrest_catalog.getCatalogSchema().schemas[plan.description['schema_name']]
        schema.create_table(self.ermrest_catalog, tab_def)
        # Unfortunately, the 'paths' interface must be rebuilt for every relation to be materialized because the remote
        # schema itself is changing (by definition) throughout the `commit` process.
        paths = self.ermrest_catalog.getPathBuilder()  # TODO: also look to improve efficiency here too
        new_table = paths.schemas[schema.name].tables[plan.description['table_name']]
        # Insert data
        new_table.insert(plan)


class ERMrestSchema (base.Schema):
    """Represents a 'schema' (a.k.a., a namespace) in a database catalog."""

    def _new_table_instance(self, table_doc):
        return ERMrestTable(table_doc, self)


class ERMrestTable (base.AbstractTable):
    """Extant table in an ERMrest catalog."""

    @property
    def logical_plan(self):
        """The logical plan used to compute this relation; intended for internal use."""
        return optimizer.Extant(self)
