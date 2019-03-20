"""Catalog model for remote ERMrest catalog services."""

from deriva import core as _deriva_core
from deriva.core import ermrest_model as _em
from .. import optimizer
from .. import util
from . import base

#: instance wide setting for providing system columns when creating new tables (default: True)
provide_system = True


def _kwargs(**kwargs):
    """Helper for extending module with sub-types for the whole model tree."""
    kwargs2 = {
        'schema_class': base.Schema,
        'table_class': ERMrestTable,
        'column_class': base.Column
    }
    kwargs2.update(kwargs)
    return kwargs2


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
    return from_ermrest_catalog(ec)


def from_ermrest_catalog(ermrest_catalog):
    """Returns a database catalog instance backed by a remote ERMrest catalog service.

    :param ermrest_catalog: `ErmrestCatalog` instance from the deriva-py package
    :return: a database catalog instance from the chisel package
    """
    return ERMrestCatalog(ermrest_catalog.getCatalogSchema(), ermrest_catalog=ermrest_catalog)


class ERMrestCatalog(base.AbstractCatalog):
    """Database catalog backed by a remote ERMrest catalog service."""
    def __init__(self, model_doc, **kwargs):
        super(ERMrestCatalog, self).__init__(model_doc, **_kwargs(**kwargs))
        self.ermrest_catalog = kwargs['ermrest_catalog'] if 'ermrest_catalog' in kwargs else None

    def _materialize_relation(self, schema, plan):
        """Materializes a relation from a physical plan.

        :param schema: a `Schema` in which to materialize the relation
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
            provide_system=provide_system
        )
        # Create table
        schema.create_table(self.ermrest_catalog, tab_def)
        # Unfortunately, the 'paths' interface must be rebuilt for every relation to be materialized because the remote
        # schema itself is changing (by definition) throughout the `commit` process.
        paths = self.ermrest_catalog.getPathBuilder()
        new_table = paths.schemas[schema.name].tables[plan.description['table_name']]
        # Insert data
        new_table.insert(plan)


class ERMrestTable (base.AbstractTable):
    """Extant table in an ERMrest catalog."""
    def __init__(self, sname, tname, table_doc, **kwargs):
        super(ERMrestTable, self).__init__(sname, tname, table_doc, **_kwargs(**kwargs, table=self))
        self.schema = kwargs['schema']

    @property
    def logical_plan(self):
        """The logical plan used to compute this relation; intended for internal use."""
        return optimizer.Extant(self)
