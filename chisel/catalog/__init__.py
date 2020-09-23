"""Catalog package."""
from .. import optimizer as _op
from . import base as _base
from .ermrest import ERMrestCatalog
from .semistructured import SemistructuredCatalog
from .base import CatalogMutationError, data_types, Schema, Table, Column, Key, ForeignKey


_connection_mapping = {
    'https': ERMrestCatalog,
    'http': ERMrestCatalog,
    'file': SemistructuredCatalog
}
"""Mappings from URI scheme to concrete catalog class."""


def connect(url, credentials=None, **kwargs):
    """Connect to a data source.

    The `connect` function will attempt to connect to the remote or local data
    source, introspect its schema, and return a catalog object which can be
    used to access the objects that represent the catalog schema.

    Two types of data sources are currently supported:

    - ERMrest: chisel can be used to connect an ERMrest data source. It should
      should be specified by an http(s) URL up to its catalog identifier.
      ```
      catalog = chisel.connect('https://example.org/ermrest/catalog/1')
      ```

    - Semistructured: chisel can be used to connect to a local catalog
      consisting of a shallow hierarchy of semistructured tabular data in
      either CSV or JSON format. It should be specified by a file URL up
      to the base data directory. Files may reside in the base data dir or
      up to one directory-level deep under the base data dir.
      ```
      catalog = chisel.connect('file:///path/to/data_dir')
      ```

    For connections to ERMrest, `credentials` may be passed in the `connect`
    function. Use the `deriva.core.get_credentails` function to get the user
    credential object.

    :param url: connection string url
    :param credentials: user credentials (optional)
    :return: catalog for data source
    """
    for scheme, clz in _connection_mapping.items():
        if url.startswith(scheme + ':'):
            return clz(url, credentials)
    else:
        raise ValueError("Unknown connection scheme")


def shred(graph, expression):
    """Shreds graph data (e.g., RDF, JSON-LD, etc.) into relational (tabular) data structure as a computed relation.

    :param graph: a filename of an RDF jsonld graph or a parsed rdflib.Graph instance
    :param expression: text of a SPARQL query statement
    :return: a computed relation object
    """
    if not graph:
        raise ValueError("Invalid value for 'graph'")
    if not expression:
        raise ValueError("Invalid value for 'expression'")
    return _base.ComputedRelation(_op.Shred(graph, expression))
