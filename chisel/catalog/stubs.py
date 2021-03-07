"""Catalog model stubs.
"""
from deriva.core import ermrest_model as _erm


class CatalogStub (object):
    """Stubbed out catalog to simulate ErmrestCatalog interfaces used by catalog model objects.
    """

    __not_implemented_message__ = 'The model object does not support this method.'

    def get(self, path):
        raise NotImplementedError(CatalogStub.__not_implemented_message__)

    def put(self, path, json=None):
        raise NotImplementedError(CatalogStub.__not_implemented_message__)

    def post(self, path, json=None):
        raise NotImplementedError(CatalogStub.__not_implemented_message__)

    def delete(self, path):
        raise NotImplementedError(CatalogStub.__not_implemented_message__)


class ModelStub (_erm.Model):
    """Stubbed out subclass of `ermrest_model.Model` for model document subsets.
    """

    def digest_fkeys(self):
        """Stubbed out method to allow unresolvable foreign keys in an incomplete model document.
        """
        return


class SchemaStub (object):
    """Stubbed out schema to simulate minimal ermrest_model.Schema.
    """

    def __init__(self, name):
        """Initializes the schema stub.

        :param name: name of the schema
        """
        super(SchemaStub, self).__init__()
        self.name = name
