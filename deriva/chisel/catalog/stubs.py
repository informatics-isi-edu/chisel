"""Catalog model stubs.
"""
from deriva.core import ermrest_model as _erm, DEFAULT_HEADERS


class CatalogStub (object):
    """Stubbed out catalog to simulate ErmrestCatalog interfaces used by catalog model objects.
    """

    def __init__(self, model_doc=None):
        """Initialize catalog stub.

        :param model_doc: ERMrest model document (optional)
        """
        self._model_doc = model_doc

    __not_implemented_message__ = 'The model object does not support this method.'

    def get(self, path, headers=DEFAULT_HEADERS, raise_not_modified=False, stream=False):
        raise Exception(CatalogStub.__not_implemented_message__)

    def put(self, path, data=None, json=None, headers=DEFAULT_HEADERS, guard_response=None):
        raise Exception(CatalogStub.__not_implemented_message__)

    def post(self, path, data=None, json=None, headers=DEFAULT_HEADERS):
        raise Exception(CatalogStub.__not_implemented_message__)

    def delete(self, path, headers=DEFAULT_HEADERS, guard_response=None):
        raise Exception(CatalogStub.__not_implemented_message__)

    def getCatalogModel(self):
        return _erm.Model(self, self._model_doc)


class ModelStub:
    """Simple model stub suitable for limited introspection by the model management operations.
    """

    class _TableStub:
        def __init__(self, schema, table_name, table_doc):
            self.schema = schema
            self.name = table_name
            self.annotations = table_doc.get('annotations', {})
            for fkey_doc in table_doc.get('foreign_keys', []):
                schema._fkeys[fkey_doc['names'][0][1]] = ModelStub._ForeignKeyStub(self, fkey_doc)

    class _SchemaStub:
        def __init__(self, model, schema_name, schema_doc):
            self.model = model
            self.name = schema_name
            self._fkeys = {}
            self.tables = {k: ModelStub._TableStub(self, k, v) for k, v in schema_doc.get('tables', {}).items()}

    class _ForeignKeyStub:
        def __init__(self, table, fkey_doc):
            self.table = table
            self._fkey_doc = fkey_doc
            self.names = fkey_doc.get('names', [])
            self.pk_table = None

    def __init__(self, model_doc):
        # setup null model objects for lookups that are not known to this limited model;
        # these should ensure that attempts to introspect will end without raising exceptions
        unknown_schema = ModelStub._SchemaStub(self, None, {'tables': {None: {}}})
        self._unknown_table = unknown_schema.tables[None]
        self._unknown_fkey = ModelStub._ForeignKeyStub(unknown_schema.tables[None], {})
        # populate the schemas
        self.schemas = {k: ModelStub._SchemaStub(self, k, v) for k, v in model_doc.get('schemas', {}).items()}
        # digest the fkeys
        self._digest_fkeys()

    def _digest_fkeys(self):
        """Populate the pk_table property of fkeys if known to the model."""
        for schema in self.schemas.values():
            for fkey in schema._fkeys.values():
                ref_col = fkey._fkey_doc.get('referenced_columns', [{}])[0]
                pk_sname, pk_tname = ref_col.get('schema_name'), ref_col.get('table_name')
                if pk_sname in self.schemas and pk_tname in self.schemas[pk_sname].tables:
                    fkey.pk_table = self.schemas[pk_sname].tables[pk_tname]
                else:
                    fkey.pk_table = self._unknown_table

    def fkey(self, constraint_name):
        """Returns known fkeys or otherwise the unknown fkey."""
        try:
            return self.schemas[constraint_name[0]]._fkeys[constraint_name[1]]
        except KeyError:
            return self._unknown_fkey

    @classmethod
    def for_table(cls, table_doc):
        """Instantiates a model stub based on a single table doc.
        """
        model_doc = {
            'schemas': {
                table_doc['schema_name']: {
                    'tables': {
                        table_doc['table_name']: table_doc
                    }
                }
            }
        }
        return cls(model_doc)


class SchemaStub (object):
    """Stubbed out schema to simulate minimal ermrest_model.Schema.
    """

    class _ModelStub (object):
        """Model stub within a schema stub.
        """
        def make_extant_symbol(self, s, t):
            return

        def prejson(self):
            return {
                'schemas': {
                    '.': {
                        'tables': {}
                    }
                }
            }

    def __init__(self, name):
        """Initializes the schema stub.

        :param name: name of the schema
        """
        super(SchemaStub, self).__init__()
        self.model = SchemaStub._ModelStub()
        self.name = name
