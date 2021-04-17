"""Catalog model for semi-structured data source.

The "semi-structure" module is intended only for testing, experimenting with different transformations outside of a
remote catalog, light "ETL" work to or from flat file sources, and similar non-critical workloads. It is not intended
for use in critical, production workloads.
"""
import os
import csv
import json
import logging
import re
from typing import Optional, Any
from urllib.parse import unquote as urlunquote
from deriva.core import ermrest_model as _erm
from deriva.core import datapath, DEFAULT_HEADERS
from ..optimizer import symbols
from . import ext, stubs

logger = logging.getLogger(__name__)


def _introspect(path):
    """Introspects the model of semistructured files in a shallow directory hierarchy.

    :param path: the directory path
    :return: a catalog model document
    """
    def table_definition_from_file(base_dir, schema_name, filename):
        abs_filename = os.path.join(base_dir, schema_name, filename)
        if os.path.isdir(abs_filename):
            return None
        elif filename.endswith('.csv') or filename.endswith('.tsv') or filename.endswith('.txt'):
            return csv_reader(abs_filename).prejson()
        elif filename.endswith('.json'):
            return json_reader(abs_filename).prejson()
        else:
            logger.warning('Unsupported file extension encountered for file: {file}'.format(file=abs_filename))
            return None

    model_doc = {'schemas': {}}
    model_doc['schemas']['.'] = _erm.Schema.define('.')
    model_doc['schemas']['.']['tables'] = {}

    # Iterate over directory, ignoring sub-directories for now (os.walk later if desired)
    for filename in os.listdir(path):
        abs_filename = os.path.join(path, filename)
        if os.path.isdir(abs_filename):
            schema_name = filename
            schema_doc = _erm.Schema.define(schema_name)
            schema_doc['tables'] = {}
            model_doc['schemas'][schema_name] = schema_doc
            for filename in os.listdir(abs_filename):
                table = table_definition_from_file(path, schema_name, filename)
                if table:
                    schema_doc['tables'][filename] = table
        elif os.path.isfile(abs_filename):
            table = table_definition_from_file(path, '.', filename)
            if table:
                model_doc['schemas']['.']['tables'][filename] = table

    # Return model document
    return model_doc


class SemiStructuredCatalog (stubs.CatalogStub):
    """Catalog of semi-structured data.
    """
    def __init__(self, rootdir):
        """Initializes the semi-structured catalog.

        :param rootdir: the root directory of the semi-structured catalog
        """
        super(SemiStructuredCatalog, self).__init__()
        self.rootdir = rootdir
        self._server_uri = 'file://%s' % rootdir

    class Response (object):
        """Response object for catalog stub operations.
        """
        def __init__(self, error: Optional[Exception] = None, payload: Any = None):
            """Initializes the response object.

            :param error: exception to raise
            :param payload: JSON-like object representing the response body.
            """
            self._error = error
            self._payload = payload

        def raise_for_status(self):
            if self._error:
                raise self._error

        def json(self):
            return self._payload

    def getCatalogModel(self):
        return _erm.Model(self, _introspect(self.rootdir))

    def getPathBuilder(self):
        """Returns the 'path builder' interface for this catalog."""
        return datapath.from_catalog(self)

    def post(self, path, data=None, json=None, headers=DEFAULT_HEADERS):
        """Handles POST request, returns Response object.

        This interface supports the minimal dialect for:
          - table creation
          - row insertion

        :param path: resource path
        :param data: buffer or file-like content value (not supported)
        :param json: in-memmory data object
        :param headers: request headers
        :return: response object
        """
        logger.debug('path: %s' % path)
        logger.debug('json: %s' % str(json))

        # handle table creation
        m = re.match(r'/schema/(?P<schema_name>[^/]+)/table', path)
        if m:
            try:
                schema_name = urlunquote(m.group('schema_name'))
                return SemiStructuredCatalog.Response(payload=self._create_table_on_disk(schema_name, json))
            except Exception as e:
                return SemiStructuredCatalog.Response(error=e)

        # handle row insertion
        m = re.match(r'/entity/(?P<schema_name>[^/]+):(?P<table_name>[^/?]+)([?]defaults=(?P<defaults>.+))?', path)
        if m:
            try:
                schema_name = urlunquote(m.group('schema_name'))
                table_name = urlunquote(m.group('table_name'))
                return SemiStructuredCatalog.Response(payload=self._write_rows_to_file(schema_name, table_name, json))
            except Exception as e:
                return SemiStructuredCatalog.Response(error=e)

        # all others, unhandled
        super(SemiStructuredCatalog, self).post(path, data=data, json=json, headers=headers)

    def _create_table_on_disk(self, schema_name: str, table_def: dict) -> dict:
        """Creates a table on disk.

        :param schema_name: subdir "schema" name
        :param table_def: table description document
        :return: returns the table description document as it was created on disk
        """
        _table_name_ = 'table_name'
        _column_definitions_ = 'column_definitions'

        if _table_name_ not in table_def:
            raise ValueError('"table_def" object must include %s' % _table_name_)
        if _column_definitions_ not in table_def:
            raise ValueError('"table_def" object must include %s' % _column_definitions_)

        filename = os.path.join(self.rootdir, schema_name, table_def[_table_name_])
        logger.debug('Creating table in file: %s' % filename)

        if os.path.exists(filename):
            raise ValueError('%s:%s exists')

        if filename.endswith('.json'):
            with open(filename, 'w') as jsonfile:
                json.dump([], jsonfile, indent=2)
        elif filename.endswith('.csv') or filename.endswith('.tsv') or filename.endswith('.txt'):
            dialect = 'excel' if filename.endswith('.csv') else 'excel-tab'
            field_names = [col['name'] for col in table_def['column_definitions']]
            with open(filename, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=field_names, dialect=dialect)
                writer.writeheader()
        else:
            raise ValueError("Unsupported filename extension for file: %s" % filename)

        created_table_def = {
            _table_name_: table_def[_table_name_],
            _column_definitions_: table_def[_column_definitions_]
        }
        return created_table_def

    def _write_rows_to_file(self, schema_name: str, table_name: str, rows: list) -> list:
        """Appends rows of data into a file on disk.

        :param schema_name: subdir "schema" name
        :param table_name: file "table" name
        :param rows: list of dictionary-like objects
        :return: returns the rows written to file on disk
        """

        if not isinstance(rows, list):
            raise ValueError('Expecting "list" payload but received "%s"' % type(rows).__name__)

        filename = os.path.join(self.rootdir, schema_name, table_name)
        logger.debug('Inserting rows in file: %s' % filename)

        if not os.path.exists(filename):
            raise KeyError('%s:%s does not exist')

        if filename.endswith('.json'):
            with open(filename, 'a') as jsonfile:
                json.dump(rows, jsonfile, indent=2)
        elif filename.endswith('.csv') or filename.endswith('.tsv') or filename.endswith('.txt'):
            dialect = 'excel' if filename.endswith('.csv') else 'excel-tab'
            with open(filename, 'r') as csvfile:
                reader = csv.DictReader(csvfile)
                field_names = reader.fieldnames
            with open(filename, 'a') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=field_names, dialect=dialect)
                writer.writerows(rows)
        else:
            raise ValueError("Unsupported filename extension for file: %s" % filename)

        return rows


class SemiStructuredModel (ext.Model):
    """Catalog model representation of semi-structured data.
    """
    def __init__(self, catalog):
        """Initializes the model.

        :param catalog: SemiStructuredCatalog object
        """
        assert isinstance(catalog, SemiStructuredCatalog)
        super(SemiStructuredModel, self).__init__(catalog)

    def make_extant_symbol(self, schema_name, table_name):
        """Makes a symbol for representing an extant relation.

        :param schema_name: schema name
        :param table_name: table name
        """
        filename = os.path.join(os.path.expanduser(self.catalog.rootdir), schema_name, table_name)
        if filename.endswith('.csv') or filename.endswith('.tsv') or filename.endswith('.txt'):
            return symbols.TabularDataExtant(filename=filename)
        elif filename.endswith('.json'):
            return symbols.JSONDataExtant(
                input_filename=filename, json_content=None, object_payload=None, key_regex=None)
        else:
            raise ValueError('Filename extension must be "csv" or "json" (filename: %s)' % filename)


def csv_reader(filename):
    """Reads and parses a CSV file and returns a computed relation.

    The CSV contents must include a header row.

    :param filename: a filename of a tabular data file in CSV format
    :return: a computed relation object
    """
    return ext.ComputedRelation(stubs.SchemaStub('.'), symbols.TabularDataExtant(filename))


def json_reader(input_filename=None, json_content=None, object_payload=None, key_regex='^RID$|^ID$|^id$|^name$|^Name$'):
    """Reads, parses, and (minimally) instrospects JSON input data from a file, text, or object source.

    The input data, whether passed as `input_filename`, `json_content`, or
    `object_payload` must represent a JSON list of JSON objects. Only a
    shallow introspection will be performed to determine the table definition,
    by examining the first object in the list. Columns that match the
    `key_regex` will be identified as keys (i.e., unique and not null).

    :param input_filename: a filename of a tabular data file in JSON format
    :param json_content: a text payload in JSON format
    :param object_payload: a python list of dictionaries
    :param key_regex: a regular expression used to guess a key column from a property name
    :return: a computed relation object
    """
    return ext.ComputedRelation(
        stubs.SchemaStub('.'),
        symbols.JSONDataExtant(input_filename, json_content, object_payload, key_regex)
    )


def shred(filename_or_graph, sparql_query):
    """Shreds graph data (e.g., RDF, JSON-LD, etc.) into relational (tabular) data structure as a computed relation.

    :param filename_or_graph: a filename of an RDF jsonld graph or a parsed rdflib.Graph instance
    :param sparql_query: SPARQL query expression
    :return: a computed relation object
    """
    if not filename_or_graph:
        raise ValueError('Parameter "filename_or_graph" must be a filename or a graph object')
    if not sparql_query:
        raise ValueError('Parameter "sparql_query" must be a SPARQL query expression string')

    return ext.ComputedRelation(stubs.SchemaStub('.'), symbols.Shred(filename_or_graph, sparql_query))
