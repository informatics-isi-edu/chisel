"""Catalog model for semi-structured data source.

The "semi-structure" module is intended only for testing, experimenting with different transformations outside of a
remote catalog, light "ETL" work to or from flat file sources, and similar non-critical workloads. It is not intended
for use in critical, production workloads.
"""
import os
import csv
import json
import logging
from deriva.core import ermrest_model as _erm
from ..optimizer import symbols
from .. import operators
from .. import util
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
    def __init__(self, path):
        """Initializes the semi-structured catalog.

        :param path: the root directory of the semi-structured catalog
        """
        super(SemiStructuredCatalog, self).__init__()
        self.path = path

    def getCatalogModel(self):
        return _erm.Model(self, _introspect(self.path))


class SemiStructuredModel (ext.Model):
    """Catalog model representation of semi-structured data.

    NOTE: only read operations are supported at this time.
    """
    def __init__(self, catalog):
        """Initializes the model.

        :param catalog: SemiStructuredCatalog object
        """
        assert isinstance(SemiStructuredCatalog)
        super(Model, self).__init__(catalog)

    def make_extant_symbol(self, schema_name, table_name):
        """Makes a symbol for representing an extant relation.

        :param schema_name: schema name
        :param table_name: table name
        """
        filename = os.path.join(os.path.expanduser(self.path), schema_name, table_name)
        if filename.endswith('.csv') or filename.endswith('.tsv') or filename.endswith('.txt'):
            return symbols.TabularDataExtant(filename=filename)
        elif filename.endswith('.json'):
            return symbols.JSONDataExtant(
                input_filename=filename, json_content=None, object_payload=None, key_regex=None)
        else:
            raise ValueError('Filename extension must be "csv" or "json" (filename: %s)' % filename)

    # TODO: refactor old code below to restore support for writeable semi-structured catalogs
    # def _materialize_relation(self, plan):
    #     """Materializes a relation from a physical plan.
    #
    #     :param plan: a `PhysicalOperator` instance from which to materialize the relation
    #     :return: None
    #     """
    #     if isinstance(plan, operators.Alter) or isinstance(plan, operators.Drop):
    #         raise NotImplementedError('"%s" operation not supported' % type(plan).__name__)
    #
    #     filename = os.path.join(self.path, plan.description['schema_name'], plan.description['table_name'])
    #     if os.path.exists(filename) and not self._evolve_ctx.allow_alter:
    #         raise base.CatalogMutationError('"allow_alter" flag is not True')
    #
    #     if filename.endswith('.json'):
    #         with open(filename, 'w') as jsonfile:
    #             json.dump(list(plan), jsonfile, indent=2)
    #     elif filename.endswith('.csv') or filename.endswith('.tsv') or filename.endswith('.txt'):
    #         dialect = 'excel' if filename.endswith('.csv') else 'excel-tab'
    #         # else, by default materialize as csv
    #         field_names = [col['name'] for col in plan.description['column_definitions']]
    #         with open(filename, 'w') as csvfile:
    #             writer = csv.DictWriter(csvfile, fieldnames=field_names, dialect=dialect)
    #             writer.writeheader()
    #             writer.writerows(plan)
    #     else:
    #         raise Exception("Unable to materialize relation. Unknown file extension for '{}'.".format(filename))


def csv_reader(filename):
    """Reads and parses a CSV file and returns a computed relation.

    The CSV contents must include a header row.

    :param filename: a filename of a tabular data file in CSV format
    :return: a computed relation object
    """
    return ext.ComputedRelation(stubs.SchemaStub('none'), symbols.TabularDataExtant(filename))


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
        stubs.SchemaStub('none'),
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

    return ext.ComputedRelation(stubs.SchemaStub('none'), symbols.Shred(filename_or_graph, sparql_query))
