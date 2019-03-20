"""Catalog model for semistructured data source."""

import os
import csv
import json
import logging
from deriva.core import ermrest_model as _em
from .. import optimizer
from .. import util
from . import base

logger = logging.getLogger(__name__)


def _kwargs(**kwargs):
    """Helper for extending module with sub-types for the whole model tree."""
    kwargs2 = {
        'schema_class': base.Schema,
        'table_class': SemistructuredTable,
        'column_class': base.Column
    }
    kwargs2.update(kwargs)
    return kwargs2


def connect(url, credentials=None):
    """Connect to a local, semi-structured (i.e., CSV, JSON) data source.

    :param url: connection string url
    :param credentials: user credentials
    :return: catalog for data source
    """
    parsed_url = util.urlparse(url)
    if credentials:
        logger.warning('Credentials not supported by semistructured catalog')
    return from_semistructured_files(parsed_url.path)


def from_semistructured_files(path):
    """Initializes a database catalog based on semistructured files in a shallow directory hierarchy.

    :param path: the directory path
    :return: a catalog instance
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
            logger.debug('Unsupported file extension encountered for file: {file}'.format(file=abs_filename))
            return None

    model_doc = {'schemas': {}}
    model_doc['schemas']['.'] = _em.Schema.define('.')
    model_doc['schemas']['.']['tables'] = {}

    # Iterate over directory, ignoring sub-directories for now (os.walk later if desired)
    for filename in os.listdir(path):
        abs_filename = os.path.join(path, filename)
        if os.path.isdir(abs_filename):
            schema_name = filename
            schema_doc = _em.Schema.define(schema_name)
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

    # Define model doc and instantiate catalog
    return SemistructuredCatalog(model_doc, path=path)


class SemistructuredCatalog(base.AbstractCatalog):
    """Database catalog backed by semistructured files."""
    def __init__(self, model_doc, **kwargs):
        super(SemistructuredCatalog, self).__init__(model_doc, **_kwargs(**kwargs))
        assert 'path' in kwargs, "required argument 'path' missing"
        self.path = kwargs['path']

    def _materialize_relation(self, schema, plan):
        """Materializes a relation from a physical plan.

        :param schema: a `Schema` in which to materialize the relation
        :param plan: a `PhysicalOperator` instance from which to materialize the relation
        :return: None
        """
        filename = os.path.join(self.path, schema.name, plan.description['table_name'])
        if filename.endswith('.json'):
            with open(filename, 'w') as jsonfile:
                json.dump(list(plan), jsonfile, indent=2)
        elif filename.endswith('.csv') or filename.endswith('.tsv') or filename.endswith('.txt'):
            dialect = 'excel' if filename.endswith('.csv') else 'excel-tab'
            # else, by default materialize as csv
            field_names = [col['name'] for col in plan.description['column_definitions']]
            with open(filename, 'w') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=field_names, dialect=dialect)
                writer.writeheader()
                writer.writerows(plan)
        else:
            raise Exception("Unable to materialize relation. Unknown file extension for '{}'.".format(filename))


class SemistructuredTable (base.AbstractTable):
    """Extant table in a semistructured catalog."""
    def __init__(self, sname, tname, table_doc, **kwargs):
        super(SemistructuredTable, self).__init__(sname, tname, table_doc, **_kwargs(**kwargs, table=self))
        self.schema = kwargs['schema']

    @property
    def logical_plan(self):
        """The logical plan used to compute this relation; intended for internal use."""
        filename = os.path.join(self.schema.model.path, self.sname, self.name)
        if filename.endswith('.csv') or filename.endswith('.tsv') or filename.endswith('.txt'):
            return optimizer.Scan(filename=filename)
        elif filename.endswith('.json'):
            return optimizer.JSONScan(input_filename=filename, json_content=None, object_payload=None, key_regex=None)
        else:
            raise ValueError('Invalid data source file type: unknown extension for %s' % filename)


def csv_reader(filename):
    """Reads and parses a CSV file and returns a computed relation.

    The CSV contents must include a header row.

    :param filename: a filename of a tabular data file in CSV format
    :return: a computed relation object
    """
    return base.ComputedRelation(optimizer.Scan(filename))


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
    return base.ComputedRelation(optimizer.JSONScan(input_filename, json_content, object_payload, key_regex))
