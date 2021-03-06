"""Catalog model for semistructured data source."""

import os
import csv
import json
import logging
from deriva.core import ermrest_model as _em
from .. import optimizer
from .. import operators
from .. import util
from . import base

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

    # Return model document
    return model_doc


class SemistructuredCatalog (base.AbstractCatalog):
    """Database catalog backed by semistructured files."""
    def __init__(self, url, credentials):
        parsed_url = util.urlparse(url)
        if credentials:
            logger.warning('Credentials not supported by semistructured catalog')
        self.path = parsed_url.path
        super(SemistructuredCatalog, self).__init__(_introspect(self.path))

    def _materialize_relation(self, plan):
        """Materializes a relation from a physical plan.

        :param plan: a `PhysicalOperator` instance from which to materialize the relation
        :return: None
        """
        if isinstance(plan, operators.Alter) or isinstance(plan, operators.Drop):
            raise NotImplementedError('"%s" operation not supported' % type(plan).__name__)

        filename = os.path.join(self.path, plan.description['schema_name'], plan.description['table_name'])
        if os.path.exists(filename) and not self._evolve_ctx.allow_alter:
            raise base.CatalogMutationError('"allow_alter" flag is not True')

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

    def logical_plan(self, table):
        """Symbolic representation of the table extant; intended for internal use."""
        filename = os.path.join(self.path, table.schema.name, table.name)
        if filename.endswith('.csv') or filename.endswith('.tsv') or filename.endswith('.txt'):
            return optimizer.TabularDataExtant(filename=filename)
        elif filename.endswith('.json'):
            return optimizer.JSONDataExtant(input_filename=filename, json_content=None, object_payload=None, key_regex=None)
        else:
            raise ValueError('Invalid data source file type: unknown extension for %s' % filename)


def csv_reader(filename):
    """Reads and parses a CSV file and returns a computed relation.

    The CSV contents must include a header row.

    :param filename: a filename of a tabular data file in CSV format
    :return: a computed relation object
    """
    return base.ComputedRelation(optimizer.TabularDataExtant(filename))


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
    return base.ComputedRelation(optimizer.JSONDataExtant(input_filename, json_content, object_payload, key_regex))
