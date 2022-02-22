"""Physical operators specific to semi-structured (CSV, JSON) data sources.
"""
from copy import deepcopy
import csv
from datetime import datetime
import json
import logging
import os
import re
from deriva.core import ermrest_model as _em
from .base import PhysicalOperator

logger = logging.getLogger(__name__)

#: Default regular expression for shallow introspection of a key based on the attribute name.
_default_key_regex = '^RID$|^ID$|^Name$|^Key$|^pk$'


class JSONScan (PhysicalOperator):
    """Scan operator for JSON files and text payloads."""
    def __init__(self, input_filename=None, json_content=None, object_payload=None, key_regex=_default_key_regex):
        """Reads, parses, and (minimally) introspects JSON input data from a file, text, or object source.

        :param input_filename: an input filename with JSON contents
        :param json_content: a text payload in JSON format
        :param object_payload: a python list of dictionaries
        :param key_regex: a regular expression used to guess a key column from a property name
        """
        super(JSONScan, self).__init__()
        if [input_filename, json_content, object_payload].count(None) != 2:
            raise ValueError('Only one data source expected')

        if input_filename:
            with open(input_filename) as fp:
                # consider using ijson, or writing a custom parser the reads one object at a time
                # for now, this is not a high priority to optimize, so we use the 'json' library
                self._data = json.load(fp)
            table_name = os.path.basename(input_filename)
        elif json_content:
            self._data = json.loads(json_content)
            table_name = 'JSON_DOCUMENT'
        else:
            assert object_payload, "object payload expected"
            self._data = object_payload
            table_name = 'PYTHON_OBJECT'

        if not isinstance(self._data, list) and len(self._data) and isinstance(self._data[0], dict):
            raise ValueError('Input source must be a non-empty array of objects')

        # make sure key_regex has a default value
        key_regex = key_regex if key_regex else _default_key_regex

        row_0_keys = self._data[0].keys() if self._data else []
        col_defs = [_em.Column.define(name, _em.builtin_types['text']) for name in row_0_keys]
        key_defs = [_em.Key.define([name]) for name in row_0_keys if re.match(key_regex, name, re.IGNORECASE)]
        if not key_defs:
            logger.warning("Expected to find at least one key, but none were identified.")
        self._description = _em.Table.define(table_name, column_defs=col_defs, key_defs=key_defs, provide_system=False)
        self._description['schema_name'] = os.path.dirname(input_filename) if input_filename else ''
        self._description['kind'] = 'file' if input_filename else 'json'

    def __iter__(self):
        return iter(self._data)


class TabularFileScan (PhysicalOperator):
    """Scan operator for tabular file formats (e.g., CSV, TSV, or other similar formats)."""

    #: schema cache to avoid multiple file seeks for same input file; keyed on 'filename'
    _schema_cache = {}

    def __init__(self, filename, key_regex=_default_key_regex, deep_introspection=False):
        super(TabularFileScan, self).__init__()
        self._filename = filename
        self._dialect = 'excel' if filename.endswith('.csv') else 'excel-tab'  # csv or tabular dialect expected
        self._key_regex = key_regex if key_regex else _default_key_regex  # make sure key_regex has a default value

        if filename in TabularFileScan._schema_cache:
            self._description = deepcopy(TabularFileScan._schema_cache[filename])
        else:
            # shallow introspection of relation schema based on field names
            self._description = self._shallow_introspection()
            if deep_introspection:
                self._deep_introspection()
            TabularFileScan._schema_cache[filename] = self._description

    def __iter__(self):
        """Returns a generator function."""
        with open(self._filename) as file:
            reader = csv.DictReader(file, dialect=self._dialect)
            for line in reader:
                yield line

    def _shallow_introspection(self):
        # shallow introspection of relation schema based on field names
        with open(self._filename) as f:
            field_names = csv.DictReader(f, dialect=self._dialect).fieldnames
            col_defs = [_em.Column.define(name, _em.builtin_types['text']) for name in field_names]
            key_defs = [_em.Key.define([name]) for name in field_names if re.match(self._key_regex, name, re.IGNORECASE)]
        table_doc = _em.Table.define(self._filename, col_defs, key_defs, provide_system=False)
        table_doc['schema_name'] = os.path.dirname(self._filename)
        table_doc['kind'] = 'file'
        return table_doc

    def _deep_introspection(self):
        """Introspects the schema by inspecting the row data.
        """

        # initialize candidates for types, notnulls, and uniques
        columns = self._description['column_definitions']
        candidate_types = {col['name']: {float, int, bool, datetime} for col in columns}
        candidate_notnull = {col['name'] for col in columns}
        candidate_uniques = {col['name']: set() for col in columns}

        # test all values of all tuples; short-circuit when exhausted types
        for row in iter(self):
            # determine if column is not null (or empty string)
            if candidate_notnull:
                candidate_notnull = {col for col in candidate_notnull if row[col] is not None and row[col] != ''}
                candidate_notnull = None if len(candidate_notnull) == 0 else candidate_notnull

            # determine if column is not unique
            for col in candidate_uniques.copy():
                val = row[col]
                # we won't test large values (e.g., a large block of text is likely to be unique but not by definition)
                if not val or len(val) > 256 or val in candidate_uniques[col]:
                    del candidate_uniques[col]
                else:
                    candidate_uniques[col].add(val)

            # determine the valid type(s) for the each column
            for col in candidate_types:
                candidate_types[col] = self._valid_types(candidate_types[col], row[col])

            # revise the candidate types; leave out ones with no candidate types left
            candidate_types = {col: types for (col, types) in candidate_types.items() if len(types)}

            # break when no column is left to test
            if not len(candidate_types) and candidate_notnull is None and not len(candidate_uniques):
                break

        # update relation schema
        for column in columns:
            cname = column['name']
            column['nullok'] = False if cname in candidate_notnull else True
            column['type'] = self._py_to_ermrest_types(candidate_types.get(cname, {})).prejson()

    @classmethod
    def _valid_types(cls, types, value):
        """Given a set of types, returns the valid types for the given value."""
        if not value or value == '':
            # do not rule out any types if the value was None or empty string
            return types

        if int in types:
            try:
                int(value)
                if float in types:
                    # any value that passes as an int will pass as a float
                    return {int, float}
                else:
                    return {int}
            except ValueError:
                pass

        if float in types:
            try:
                float(value)
                return {float}
            except ValueError:
                pass

        if bool in types:
            if value.lower() in ['true', 't', 'false', 'f']:
                return {bool}

        if datetime in types:
            # very simple method for detecting a timestamptz value
            pattern = "%Y-%m-%d %H:%M:%S.%f%z"
            try:
                datetime.strptime(value, pattern)
                return {datetime}
            except ValueError:
                try:
                    # python expects timestamps with 4 digit timezone
                    datetime.strptime(value+"00", pattern)
                    return {datetime}
                except ValueError:
                    pass

        return {}

    @classmethod
    def _py_to_ermrest_types(cls, types):
        """Takes a set of native python types and returns the best matching ERMrest type.

        :param types: set of python types
        :return: ermrest type definition
        """
        if len(types) == 1:
            base_type = types.pop()
            if base_type is str:
                return _em.builtin_types.text
            elif base_type is int:
                return _em.builtin_types.int4
            elif base_type is float:
                return _em.builtin_types.float4
            elif base_type is datetime:
                return _em.builtin_types.timestamptz
            elif base_type is bool:
                return _em.builtin_types.boolean
            else:
                raise ValueError("Unknown base type: {t}".format(t=base_type))
        elif len(types) == 2 and int in types:
            return _em.builtin_types.int4
        else:
            return _em.builtin_types.text
