"""Helpers for the tests."""

import abc
import csv
import logging
import json
import os
from os.path import dirname as up
import unittest
from requests import HTTPError

from deriva.core import DerivaServer, ErmrestCatalog, urlquote, get_credential
from deriva.core.ermrest_model import Schema, Table, Column, Key, builtin_types

from deriva.chisel.catalog.semistructured import SemiStructuredCatalog, SemiStructuredModel
from deriva.chisel import Model

logger = logging.getLogger(__name__)


class TestHelper:
    """Test helper class for defining test data.
    """

    FIELDS = ['id', 'species', 'list_of_closest_genes', 'list_of_anatomical_structures']

    DUMMY_ROWS = [
        (0, 'Mus musculus',   'H2afy3, LOC432958',     'upper lib, ear, limb, nose surface ectoderm'),
        (0, 'Mus muscullus',  'Msx1as, Stx18',         'nose, palate'),
        (0, 'mus musculus',   '1700029J03Rik, Setd4',  'nose, limb, ribs'),
        (0, 'Mus musclus',    'LOC101927620, MIR5580', 'facial mesenchyme, somite, nose'),
        (0, 'musmusculus',    'LOC101927620, MIR5580', 'heart, limb, several craniofacial structures'),
        (0, 'Mus musculus',   'Leprel1, Leprel1',      'limb, nose, various facial structures'),
        (0, 'Mus muscullus',  'BET1, COL1A2',          'branchial arch, facial mesenchyme'),
        (0, 'mus musculus',   '5430421F17Rik, Fgfr1',  'facial mesenchyme, limb'),
        (0, 'Mus musclus',    'A530065N20, Gas1',      'forebrain, hindbrain, midbrain, limb, neural tube, nose, somite'),
        (0, 'musmusculus',    'Mitf, Gm765',           'branchial arch')
    ]
    DUMMY_LEN = len(DUMMY_ROWS)

    def __init__(self, num_test_rows=30):
        """Initializes the catalog helper.

        :param num_test_rows: number of test rows to produce from the dummy rows
        """
        self.num_test_rows = num_test_rows
        self.test_data = [
            {
                'id': i,
                'species': self.DUMMY_ROWS[i % self.DUMMY_LEN][1],
                'list_of_closest_genes': self.DUMMY_ROWS[i % self.DUMMY_LEN][2],
                'list_of_anatomical_structures': self.DUMMY_ROWS[i % self.DUMMY_LEN][3]
            } for i in range(num_test_rows)
        ]


class AbstractCatalogHelper (TestHelper):
    """Abstract catalog helper class for setting up & tearing down catalogs during unit tests.
    """
    def __init__(self, num_test_rows=30):
        super(AbstractCatalogHelper, self).__init__(num_test_rows=num_test_rows)

    @abc.abstractmethod
    def suite_setup(self):
        """Creates and populates a test catalog."""

    @abc.abstractmethod
    def suite_teardown(self):
        """Deletes the test catalog."""

    @abc.abstractmethod
    def unit_setup(self):
        """Defines schema and populates data for a unit test setup."""

    @abc.abstractmethod
    def unit_teardown(self, other=[]):
        """Deletes tables that have been mutated during a unit test."""

    @abc.abstractmethod
    def exists(self, tablename):
        """Tests if a table exists."""

    @abc.abstractmethod
    def connect(self):
        """Connect the catalog."""


class CatalogHelper (AbstractCatalogHelper):
    """Helper class that sets up and tears down a local catalog.
    """

    CSV = 'csv'
    JSON = 'json'

    def __init__(self, table_names=[], file_format=CSV):
        """Initializes catalog helper.

        :param table_names: list of tables to be added to this catalog during unit testing
        :param file_format: file format used by the catalog. Acceptable values: 'csv' or 'json'.
        """
        super(CatalogHelper, self).__init__()

        if file_format not in {self.CSV, self.JSON}:
            raise ValueError('Invalid file format')
        self._data_dir = os.path.join(up(up(__file__)), 'data')
        self._file_format = file_format

        # 'samples' tabular data
        self.samples = 'samples.' + file_format
        self.samples_filename = os.path.join(self._data_dir, self.samples)

        # output data files expected
        self._unit_table_names = table_names
        self._unit_table_filenames = [os.path.join(self._data_dir, basename) for basename in table_names]

    def suite_setup(self):
        os.makedirs(self._data_dir, exist_ok=True)

        with open(self.samples_filename, 'w', newline='') as ofile:
            if self._file_format == self.CSV:
                csvwriter = csv.DictWriter(ofile, fieldnames=self.FIELDS)
                csvwriter.writeheader()
                csvwriter.writerows(self.test_data)
            else:
                json.dump(self.test_data, ofile)

    def suite_teardown(self):
        self.unit_teardown(other=[self.samples_filename])
        os.rmdir(self._data_dir)

    def unit_setup(self):
        pass

    def unit_teardown(self, other=[]):
        filenames = self._unit_table_filenames + other
        for filename in filenames:
            try:
                os.remove(filename)
            except FileNotFoundError:
                pass

    def exists(self, tablename):
        return os.path.isfile(os.path.join(self._data_dir, tablename))

    def connect(self):
        return SemiStructuredModel(SemiStructuredCatalog(self._data_dir))


class ERMrestHelper (AbstractCatalogHelper):
    """Helper class that sets up and tears down an ERMrest catalog.
    """

    samples = 'samples'

    def __init__(self, hostname, catalog_id=None, unit_schema_names=[], unit_table_names=[]):
        """Initializes the ERMrest catalog helper

        :param hostname: hostname of the deriva test server
        :param catalog_id: optional id of catalog to _reuse_ by this unit test suite
        :param unit_table_names: list of names of tables used in unit tests
        :param unit_schema_names: list of names of schemas used in unit tests (will be created during setup)
        """
        super(ERMrestHelper, self).__init__()
        self._hostname = hostname
        self._ermrest_catalog = None
        self._reuse_catalog_id = catalog_id
        self._unit_schema_names = unit_schema_names
        self._unit_table_names = unit_table_names

    @classmethod
    def _parse_table_name(cls, tablename):
        if not tablename:
            raise ValueError("tablename not given")
        fq_name = tablename.split(':')
        if len(fq_name) == 2:
            sname, tname = fq_name
        elif len(fq_name) < 2:
            sname, tname = 'public', fq_name[0]
        else:
            raise ValueError("invalid 'tablename': " + tablename)
        return sname, tname

    def suite_setup(self):
        # create catalog
        server = DerivaServer('https', self._hostname, credentials=get_credential(self._hostname))
        if self._reuse_catalog_id:
            self._ermrest_catalog = server.connect_ermrest(self._reuse_catalog_id)
            self.unit_teardown()  # in the event that the last run terminated abruptly and didn't properly teardown
        else:
            self._ermrest_catalog = server.create_ermrest_catalog()

    def suite_teardown(self):
        # leave test catalogs to be cleaned up by the server policy rather than risk someone pointing the test suite
        # at their production server and catalog, and deleting it by accident.
        return

    def unit_setup(self):
        # get public schema
        model = self._ermrest_catalog.getCatalogModel()
        public = model.schemas['public']
        assert isinstance(public, Schema)

        # create schema
        for sname in self._unit_schema_names:
            model.create_schema(Schema.define(sname))

        # create table
        public.create_table(
            Table.define(
                self.samples,
                column_defs=[
                    Column.define(
                        self.FIELDS[0],
                        builtin_types.int8,
                        False
                    )
                ] + [
                    Column.define(
                        field_name,
                        builtin_types.text
                    )
                    for field_name in self.FIELDS[1:]
                ],
                key_defs=[
                    Key.define(
                        ['id']
                    )
                ]
            )
        )

        # insert test data
        pb = self._ermrest_catalog.getPathBuilder()
        samples = pb.schemas['public'].tables[self.samples]
        samples.insert(self.test_data)

    def unit_teardown(self, other=[]):
        # delete any mutated tables
        assert isinstance(self._ermrest_catalog, ErmrestCatalog)
        model = self._ermrest_catalog.getCatalogModel()

        # delete tables
        for tablename in self._unit_table_names + other + [self.samples]:
            try:
                s, t = self._parse_table_name(tablename)
                if s in model.schemas and t in model.schemas[s].tables:
                    logger.debug('Dropping table "%s"' % t)
                    model.schemas[s].tables[t].drop()
            except HTTPError as e:
                if e.response.status_code != 404:  # suppress the expected 404
                    raise e

        # delete schemas
        for s in self._unit_schema_names:
            try:
                if s != 'public' and s in model.schemas:
                    logger.debug('Dropping schema "%s"' % s)
                    model.schemas[s].drop()
            except HTTPError as e:
                if e.response.status_code != 404:  # suppress the expected 404
                    raise e

    def exists(self, tablename):
        # check if table exists in ermrest catalog
        assert isinstance(self._ermrest_catalog, ErmrestCatalog)
        sname, tname = self._parse_table_name(tablename)

        try:
            path = '/schema/%s/table/%s' % (urlquote(sname), urlquote(tname))
            r = self._ermrest_catalog.get(path)
            r.raise_for_status()
            resp = r.json()
            return resp is not None
        except HTTPError as e:
            if e.response.status_code == 404:
                return False
            else:
                raise e

    def connect(self):
        # connect to catalog
        assert isinstance(self._ermrest_catalog, ErmrestCatalog)
        return Model.from_catalog(self._ermrest_catalog)


class BaseTestCase (unittest.TestCase):
    """A base class test case that can be used to reduce boilerplate catalog setup.
    """

    catalog_helper = CatalogHelper()

    @classmethod
    def setUpClass(cls):
        cls.catalog_helper.suite_setup()

    @classmethod
    def tearDownClass(cls):
        cls.catalog_helper.suite_teardown()

    def setUp(self):
        self.catalog_helper.unit_setup()
        self.model = self.catalog_helper.connect()

    def tearDown(self):
        self.catalog_helper.unit_teardown()
