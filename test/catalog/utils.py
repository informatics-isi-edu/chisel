import abc
import csv
import json
import os
from os.path import dirname as up
import unittest

from deriva.core import DerivaServer, ErmrestCatalog, urlquote
from deriva.core.ermrest_model import Schema, Table, Column, Key, builtin_types

import chisel


class AbstractCatalogHelper:
    """Abstract catalog helper class for setting up & tearing down catalogs during unit tests.
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
        self._test_rows = [
            {
                'id': i,
                'species': self.DUMMY_ROWS[i % self.DUMMY_LEN][1],
                'list_of_closest_genes': self.DUMMY_ROWS[i % self.DUMMY_LEN][2],
                'list_of_anatomical_structures': self.DUMMY_ROWS[i % self.DUMMY_LEN][3]
            } for i in range(num_test_rows)
        ]

    @abc.abstractmethod
    def setup(self):
        """Creates and populates a test catalog."""
        pass

    @abc.abstractmethod
    def teardown(self):
        """Deletes the test catalog."""
        pass

    @abc.abstractmethod
    def unit_teardown(self, other=[]):
        """Deletes tables that have been mutated during a unit test."""
        pass

    @abc.abstractmethod
    def exists(self, tablename):
        """Tests if a table exists."""
        pass

    @abc.abstractmethod
    def connect(self):
        pass


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

    def setup(self):
        os.makedirs(self._data_dir, exist_ok=True)

        with open(self.samples_filename, 'w', newline='') as ofile:
            if self._file_format == self.CSV:
                csvwriter = csv.DictWriter(ofile, fieldnames=self.FIELDS)
                csvwriter.writeheader()
                csvwriter.writerows(self._test_rows)
            else:
                json.dump(self._test_rows, ofile)

    def teardown(self):
        self.unit_teardown(other=[self.samples_filename])
        os.rmdir(self._data_dir)

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
        return chisel.connect(self._data_dir)


class ERMrestHelper (AbstractCatalogHelper):
    """Helper class that sets up and tears down an ERMrest catalog.
    """

    def __init__(self, hostname):
        """Initializes the ERMrest catalog helper

        :param hostname: hostname of the deriva test server
        """
        super(ERMrestHelper, self).__init__()
        self.samples = 'samples'
        self._unit_table_names = []
        self._hostname = hostname
        self._ermrest_catalog = None

    @classmethod
    def _parse_table_name(cls, tablename):
        if not tablename:
            raise ValueError("tablename not given")
        fq_name = tablename.split(':')
        if len(fq_name) == 2:
            sname, tname = fq_name
        elif len(fq_name) < 2:
            sname, tname = 'public', fq_name
        else:
            raise ValueError("invalid 'tablename': " + tablename)
        return sname, tname

    def setup(self):
        # create catalog
        server = DerivaServer('https', self._hostname)
        self._ermrest_catalog = server.create_ermrest_catalog()

        # get public schema
        model = self._ermrest_catalog.getCatalogModel()
        public = model.schemas['public']
        assert isinstance(public, Schema)

        # create table
        public.create_table(
            self._ermrest_catalog,
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
        path = pb.schemas['public'].tables[self.samples].path
        path.insert(self._test_rows)

    def teardown(self):
        # delete catalog
        assert isinstance(self._ermrest_catalog, ErmrestCatalog)
        self._ermrest_catalog.delete_ermrest_catalog(really=True)

    def unit_teardown(self, other=[]):
        # delete any mutated tables
        model = self._ermrest_catalog.getCatalogModel()
        for tablename in self._unit_table_names + other:
            try:
                s, t = self._parse_table_name(tablename)
                model.schemas[s].tables[t].delete(self._ermrest_catalog)
            except Exception:
                pass

    def exists(self, tablename):
        # check if table exists in ermrest catalog
        assert isinstance(self._ermrest_catalog, ErmrestCatalog)
        sname, tname = self._parse_table_name(tablename)

        path = '/schema/%s/table/%s' % (urlquote(sname), urlquote(tname))
        r = self._ermrest_catalog.get(path)
        r.raise_for_status()
        resp = r.json()
        return resp is not None

    def connect(self):
        # connect to catalog
        assert isinstance(self._ermrest_catalog, ErmrestCatalog)
        return chisel.connect('https://{hostname}/ermrest/catalog/{id}'.format(
            hostname=self._hostname,
            id=self._ermrest_catalog._catalog_id
        ))


class BaseTestCase (unittest.TestCase):
    """A base class test case that can be used to reduce boilerplate catalog setup.
    """

    catalog_helper = CatalogHelper()

    @classmethod
    def setUpClass(cls):
        cls.catalog_helper.setup()

    @classmethod
    def tearDownClass(cls):
        cls.catalog_helper.teardown()

    def setUp(self):
        self._catalog = self.catalog_helper.connect()

    def tearDown(self):
        self.catalog_helper.unit_teardown()
