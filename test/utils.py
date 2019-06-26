import abc
import csv
import json
import os
from os.path import dirname as up
import unittest

from deriva.core import DerivaServer, ErmrestCatalog, urlquote, get_credential
from deriva.core.ermrest_model import Schema, Table, Column, Key, builtin_types

import chisel


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
        return chisel.connect(self._data_dir)


class ERMrestHelper (AbstractCatalogHelper):
    """Helper class that sets up and tears down an ERMrest catalog.
    """

    def __init__(self, hostname, catalog_id=None, unit_table_names=[], use_deriva_catalog_manage=False):
        """Initializes the ERMrest catalog helper

        :param hostname: hostname of the deriva test server
        :param catalog_id: optional id of catalog to _reuse_ by this unit test suite
        :param unit_table_names: list of names of tables used in unit tests
        :param use_deriva_catalog_manage: flag to use deriva catalog manage classes instead of deriva core classes
        """
        super(ERMrestHelper, self).__init__()
        self.samples = 'samples'
        self._hostname = hostname
        self._ermrest_catalog = None
        self._reuse_catalog_id = catalog_id
        self._unit_table_names = unit_table_names
        self._use_deriva_catalog_manage = use_deriva_catalog_manage

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
        # delete catalog
        assert isinstance(self._ermrest_catalog, ErmrestCatalog)
        if not self._reuse_catalog_id:
            self._ermrest_catalog.delete_ermrest_catalog(really=True)

    def unit_setup(self):
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
        samples = pb.schemas['public'].tables[self.samples]
        samples.insert(self.test_data)

    def unit_teardown(self, other=[]):
        # delete any mutated tables
        assert isinstance(self._ermrest_catalog, ErmrestCatalog)
        model = self._ermrest_catalog.getCatalogModel()
        for tablename in [self.samples] + self._unit_table_names + other:
            try:
                s, t = self._parse_table_name(tablename)
                model.schemas[s].tables[t].delete(self._ermrest_catalog)
            except Exception:
                pass

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
        except Exception:
            return False

    def connect(self):
        # connect to catalog
        assert isinstance(self._ermrest_catalog, ErmrestCatalog)
        return chisel.connect(
            'https://{hostname}/ermrest/catalog/{id}'.format(
                hostname=self._hostname,
                id=self._ermrest_catalog.catalog_id
            ),
            use_deriva_catalog_manage=self._use_deriva_catalog_manage
        )


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
        self._catalog = self.catalog_helper.connect()

    def tearDown(self):
        self.catalog_helper.unit_teardown()
