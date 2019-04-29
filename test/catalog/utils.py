import abc
import csv
import json
import os
from os.path import dirname as up
import unittest

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
        if file_format not in {self.CSV, self.JSON}:
            raise ValueError('Invalid file format')
        self.max_test_rows = 30
        self.data_dir = os.path.join(up(up(__file__)), 'data')
        self.file_format = file_format

        # 'samples' tabular data
        self.samples = 'samples.' + file_format
        self.samples_filename = os.path.join(self.data_dir, self.samples)

        # output data files expected
        self.unit_table_names = table_names
        self.unit_table_filenames = [os.path.join(self.data_dir, basename) for basename in table_names]

    def setup(self):
        os.makedirs(self.data_dir, exist_ok=True)

        rows = [
            {
                'id': i,
                'species': self.DUMMY_ROWS[i % self.DUMMY_LEN][1],
                'list_of_closest_genes': self.DUMMY_ROWS[i % self.DUMMY_LEN][2],
                'list_of_anatomical_structures': self.DUMMY_ROWS[i % self.DUMMY_LEN][3]
            } for i in range(self.max_test_rows)
        ]

        with open(self.samples_filename, 'w', newline='') as ofile:
            if self.file_format == self.CSV:
                csvwriter = csv.DictWriter(ofile, fieldnames=self.FIELDS)
                csvwriter.writeheader()
                csvwriter.writerows(rows)
            else:
                json.dump(rows, ofile)

    def teardown(self):
        self.unit_teardown(other=[self.samples_filename])
        os.rmdir(self.data_dir)

    def unit_teardown(self, other=[]):
        filenames = self.unit_table_filenames + other
        for filename in filenames:
            try:
                os.remove(filename)
            except FileNotFoundError:
                pass

    def exists(self, tablename):
        return os.path.isfile(os.path.join(self.data_dir, tablename))

    def connect(self):
        return chisel.connect(self.data_dir)


class ERMrestHelper (AbstractCatalogHelper):
    """Helper class that sets up and tears down an ERMrest catalog.
    """

    def setup(self):
        # create catalog
        pass

    def teardown(self):
        # delete catalog
        pass

    def unit_teardown(self, other=[]):
        # delete any mutated tables
        pass

    def exists(self, tablename):
        pass

    def connect(self):
        # connect to catalog
        pass


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
