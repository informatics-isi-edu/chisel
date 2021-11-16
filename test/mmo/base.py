"""Base class for MMO test cases.
"""
import os
import logging
import unittest
from deriva import chisel
from deriva.core import DerivaServer, ErmrestCatalog, get_credential
from deriva.core.ermrest_model import Schema, Table, Column, Key, ForeignKey, tag, builtin_types

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv('DERIVA_PY_TEST_LOGLEVEL', default=logging.WARNING))
ermrest_hostname = os.getenv('DERIVA_PY_TEST_HOSTNAME')
ermrest_catalog_id = os.getenv('DERIVA_PY_TEST_CATALOG')
catalog = None

# todo: add 'search-box' annotation(s)

# baseline annotation doc for `dept` table
dept_annotations = {
    "tag:isrd.isi.edu,2016:visible-columns": {
        "compact": [
            ["org", "dept_RID_key"],
            "dept_no",
            "name"
        ],
        "detailed": [
            "RID",
            "RCT",
            {
                "source": "RMT",
                "markdown_name": "Last Modified Time"
            },
            "dept_no",
            "name",
            {
                "source": "street_address",
                "markdown_name": "Number and Street Name"
            },
            "postal_code",
            {
                "sourcekey": "head_count",
                "markdown_name": "Head Count"
            },
            {
                "display": {
                    "wait_for": [
                        "personnel"
                    ],
                    "template_engine": "handlebars",
                    "markdown_pattern": "{{#each personnel}}{{{this.values.name}}}{{#unless @last}}, {{/unless}}{{/each}}."
                },
                "markdown_name": "Personnel"
            }
        ]
    },
    "tag:isrd.isi.edu,2016:visible-foreign-keys": {
        "*": [
            [
                "org",
                "person_dept_fkey"
            ]
        ]
    },
    "tag:isrd.isi.edu,2019:source-definitions": {
        "columns": [
            "dept_no",
            "name",
            "RID",
            "country"
        ],
        "sources": {
            "personnel": {
                "source": [
                    {
                        "inbound": [
                            "org",
                            "person_dept_fkey"
                        ]
                    },
                    "name"
                ]
            },
            "head_count": {
                "source": [
                    {
                        "inbound": [
                            "org",
                            "person_dept_fkey"
                        ]
                    },
                    "RID"
                ],
                "entity": False,
                "aggregate": "cnt_d"
            }
        }
    }
}

# baseline annotation doc for `person` table
person_annotations = {
    "tag:isrd.isi.edu,2016:visible-columns": {
        "compact": [
            ["org", "person_RID_key"],
            "name"
        ],
        "detailed": [
            "RID",
            "name",
            ["org", "person_dept_fkey"],
            {
                "markdown_name": "Department Name",
                "source": [
                    {
                        "outbound": [
                            "org",
                            "person_dept_fkey"
                        ]
                    },
                    "name"
                ],
                "entity": False
            },
            {
                "sourcekey": "dept_size",
                "markdown_name": "Department Size"
            },
            {
                "sourcekey": "dept_city",
                "markdown_name": "City"
            },
            {
                "markdown_name": "State or Province",
                "source": [
                    {
                        "outbound": [
                            "org",
                            "person_dept_fkey"
                        ]
                    },
                    "state"
                ],
                "entity": False
            },
        ]
    },
    "tag:isrd.isi.edu,2019:source-definitions": {
        "columns": [
            "RID",
            "name",
            "dept"
        ],
        "fkeys": [
            ["org", "person_dept_fkey"]
        ],
        "sources": {
            "dept_size": {
                "source": [
                    {
                        "outbound": [
                            "org",
                            "person_dept_fkey"
                        ]
                    },
                    {
                        "inbound": [
                            "org",
                            "person_dept_fkey"
                        ]
                    },
                    "RID"
                ],
                "entity": False,
                "aggregate": "cnt_d"
            },
            "dept_city":             {
                "markdown_name": "City",
                "source": [
                    {
                        "outbound": [
                            "org",
                            "person_dept_fkey"
                        ]
                    },
                    "city"
                ],
                "entity": False
            }
        }
    }
}


@unittest.skipUnless(ermrest_hostname, 'ERMrest hostname not defined.')
class BaseMMOTestCase (unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        global catalog

        # create catalog
        server = DerivaServer('https', ermrest_hostname, credentials=get_credential(ermrest_hostname))
        if ermrest_catalog_id:
            logger.debug(f'Connecting to {ermrest_hostname}/ermrest/catalog/{ermrest_catalog_id}')
            catalog = server.connect_ermrest(ermrest_catalog_id)
        else:
            catalog = server.create_ermrest_catalog()
            logger.debug(f'Created {ermrest_hostname}/ermrest/catalog/{catalog.catalog_id}')

        # get the chiseled model
        model = chisel.Model.from_catalog(catalog)

        # drop `org` schema, if exists
        try:
            model.schemas['org'].drop(cascade=True)
        except Exception as e:
            logger.debug(e)

        # create `org` schema
        model.create_schema(
            Schema.define('org')
        )

        # create `dept` table
        model.schemas['org'].create_table(
            Table.define(
                'dept',
                column_defs=[
                    Column.define('dept_no', builtin_types.int8),
                    Column.define('name', builtin_types.text),
                    Column.define('street_address', builtin_types.text),
                    Column.define('city', builtin_types.text),
                    Column.define('state', builtin_types.text),
                    Column.define('country', builtin_types.text),
                    Column.define('postal_code', builtin_types.int8)
                ],
                key_defs=[
                    Key.define(['dept_no'])
                ],
                annotations=dept_annotations
            )
        )

        # create `person` table
        model.schemas['org'].create_table(
            Table.define(
                'person',
                column_defs=[
                    Column.define('name', builtin_types.text),
                    Column.define('dept', builtin_types.int8)
                ],
                fkey_defs=[
                    ForeignKey.define(['dept'], 'org', 'dept', ['dept_no'])
                ],
                annotations=person_annotations
            )
        )

        # populate for good measure (though not necessary for current set of tests)
        pbuilder = catalog.getPathBuilder()

        pbuilder.org.dept.insert([
            {'dept_no': 1, 'name': 'Dept A', 'street_address': '123 Main St', 'city': 'Anywhere', 'state': 'CA', 'country': 'US', 'postal_code': 98765},
            {'dept_no': 2, 'name': 'Dept B', 'street_address': '777 Oak Ave', 'city': 'Somewhere', 'state': 'NY', 'country': 'US', 'postal_code': 12345}
        ])

        pbuilder.org.person.insert([
            {'name': 'John', 'dept': 1},
            {'name': 'Helena', 'dept': 1},
            {'name': 'Ben', 'dept': 1},
            {'name': 'Sonia', 'dept': 2},
            {'name': 'Rafael', 'dept': 2},
        ])

    @classmethod
    def tearDownClass(cls):
        global catalog
        if not ermrest_catalog_id and isinstance(catalog, ErmrestCatalog) and int(catalog.catalog_id) > 10000:
            # note: ... > 10000 is simply a guard against accidentally pointing at and deleting a production catalog
            catalog.delete_ermrest_catalog(really=True)
        catalog = None

    def setUp(self):
        # reset annotations to baseline
        assert isinstance(catalog, ErmrestCatalog)
        self.model = catalog.getCatalogModel()

    def tearDown(self):
        pass