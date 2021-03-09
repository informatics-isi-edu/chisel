"""Tests for the Shred operator.
"""
import itertools
import os
import unittest
import rdflib as _rdflib
import deriva.chisel.operators as _op
from deriva.chisel.catalog.semistructured import shred as chisel_shred

# flag to run all unit tests
CHISEL_TEST_ALL = os.getenv('CHISEL_TEST_ALL')

# local path to a suitable owl file to use for these tests
# consider fetching 'http://purl.obolibrary.org/obo/zfs.owl'
CHISEL_TEST_OWL_PATH = os.getenv('CHISEL_TEST_OWL_PATH')

# SPARQL expression to extract the id (i.e., short identifier) and name (i.e., preferred readable name) from the graph
sparql_class_and_props = """
SELECT DISTINCT ?id (?label AS ?name)
WHERE {
  ?s oboInOwl:id ?id .
  ?s rdfs:label ?label .
}"""

# this is just a dummy value used for passing assertions so that other parameters can trip an error condition
dummy_graph = dict(dummy="just a dummy")


class TestShred (unittest.TestCase):
    """Basic tests for Shred operator."""

    def test_invalid_graph_parameter(self):
        """Tests expected exception from invalid graph object."""
        with self.assertRaises(AssertionError):
            _op.Shred(None, "SELECT ?id")
        with self.assertRaises(ValueError):
            chisel_shred(None, "SELECT ?id")

    def test_invalid_expression_parameter(self):
        """Tests expected exception from invalid expression string."""
        with self.assertRaises(AssertionError):
            _op.Shred(dummy_graph, "")
        with self.assertRaises(ValueError):
            chisel_shred(dummy_graph, "")

    def test_invalid_graph_execution(self):
        """Tests expected exception from executing shred operation on invalid graph object."""
        with self.assertRaises(ValueError):
            list(_op.Shred(dummy_graph, "SELECT ?id WHERE { ?id ... }"))

    @unittest.skipUnless(CHISEL_TEST_OWL_PATH, 'you need an owl file for this test')
    def test_shallow_introspection(self):
        """Test shallow introspection of the shredded table description."""
        for cnames, expression in [
            (['id'], "SELECT DISTINCT ?id WHERE ..."),
            (['id', 'name'], "SELECT DISTINCT ?id ?name WHERE ..."),
            (['name'], "SELECT DISTINCT ?foo AS ?name WHERE ..."),
            (['name'], "SELECT DISTINCT (?foo AS ?name) WHERE ..."),
            (['name'], "SELECT DISTINCT (count(?foo) AS ?name) WHERE ..."),
            (['name'], "SELECT DISTINCT (count(?foo) * 3 AS ?name) WHERE ..."),
            (['foo', 'bar'], "SELECT DISTINCT (?boo AS ?foo) (count(?foo) * 3 AS ?bar) WHERE ..."),
            (['foo', 'bar'], "SELECT DISTINCT ?boo AS ?foo (?baz AS ?bar) WHERE ..."),
            (['id', 'name'], "SELECT DISTINCT (?foo AS ?id) ?name WHERE ..."),
            (['id', 'name'], "select Distinct (?foo as ?id) ?name Where ..."),
            (['id', 'name', 'bar'], "SELECT DISTINCT ?id (?foo AS ?name) ?foo AS ?bar WHERE ..."),
            (['id', 'name', 'bar'], "SELECT ?id (?foo AS ?name) ?foo AS ?bar WHERE ..."),
            (['id', 'name'], sparql_class_and_props)
        ]:
            with self.subTest(expression=expression):
                shred = _op.Shred(CHISEL_TEST_OWL_PATH, expression)
                self.assertSequenceEqual(
                    cnames,
                    [col['name'] for col in shred.description['column_definitions']],
                    'shallow introspection of columns from SPARQL query expression failed'
                )

    @unittest.skipUnless(CHISEL_TEST_ALL, 'deep introspection of RDF graph is slow')
    @unittest.skipUnless(CHISEL_TEST_OWL_PATH, 'you need an owl file for this test')
    def test_deep_introspection(self):
        """Test shallow introspection of the shredded table description."""

        # reusable where clause for deep introspection tests
        sparql_where_clause = """
        WHERE {
          ?s oboInOwl:id ?id .
          ?s rdfs:label ?label .
        }"""

        # parse the graph in advance to save on repeated parsing effort below
        graph = _rdflib.Graph()
        graph.parse(CHISEL_TEST_OWL_PATH)

        for cnames, expression in [
            (['id', 'name'], sparql_class_and_props),
            (['foo'], "SELECT DISTINCT (?id AS ?foo) " + sparql_where_clause),
            (['foo'], "select (?id AS ?foo) " + sparql_where_clause),
            (['foo'], "SELECT DISTINCT (count(?id) AS ?foo) " + sparql_where_clause),
            (['count_foo'], "SELECT DISTINCT (count(?id) * 2 AS ?count_foo) " + sparql_where_clause),
            (['foo', 'bar'], "SELECT DISTINCT (?id AS ?foo) (?label AS ?bar) " + sparql_where_clause)
        ]:
            with self.subTest(expression=expression):
                shred = _op.Shred(graph, expression, introspect='deep')
                self.assertSequenceEqual(
                    cnames,
                    [col['name'] for col in shred.description['column_definitions']],
                    'deep introspection of columns from SPARQL query expression failed'
                )

    @unittest.skipUnless(CHISEL_TEST_ALL, 'shredding an RDF graph is slow')
    @unittest.skipUnless(CHISEL_TEST_OWL_PATH, 'you need an owl file for this test')
    def test_shredding_rows(self):
        """Test shredding rows from a graph."""
        shred = _op.Shred(CHISEL_TEST_OWL_PATH, sparql_class_and_props)
        for row in itertools.islice(shred, 100):
            self.assertIn('id', row)
            self.assertIn('name', row)
