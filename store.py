from rdflib.plugins.sparql.processor import SPARQLResult
from rdfops import *


class Store:
    def parse(self, **kwargs):
        pass

    def query(self, name, *args):
        pass

    def value(self, subject, predicate):
        pass

    def filter(self, triple):
        pass

    def run_sparql(self, sparql):
        pass

    def add_triple(self, triple):
        pass

    def add(self, s, p, o):
        pass


class InMemoryStore(Store):
    def __init__(self, uri_sub):
        self.g = r.Graph()
        self.uri_stub = uri_sub

    def parse(self, **kwargs):
        self.g.parse(**kwargs)

    def value(self, subject: r.URIRef, predicate:r.URIRef):
        value = self.g.value(subject, predicate)
        return value

    def filter(self, triple):
        return self.g.triples(triple)

    def query(self, name, args=None) -> SPARQLResult:
        return run_query(self, ensure(name, self.uri_stub), args)

    def run_sparql(self, sparql):
        return self.g.query(sparql)

    def add_triple(self, triple):
        self.g.add(triple)

    def add(self, s, p, o):
        self.g.add((ensure(s, self.uri_stub), ensure(p, self.uri_stub), ensure(o)))

