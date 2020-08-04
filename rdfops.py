import rdflib as r
from pybars import Compiler
compiler = Compiler()


class NodeConstants:

    config_uri_stub = 'http://node/config/'
    processor_type = r.URIRef(config_uri_stub+'Processor')
    cron_predicate = r.URIRef(config_uri_stub+'cron')
    sparql_predicate = r.URIRef(config_uri_stub + 'sparql')



def get_class(class_name, node, config ):
    parts = class_name.split('.')
    module = ".".join(parts[:-1])
    m = __import__(module)
    for comp in parts[1:]:
        m = getattr(m, comp)
    obj = m(config=config)
    return obj


def ensure(value, uri_stub=None):
    if value is None:
        return None

    if not isinstance(value, r.URIRef):
        str_val = str(value)
        if str_val.startswith('http') and ':' in str_val:
            value = r.URIRef(value)
        else:
            if uri_stub is not None:
                value = r.URIRef(value, uri_stub)
            else:
                value = r.Literal(value)
    return value


def run_query(store, query_uri:r.URIRef, args=None):

    sparql = store.value(query_uri, NodeConstants.sparql_predicate)
    if sparql is None:
        raise Exception(f'the query {query_uri} does not have a predicate {NodeConstants.sparql_predicate}')
    template = compiler.compile(sparql)
    output = template(args)
    return store.run_sparql(output)