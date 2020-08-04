import ast
import csv
from typing import Dict, Any
import rdflib as r
from ldnode import Subject
from rdfops import compiler, ensure
from dateutil.parser import parse


def guess_type(value):
    if value is None or type(value) is not str:
        return value
    try:
        return int(str(value))
    except ValueError:
        try:
            return float(str(value))
        except ValueError:
            try:
                return parse(str(value))
            except ValueError:
                return value


class Mapper:
    def __init__(self, subject_template: str, uri_stub, type_uri=None, mappings: str = None, auto_guess_type=False):
        self.auto_guess_type = auto_guess_type
        self.subject_template = compiler.compile(subject_template)
        self.uri_stub = uri_stub
        self.type_uri = ensure(type_uri, uri_stub)
        self.mappings: dict[str, str] = None if mappings is None else ast.literal_eval(mappings)

    def to_rdf(self, row: Dict[str, Any]):
        result = []
        subject = ensure(self.subject_template(row), self.uri_stub)
        if self.type_uri is not None:
            result.append((subject, r.RDF.type, self.type_uri))
        for key, value in row.items():
            s = ensure(subject, self.uri_stub)
            p = ensure(key, self.uri_stub)
            if self.mappings is not None and key in self.mappings:
                mapping = self.mappings[key]
                mapping = compiler.compile(mapping)(row)
                mapping = str(mapping).replace('\'', '"')
                value = eval(mapping)
            if self.auto_guess_type:
                value = guess_type(value)
            o = ensure(value)
            if o is not None:
                result.append((s, p, o))
        return result


class Source:
    def read(self):
        pass


class Target:
    # todo: combine store and read
    def store(self, triples):
        pass

    async def read(self, rsp):
        pass

    def start(self):
        pass

    def end(self):
        pass


class Parser:
    def __init__(self, mapper: Mapper, writer: Target):
        self.mapper = mapper
        self.writer = writer

    def parse(self, file_like):
        pass


class CsvParser(Parser):
    def __init__(self, mapper: Mapper, writer: Target, delimiter=',', quote_char='|'):
        super(CsvParser, self).__init__(mapper, writer)
        self.quote_char = quote_char
        self.delimiter = delimiter

    def parse(self, file_like):
        reader = csv.DictReader(file_like, delimiter=self.delimiter, quotechar=self.quote_char)
        self.writer.start()
        for row in reader:
            triples = self.mapper.to_rdf(row)
            self.writer.store(triples)
        self.writer.end()


class FileSource(Source):
    def __init__(self, file_path: str, new_line=''):
        self.file_path = file_path
        self.new_line = new_line

    def read(self, parser: Parser):
        with open(self.file_path, newline=self.new_line) as this_file:
            parser.parse(this_file)


class FileTarget(Target):
    def __init__(self, file_name):
        self.file_name = file_name
        self.g: r.Graph = None

    def start(self):
        self.g = r.Graph()

    def end(self):
        self.g.serialize(destination=self.file_name, format='turtle')

    def store(self, triples):
        for t in triples:
            self.g.add(t)

    async def read(self, rsp):
        return await rsp.file_stream(self.file_name)


class Publisher:
    def __init__(self, config: Subject):
        subject_template = config.value('subject-template')
        uri_stub = config.value('uri-stub')
        type_uri = config.value('type', None, True)
        mappings = config.value('mappings', None, True)
        self.mapper = Mapper(subject_template, uri_stub, type_uri, mappings, config.value('guess-types', None, True))
        self.reader = FileSource(config.value('file'))
        self.source = FileTarget('test_data.ttl')
        self.parser = CsvParser(self.mapper, self.source)
        self.cache = config.value('cache', False)

    def run(self):
        if self.cache:
            self.reader.read(self.parser)

    async def get_data(self, rsp):
        if not self.cache:
            self.reader.read(self.parser)
        return await self.parser.writer.read(rsp)


class Consumer:
    def __init__(self, config: Subject):
        self.node = config.node.parent
        self.url = config.value('target')

    def run(self):
        self.node.store.parse(source=self.url, format='turtle')
