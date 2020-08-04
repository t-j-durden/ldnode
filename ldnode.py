from concurrent.futures.thread import ThreadPoolExecutor
import pathlib
from typing import Dict, Any
from rdfops import *
from store import Store, InMemoryStore
from web import *


class LdNode:
    def __init__(self, config: 'Config' = None, uri=None, uri_stub=None, parent: 'LdNode' = None):
        self.running_async = False
        self.bus = Bus()
        self.processors = {}
        self.config: 'Config' = config
        self.parent = parent
        self.loaded_objects: Dict[r.URIRef:, Any] = {}

        if config is not None:
            self.config = config if config is Config else Config(config)
            self.config.parent = self
            if uri is None:
                uri = self.config.uri
            self.config_instance = self.config.subject(uri)
        self.uri = uri
        self.uri_stub = uri if uri_stub is None else uri_stub

        self.store: Store = InMemoryStore(self.uri_stub)
        if self.config is not None:
            for t in self.config.store.filter((None, None, None)):
                self.store.add_triple(t)

        self.load_from_config()

    def load_from_config(self):
        if self.config is None:
            return
        self.load_processors()

    def start(self):
        self.emit('before-started')
        self.start_sync_loop()
        host = self.config_instance.value('host', 'localhost')
        port = self.config_instance.value('http_port', 8899)
        start_http_server(self, host, port)
        self.emit('after-started')

    def stop(self):
        stop_http_server()
        self.stop_sync_loop()
        self.emit('before-stop')
        self.emit('after-stop')

    def stop_sync_loop(self):
        self.running_async = False

    def start_sync_loop(self):
        if self.running_async:
            raise Exception('the loop is already running!')
        self.running_async = True

    def get_processor_subjects(self):
        results = self.config.filter(obj=NodeConstants.processor_type)
        for res in results:
            uri = res[0]
            info = self.config.subject(uri)
            yield info

    def load_processors(self):
        # TODO: need to diff here https://rdflib.readthedocs.io/en/4.0/_modules/rdflib/compare.html
        # TODO: need to stop running processors
        # TODO: type inheritance
        self.processors.clear()
        for info in self.get_processor_subjects():
            event_uris = info.values('on')
            for event_uri in event_uris:
                self.register_event(event_uri, info)
            self.processors[info.uri] = info.obj()

    def register_event(self, event_uri, info: 'Subject'):
        method_name = info.value('default-method', 'run')
        events_to_raise = info.values('raise')

        class _wrapper:
            def __init__(self, func, events, node):
                self.func = func
                self.events = events
                self.node = node

            def __call__(self, *args, **kwargs):
                self.func(*args, **kwargs)
                for e in self.events:
                    self.node.emit(e)

        method_to_call = getattr(info.obj(), method_name)
        self.on(event_uri, _wrapper(method_to_call, events_to_raise, self))

    def add_data(self, s, p, o):
        self.store.add(s, p, o)

    def on(self, event, func=None):
        event = ensure(event, self.uri_stub)
        self.bus.on(str(event), func)

    def emit(self, event, *args, **kwargs):
        event = ensure(event, self.uri_stub)
        if self.running_async:
            self.bus.emit_async(str(event), *args, **kwargs)
        else:
            self.bus.emit(str(event), *args, **kwargs)

    def value(self, subject, predicate):
        return self.store.value(ensure(subject, self.uri_stub), ensure(predicate, self.uri_stub))

    def filter(self, subject=None, predicate=None, obj=None):
        triple = (ensure(subject, self.uri_stub), ensure(predicate, self.uri_stub), ensure(obj))
        return self.store.filter(triple)

    def subject(self, name) -> 'Subject':
        return Subject(ensure(name, NodeConstants.config_uri_stub), self)

    def obj(self, name):
        return self.subject(name).obj()


class Bus:
    def __init__(self):
        self.pool = ThreadPoolExecutor(5)
        self.listeners = {}

    def on(self, event, func=None):
        if event not in self.listeners:
            self.listeners[event] = []
        self.listeners[event].append(func)

    def emit(self, event, *args, **kwargs):
        if event not in self.listeners:
            return
        for listener in self.listeners[event]:
            listener(*args, **kwargs)

    def emit_async(self, event, *args, **kwargs):
        if event not in self.listeners:
            return
        for listener in self.listeners[event]:
            self.pool.submit(lambda: listener(*args, **kwargs))

# todo: extract as interface and allow subject just to be an object/dictionary as well
class Subject:
    def __init__(self, uri, node: LdNode, parent=None):
        self.node = node
        self.uri = ensure(uri)
        self.parent = parent

    def value(self, prop, default_value=None, allow_none = False):
        predicate = ensure(prop, NodeConstants.config_uri_stub)
        value = self.node.store.value(self.uri, predicate)
        if value is not None:
            return self.obj_for_item(value)
        if self.parent is not None:
            value = self.parent.value(prop)
        if value is not None:
            return self.obj_for_item(value)

        if default_value is not None or allow_none:
            return default_value
        else:
            raise Exception(f'{predicate} does not exist on {self.uri} in {self.node.uri}')

    def values(self, prop):
        predicate = ensure(prop, NodeConstants.config_uri_stub)
        values = self.node.store.filter((self.uri, predicate, None))
        return [x[2] for x in values]

    def obj(self):
        return self.obj_for_item(self.uri)

    def obj_for_item(self, item):
        if item in self.node.parent.processors:
            return self.node.parent.processors[item]
        if type(item) is r.URIRef:
            if item in self.node.loaded_objects:
                return self.node.loaded_objects[item]
            else:
                class_name = self.node.value(item, 'class-name')
                if class_name is None:
                    return item
                else:
                    return get_class(class_name, self.node.parent, self)
        elif type(item) is r.Literal:
            return item.value
        else:
            return item


class Config(LdNode):
    def __init__(self, file_path=None, data=None, rdf_format='turtle'):
        uri = NodeConstants.config_uri_stub + 'data' \
            if file_path is None else pathlib.Path(file_path).absolute().as_uri()
        super().__init__(uri=uri, uri_stub=NodeConstants.config_uri_stub)
        open_file = None if file_path is None else open(file_path)
        self.store.parse(file=open_file, data=data, format=rdf_format)

