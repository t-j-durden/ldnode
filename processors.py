import threading
import time
from datetime import datetime
from typing import Dict
import rdflib as r
from croniter import croniter
from ldnode import Subject
from rdfops import NodeConstants


class ConstructProcessor:
    def __init__(self, config: Subject = None):
        self.query = config.value('query', config.uri)
        self.node = config.node.parent

    def run(self):
        results = self.node.store.query(self.query)
        for result in results:
            self.node.store.add_triple(result)
        self.node.emit('construct_complete')


class Scheduler:
    def __init__(self, config: Subject):
        self.node = config.node.parent
        self.config = config
        self.node.on('run-scheduler', self.run_once)
        self.schedules: Dict[r.URIRef, datetime] = {} #todo: need to handle cron changes
        self.node.on('after-started', self.start)
        self.node.on('before-stop', self.stop)
        self.running = False

    def start(self):
        if self.running:
            return
        self.running = True
        threading.Thread(target=self.run).start()
        self.node.emit('scheduler-started')

    def stop(self):
        self.running = False

    def run(self):
        print('scheduler loop started')
        while self.running:
            self.run_once()
            # this creates a loop
            time.sleep(self.config.value('sleep-for', 0.1))
        print('scheduler loop stopped')

    def run_once(self):
        for info in self.node.get_processor_subjects():
            self.check_processor_run(info)

    def check_processor_run(self, info):
        cron = info.value(NodeConstants.cron_predicate, 'none')
        if cron != 'none':
            uri = info.uri
            run_event = str(uri) + "-schedule-run"
            next_run = self.schedules[uri] if uri in self.schedules else None
            now = datetime.now()
            if next_run is None or now >= next_run:
                if next_run is None:
                    self.node.register_event(run_event, info)
                next_next_run = None if cron == 'poll' else croniter(cron, now).next(datetime)
                self.schedules[uri] = next_next_run
                self.node.emit(run_event)