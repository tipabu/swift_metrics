from . import Stat
from . import StatCollection
from . import WriteOnceStatCollection
from .df_stats import DiskTracker
from .iptables_counters import IPTablesTracker
from .process_info import ProcessTracker
from .swift_stats import SwiftRingAssignmentTracker

import queue
import sys
import threading
import time
import wsgiref.simple_server


class Manager(threading.Thread):
    WORKER_CLASSES = (
        DiskTracker,
        IPTablesTracker,
        SwiftRingAssignmentTracker,
        ProcessTracker,
    )

    def __init__(self) -> None:
        self.statq: queue.Queue[Stat] = queue.Queue()
        self.stats = StatCollection()
        self.workers = [cls(self.statq, {}) for cls in self.WORKER_CLASSES]
        super().__init__()
        self.daemon = True

    def run(self) -> None:
        for t in self.workers:
            t.start()
        while all(t.is_alive() for t in self.workers):
            self.stats.update(self.statq.get())

    def get_stats(self) -> WriteOnceStatCollection:
        for t in self.workers:
            t.ever_reported.wait()
        while not self.statq.empty():
            time.sleep(0.05)
        return WriteOnceStatCollection(self.stats)


m = Manager()
m.start()


if 'server' in sys.argv or 'serve' in sys.argv:
    def app(env, start_response):  # type: ignore
        if env['PATH_INFO'] != '/metrics':
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return [b'Not Found']
        body = m.get_stats().doc().encode('utf-8')
        start_response('200 OK', [
            ('Content-Length', str(len(body))),
            ('Content-Type', 'text/plain'),
        ])
        return [body]

    with wsgiref.simple_server.make_server('', 8001, app) as httpd:
        httpd.serve_forever()

else:
    print(m.get_stats().doc(), end='')
