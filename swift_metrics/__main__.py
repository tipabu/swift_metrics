from . import Stat
from . import StatCollection
from . import WriteOnceStatCollection
from .df_stats import DiskTracker
from .iptables_counters import IPTablesTracker
from .ntp_stats import TimeSyncTracker
from .process_info import ProcessTracker
from .swift_stats import SwiftRingAssignmentTracker
from .swift_statsd_metrics import StatsdTracker
from .swift_object_replication import SwiftObjectReplicationTracker

import queue
import sys
import threading
import time
import urllib.parse
import wsgiref.simple_server


class Manager(threading.Thread):
    WORKER_CLASSES = (
        DiskTracker,
        IPTablesTracker,
        SwiftRingAssignmentTracker,
        SwiftObjectReplicationTracker,
        StatsdTracker,
        ProcessTracker,
        TimeSyncTracker,
    )
    MAX_AGE = 150  # seconds

    def __init__(self) -> None:
        self.statq: queue.Queue[Stat] = queue.Queue()
        self.stats = StatCollection()
        self.workers = [cls(self.statq, {}) for cls in self.WORKER_CLASSES]
        super().__init__()
        self.daemon = True

    def run(self) -> None:
        for t in self.workers:
            t.start()
        last = time.time()
        while all(t.is_alive() for t in self.workers):
            self.stats.update(self.statq.get())
            now = time.time()
            if now - last > self.MAX_AGE:
                self.stats.prune(self.MAX_AGE * 1000)
                last = min(
                    (s.timestamp / 1000 for s in self.stats
                     if s.timestamp is not None),
                    default=now)

    def get_stats(self) -> WriteOnceStatCollection:
        for t in self.workers:
            t.ever_reported.wait()
        for _ in range(3):
            if self.statq.empty():
                break
            time.sleep(0.05)
        return WriteOnceStatCollection(self.stats)


m = Manager()
m.start()


if 'server' in sys.argv or 'serve' in sys.argv:
    def app(env, start_response):  # type: ignore
        if env['PATH_INFO'] != '/metrics':
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return [b'Not Found']
        params = urllib.parse.parse_qs(env.get('QUERY_STRING'))
        stats = m.get_stats()
        if 'name' in params:
            stats = WriteOnceStatCollection(
                s for s in stats if s.name in params['name'])
        body = stats.doc().encode('utf-8')
        start_response('200 OK', [
            ('Content-Length', str(len(body))),
            ('Content-Type', 'text/plain'),
        ])
        return [body]

    with wsgiref.simple_server.make_server('', 8000, app) as httpd:
        httpd.serve_forever()

else:
    print(m.get_stats().doc(), end='')
