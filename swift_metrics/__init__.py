from __future__ import annotations
import collections.abc
import dataclasses
import io
import queue
import threading
import time
import typing


MEMCACHE_PORT = 11211
RSYNC_PORT = 873


def is_swift_port(port):
    return port == 8080 or 6200 <= port <= 6300


def categorize_destination_port(port):
    port = int(port)
    if port == 6200 or 6203 <= port <= 6300:
        return 'object'
    return {
        6201: 'container',
        6202: 'account',
        8080: 'proxy',
        11211: 'memcache',
        873: 'rsync',
    }.get(port) or f'other ({port!r})'


def parse_netloc(netloc):
    if not netloc:
        return None
    host, port = netloc.rpartition(':')[::2]
    return (host, int(port))


def unpack(d, lvl=1):
    assert lvl >= 1
    if lvl == 1:
        for key, val in d.items():
            yield key, val
        return
    for key, val in d.items():
        for items in unpack(val, lvl-1):
            yield (key, ) + items


@dataclasses.dataclass(frozen=True)
class Stat:
    name: typing.ClassVar[str]
    help: typing.ClassVar[str]
    type: typing.ClassVar[typing.Literal["gauge", "counter"]]
    value: typing.Any
    timestamp: typing.Optional[int] = None
    labels: typing.Tuple[typing.Tuple[str, str], ...] = ()

    @classmethod
    def now(cls) -> int:
        return int(time.time() * 1000)

    @classmethod
    def header(cls) -> str:
        return (f'# HELP {cls.name} {cls.help}\n'
                f'# TYPE {cls.name} {cls.type}\n')

    def __str__(self) -> str:
        buf = io.StringIO()
        buf.write(self.name)
        if self.labels:
            buf.write('{')
            for i, (label, value) in enumerate(self.labels):
                if i != 0:
                    buf.write(",")
                buf.write(label)
                buf.write('="')
                buf.write(str(value))
                buf.write('"')
            buf.write('}')
        buf.write(f' {self.value}')
        if self.timestamp is not None:
            buf.write(f' {self.timestamp}')
        buf.write('\n')
        return buf.getvalue()

    def zero(self):
        return dataclasses.replace(self, value=0)


class ScrapeTime(Stat):
    name = "scrape_time"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "Amount of time required to gather information (seconds)"


class StatCollection(collections.abc.Iterable):
    def __init__(self, stats: typing.Iterable[Stat] = None):
        self._stats: typing.Dict[Stat, Stat] = {}
        if stats:
            self.update(*stats)

    def __iter__(self):
        yield from self._stats.values()

    def update(self, *stats: Stat):
        for stat in stats:
            key = dataclasses.replace(stat, value=0, timestamp=None)
            self._stats[key] = stat

    def merge(self, other: "StatCollection"):
        self.update(*other)

    def doc(self) -> str:
        stats = list(self)
        doc = [t.header() for t in set(type(s) for s in stats)]
        doc.extend(str(s) for s in stats)
        return ''.join(doc)


class WriteOnceStatCollection(StatCollection):
    def update(self, *stats: Stat):
        for stat in stats:
            key = dataclasses.replace(stat, value=0, timestamp=None)
            if key in self._stats:
                raise ValueError(f"Already have a stat for {str(key).split(' ')[0]!r}")
            self._stats[key] = stat


class Tracker(threading.Thread):
    interval = 10  # seconds

    def __init__(self, stats_queue: queue.Queue, conf: dict):
        self.stats_queue = stats_queue
        self.ever_reported = threading.Event()
        super().__init__()
        self.daemon = True
        self.configure(conf)

    def configure(self, conf):
        """Hook for subclasses to validate and extract config"""

    def scrape_time_labels(self):
        return (
            ("tracker", self.__class__.__name__),
        )

    def run(self):
        while True:
            start = time.time()
            any_stats = False
            for stat in self.get_stats():
                self.stats_queue.put(stat)
                any_stats = True
            if any_stats:
                # Some trackers don't report until their *second* scrape
                self.ever_reported.set()
            delta = time.time() - start
            self.stats_queue.put(ScrapeTime(
                delta,
                Stat.now(),
                self.scrape_time_labels(),
            ))
            if self.interval - delta > 0:
                time.sleep(self.interval - delta)

    def get_stats(self) -> WriteOnceStatCollection:
        raise NotImplementedError

    @classmethod
    def main(cls):
        statq = queue.Queue()
        thread = cls(statq, {})
        thread.start()
        stats = StatCollection()
        while thread.is_alive():
            try:
                stat = statq.get(timeout=0.25)
            except queue.Empty:
                continue
            except KeyboardInterrupt:
                break
            # shouldn't take long to emit all the stats in the batch
            time.sleep(0.05)
            batch = StatCollection()
            batch.update(stat)
            print(str(stat), end="")
            while True:
                try:
                    stat = statq.get_nowait()
                except queue.Empty:
                    print()
                    break
                else:
                    batch.update(stat)
                    print(str(stat), end="")
            stats.merge(batch)
