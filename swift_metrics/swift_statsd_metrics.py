import collections
import re
import socket
import sys
import threading
from . import Stat
from . import Tracker
from . import WriteOnceStatCollection


class PromInf(float):
    def str(self):
        return "+Inf"


INF = PromInf('inf')


class SwiftBytesSentStat(Stat):
    name = 'swift_bytes_sent'
    type = 'counter'
    help = 'bytes transferred, as measured by Swift servers'


class SwiftRequestsStat(Stat):
    name = 'swift_requests'
    type = 'counter'
    help = 'Swift requests'


class SwiftReplicatorStat(Stat):
    name = 'swift_replication_counter'
    type = 'counter'
    help = 'Swift replication stats'


class SwiftShardLookupStat(Stat):
    name = 'swift_shard_lookup'
    type = 'counter'
    help = 'Swift shard lookup stats'


class Histogram(Stat):
    thresholds = [  # ms
        10,
        20,
        40,
        80,
        160,
        320,
        640,
        1280,
        2560,
        INF,
    ]
    type = 'counter'


class SwiftServerTimingHistogram(Histogram):
    name = 'swift_server_timing_bucket'
    help = 'Swift server timing histograms'


class SwiftServerTTFBHistogram(Histogram):
    name = 'swift_server_ttfb_bucket'
    help = 'Swift server ttfb histograms'


class SwiftAuditorTimingHistogram(Histogram):
    name = 'swift_auditor_timing_bucket'
    help = 'Swift auditor timing histograms'


class SwiftUpdaterTimingHistogram(Histogram):
    name = 'swift_updater_timing_bucket'
    help = 'Swift updater timing histograms'


class StatsdStat(Stat):
    name = 'statsd'
    type = 'counter'
    help = 'stats from Swift StatsD emission'


class StatsdTracker(Tracker):
    REG_EXPS = [
        (re.compile(r"(?P<daemon>(?P<server>proxy)-server)\."
                    r"(?P<target_layer>account|container|object)\."
                    r"(?P<method>[^.]*)\."
                    r"(?P<status>\d*)\."
                    r"xfer"), SwiftBytesSentStat),
        (re.compile(r"(?P<daemon>(?P<server>proxy)-server)\."
                    r"(?P<target_layer>object)\."
                    r"policy\.(?P<policy_idx>\d*)\."
                    r"(?P<method>[^.]*)\."
                    r"(?P<status>\d*)\."
                    r"xfer"), SwiftBytesSentStat),
        (re.compile(r"(?P<daemon>(?P<target_layer>object)-replicator)\."
                    r"partition\."
                    r"(?P<job_type>delete|update)\."
                    r"count\."
                    r"(?P<device>[^.]*)"), SwiftReplicatorStat),
        (re.compile(r"(?P<daemon>(?P<server>proxy)-server)\."
                    r"(?P<target_layer>container|object)\."
                    r"(?P<type>shard_(?:listing|updating))\."
                    r"(?P<lookup>cache|backend)\."
                    r"(?P<status>[^.]*)"), SwiftShardLookupStat),
        (re.compile(r"(?P<daemon>(?P<server>proxy)-server)\."
                    r"(?P<target_layer>account|container|object)\."
                    r"(?P<method>[^.]*)\."
                    r"(?P<status>[^.]*)\."
                    r"timing"), SwiftServerTimingHistogram),
        (re.compile(r"(?P<daemon>(?P<server>proxy)-server)\."
                    r"(?P<target_layer>account|container|object)\."
                    r"(?P<method>[^.]*)\."
                    r"(?P<status>[^.]*)\."
                    r"first-byte\.timing"), SwiftServerTTFBHistogram),
        (re.compile(r"(?P<daemon>(?P<server>proxy)-server)\."
                    r"(?P<target_layer>object)\."
                    r"policy\.(?P<policy_idx>\d*)\."
                    r"(?P<method>[^.]*)\."
                    r"(?P<status>[^.]*)\."
                    r"timing"), SwiftServerTimingHistogram),
        (re.compile(r"(?P<daemon>(?P<server>proxy)-server)\."
                    r"(?P<target_layer>object)\."
                    r"policy\.(?P<policy_idx>\d*)\."
                    r"(?P<method>[^.]*)\."
                    r"(?P<status>[^.]*)\."
                    r"first-byte\.timing"), SwiftServerTTFBHistogram),
        (re.compile(r"(?P<daemon>(?P<server>account|container|object)-server)\."
                    r"(?P<method>[^.]*)\."
                    r"timing"), SwiftServerTimingHistogram),
        (re.compile(r"(?P<daemon>(?P<server>account|container|object)-auditor)\."
                    r"timing"), SwiftAuditorTimingHistogram),
        (re.compile(r"(?P<daemon>(?P<server>container|object)-updater)\."
                    r"timing"), SwiftUpdaterTimingHistogram),

    ]

    def statsd_server(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('127.0.0.1', 8125))

        unhandled_stats = set()
        while True:
            data, _ = sock.recvfrom(2048)
            line = data.decode('utf8').strip('\n')
            stat, _, value = line.rpartition(':')
            value, _, sample_rate = value.partition('|@')
            sample_rate = float(sample_rate) if sample_rate else 1

            for expr, cls in self.REG_EXPS:
                m = expr.match(stat)
                if not m:
                    continue
                labels = tuple(m.groupdict().items())
                if value.endswith('|c'):
                    self.stats[labels, cls] += int(value[:-2])/sample_rate
                    if cls is SwiftBytesSentStat:
                        self.stats[labels, SwiftRequestsStat] += 1/sample_rate
                    break
                if value.endswith('|ms'):
                    # histogram time!
                    value = float(value[:-3])
                    for threshold in cls.thresholds:
                        if value <= threshold:
                            labels_t = labels + (
                                ('le', str(threshold)),
                            )
                            self.stats[labels_t, cls] += 1/sample_rate
                    break
            else:
                if stat not in unhandled_stats:
                    print(line, file=sys.stderr)
                    unhandled_stats.add(stat)

    def configure(self, conf):
        self.stats = collections.defaultdict(int)
        threading.Thread(target=self.statsd_server, daemon=True).start()

    def get_stats(self):
        now = Stat.now()
        return WriteOnceStatCollection(
            cls(int(value), now, labels)
            for (labels, cls), value in self.stats.items()
        )


if __name__ == '__main__':
    StatsdTracker.main()
