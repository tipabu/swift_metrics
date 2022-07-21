import glob
import os
import pathlib
import queue
import typing

import swift.common.ring  # type: ignore
import swift.common.utils  # type: ignore
import swift.obj.diskfile  # type: ignore

from . import Stat
from . import StatCollection
from . import Tracker
from . import WriteOnceStatCollection


class PartitionCountStat(Stat):
    name = "partitions"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "Primary/handoff partition count"


class SuffixCountStat(Stat):
    name = "suffixes"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "Primary/handoff suffix count"


class HashdirCountStat(Stat):
    name = "hashdirs"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "Primary/handoff hashdir count"


class SwiftRingAssignmentTracker(Tracker):
    def configure(self, conf):
        self.devices_path = pathlib.Path(conf.get('devices', '/srv/node'))
        self.rings = {
            os.path.basename(r).split('.')[0]: swift.common.ring.Ring(r)
            for r in glob.glob('/etc/swift/*.ring.gz')}
        # TODO: support ring_ip config opt
        self.my_ips = set(swift.common.utils.whataremyips())
        self.worker_queue = queue.Queue()
        self.workers = [
            SwiftDiskRingAssignmentTracker(self.worker_queue, {
                'disk': disk,
                'manager': self,
            }) for disk in self.devices_path.iterdir()
        ]
        self.stats = StatCollection()
        self.track_hashdirs = swift.common.utils.config_true_value(
            conf.get('track_hashdirs', 'true'))

    def start(self):
        for t in self.workers:
            t.start()
        super().start()

    def get_stats(self):
        for t in self.workers:
            t.ever_reported.wait()

        while True:
            try:
                self.stats.update(self.worker_queue.get_nowait())
            except queue.Empty:
                break

        return WriteOnceStatCollection(self.stats)


class SwiftDiskRingAssignmentTracker(Tracker):
    interval = 60  # seconds

    def configure(self, conf):
        self.disk = conf['disk']
        self.manager = conf['manager']

    def scrape_time_labels(self):
        return super().scrape_time_labels() + (
            ("device", self.disk.name),
        )

    def get_stats(self):
        ret = WriteOnceStatCollection()
        for policy in self.disk.iterdir():
            stats = {
                'partitions': {'primary': 0, 'handoff': 0},
                'suffixes': {'primary': 0, 'handoff': 0},
                'hashdirs': {'primary': 0, 'handoff': 0},
            }
            try:
                ring = self.manager.rings[policy.name.replace('s', '')]
            except KeyError:
                # tmp or async_pending, most likely
                continue

            try:
                dev = next(
                    dev for dev in ring.devs
                    if dev and dev['device'] == self.disk.name
                    and (self.manager.my_ips & {
                        dev['ip'], dev['replication_ip']}))
            except StopIteration:
                # dev not in ring; always handoff
                dev = None

            try:
                for part in policy.iterdir():
                    try:
                        p = int(part.name)
                    except ValueError:
                        continue
                    ph = ('primary' if dev and dev['id'] in [
                        d['id'] for d in ring.get_part_nodes(p)
                    ] else 'handoff')
                    stats['partitions'][ph] += 1

                    # TODO: this only works for object policies
                    hashes = {'valid': False}
                    if policy.name.startswith('object'):
                        hashes = swift.obj.diskfile.read_hashes(part)
                    if hashes['valid']:
                        for h in hashes:
                            if not swift.obj.diskfile.valid_suffix(h):
                                continue
                            stats['suffixes'][ph] += 1
                            if self.manager.track_hashdirs:
                                stats['hashdirs'][ph] += (part / h).stat().st_nlink - 2
                    else:
                        for suf in part.iterdir():
                            if not swift.obj.diskfile.valid_suffix(suf.name):
                                continue
                            stats['suffixes'][ph] += 1
                            if self.manager.track_hashdirs:
                                stats['hashdirs'][ph] += suf.stat().st_nlink - 2

                now = Stat.now()
                ret.update(
                    PartitionCountStat(stats["partitions"]["primary"], now, (
                        ("device", self.disk.name),
                        ("policy", policy.name),
                        ("type", "primary"),
                    )),
                    PartitionCountStat(stats["partitions"]["handoff"], now, (
                        ("device", self.disk.name),
                        ("policy", policy.name),
                        ("type", "handoff"),
                    )),
                    SuffixCountStat(stats["suffixes"]["primary"], now, (
                        ("device", self.disk.name),
                        ("policy", policy.name),
                        ("type", "primary"),
                    )),
                    SuffixCountStat(stats["suffixes"]["handoff"], now, (
                        ("device", self.disk.name),
                        ("policy", policy.name),
                        ("type", "handoff"),
                    )),
                )
                if self.manager.track_hashdirs:
                    ret.update(
                        HashdirCountStat(stats["hashdirs"]["primary"], now, (
                            ("device", self.disk.name),
                            ("policy", policy.name),
                            ("type", "primary"),
                        )),
                        HashdirCountStat(stats["hashdirs"]["handoff"], now, (
                            ("device", self.disk.name),
                            ("policy", policy.name),
                            ("type", "handoff"),
                        )),
                    )
            except OSError:
                # failed disk? maybe at some point we should unmount it
                pass
        return ret


if __name__ == "__main__":
    SwiftRingAssignmentTracker.main()
