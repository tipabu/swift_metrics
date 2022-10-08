import json
import typing

from . import Stat
from . import Tracker
from . import WriteOnceStatCollection


class ObjectReplicationStat(Stat):
    name = "object_replication"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "attempted/success/failure/etc stats"


class SwiftObjectReplicationTracker(Tracker):
    def get_stats(self) -> WriteOnceStatCollection:
        stats = WriteOnceStatCollection()
        try:
            with open('/var/cache/swift/object.recon', 'r') as fp:
                data = json.load(fp)
        except (IOError, ValueError):
            data = {}  # doesn't exist? bad recon data? nothing to report

        for device in data.get('object_replication_per_disk', {}):
            dev_data = data['object_replication_per_disk'][device]
            ts = int(dev_data['object_replication_last'] * 1000)  # ms
            stats.merge(
                ObjectReplicationStat(
                    v, ts, (
                        ("device", device),
                        ("label", k),
                    )
                ) for k, v in dev_data['replication_stats'].items()
                if k != 'failure_nodes'
            )
        if not stats:
            self.ever_reported.set()
        return stats


if __name__ == "__main__":
    SwiftObjectReplicationTracker.main()
