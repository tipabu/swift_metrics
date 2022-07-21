import os
import pathlib
import subprocess
import typing

from . import Stat
from . import Tracker
from . import WriteOnceStatCollection


class DiskStat(Stat):
    name = "disk_space"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "Total/used/free bytes"


class DiskTracker(Tracker):
    def configure(self, conf: typing.Dict[str, str]) -> None:
        self.devices_path = pathlib.Path(conf.get('devices', '/srv/node'))

    def get_stats(self) -> WriteOnceStatCollection:
        mounts = tuple(self.devices_path / x
                       for x in os.listdir(self.devices_path))
        lines = [
            # ('device', 'total', 'used', 'free', '%used', 'mount')
            line.split()
            for line in subprocess.run(
                ("df", "-B", "1") + mounts,
                capture_output=True,
                check=True,
                encoding="utf-8",
            ).stdout.split("\n")
            if any(str(m) in line for m in mounts)
        ]
        now = Stat.now()
        return WriteOnceStatCollection(
            DiskStat(int(val), now, (
                ("device", line[0]),
                ("mount", line[-1]),
                ("type", typ),
            ))
            for line in lines
            for typ, val in zip(("total", "used", "free"), line[1:])
        )


if __name__ == "__main__":
    DiskTracker.main()
