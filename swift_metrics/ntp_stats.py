import subprocess
import typing

from . import Stat
from . import Tracker
from . import WriteOnceStatCollection


class DistanceStat(Stat):
    name = "ntp_root_distance"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "timesyncd root distance in microseconds"


class OffsetStat(Stat):
    name = "ntp_offset"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "timesyncd offset in microseconds"


class DelayStat(Stat):
    name = "ntp_delay"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "timesyncd delay in microseconds"


class JitterStat(Stat):
    name = "ntp_jitter"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "timesyncd jitter in microseconds"


def parse_time(time_str):
    # Root distance line can include "(max: X)"
    time_str = time_str.partition('(')[0].strip()
    if 'min' in time_str:
        m, s = time_str.partition('min')[::2]
        m = int(m) * 60
        s = float(s[:-1])
        return int((m + s) * 1e6)
    if time_str.endswith('us'):
        return int(time_str[:-2])
    if time_str.endswith('ms'):
        return int(float(time_str[:-2]) * 1e3)
    if time_str.endswith('s'):
        return int(float(time_str[:-1]) * 1e6)
    if time_str in ('0', '-0'):
        return 0
    raise ValueError(f'Could not parse time: {time_str!r}')


class TimeSyncTracker(Tracker):
    def get_stats(self) -> WriteOnceStatCollection:
        info = {}
        for line in subprocess.run(
            ['timedatectl', 'timesync-status'],
            capture_output=True,
            check=True,
            encoding='utf-8'
        ).stdout.split('\n'):
            if line:
                key, value = line.split(': ', 1)
                info[key.strip()] = value

        now = Stat.now()
        return WriteOnceStatCollection(
            cls(parse_time(info[key]), now)
            for key, cls in (
                ('Root distance', DistanceStat),
                ('Offset', OffsetStat),
                ('Delay', DelayStat),
                ('Jitter', JitterStat),
            ) if key in info
        )


if __name__ == "__main__":
    TimeSyncTracker.main()
