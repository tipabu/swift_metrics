import subprocess
import typing

from . import categorize_destination_port
from . import parse_netloc
from . import Stat
from . import Tracker
from . import WriteOnceStatCollection


class IPTablesPacketsStat(Stat):
    name = "server_net_packets"
    type: typing.ClassVar[typing.Literal["counter"]] = "counter"
    help = "Packets sent/received"


class IPTablesBytesStat(Stat):
    name = "server_net_bytes"
    type: typing.ClassVar[typing.Literal["counter"]] = "counter"
    help = "Bytes sent/received"


class IPTablesTracker(Tracker):
    def get_stats(self):
        out = subprocess.run(
            ['iptables', '-L', '-n', '-v', '-x'],
            capture_output=True,
            check=True,
            encoding='utf-8',
        ).stdout
        now = Stat.now()
        ret = WriteOnceStatCollection()
        for chain in [x.strip() for x in out.split('Chain') if x.strip()]:
            stat_for = {'INPUT': 'rx', 'OUTPUT': 'tx'}.get(
                chain.split(' ', 1)[0])
            if not stat_for:
                continue
            for line in chain.split('\n')[2:]:
                pkts, byts, _, _, _, _, _, _, _, netloc = line.split()
                _, port = parse_netloc(netloc)
                ret.update(
                    IPTablesPacketsStat(
                        int(pkts), now, (
                            ("port", port),
                            ("type", categorize_destination_port(port)),
                            ("for", stat_for),
                        ),
                    ),
                    IPTablesBytesStat(
                        int(byts), now, (
                            ("port", port),
                            ("type", categorize_destination_port(port)),
                            ("for", stat_for),
                        ),
                    ),
                )
        return ret


if __name__ == "__main__":
    IPTablesTracker.main()
