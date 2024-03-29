import itertools
import os
import subprocess
import sys
import typing

from . import categorize_destination_port
from . import is_swift_port
from . import MEMCACHE_PORT
from . import parse_netloc
from . import RSYNC_PORT
from . import Stat
from . import Tracker
from . import WriteOnceStatCollection


class PCPUStat(Stat):
    name = "pcpu"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "Process CPU usage"


class RSSStat(Stat):
    name = "rss"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "Process RSS"


class VSizeStat(Stat):
    name = "vsize"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "Process VSZ"


class ReadBytesStat(Stat):
    name = "read_bytes"
    type: typing.ClassVar[typing.Literal["counter"]] = "counter"
    help = "Bytes read over the lifetime of the process"


class WriteBytesStat(Stat):
    name = "write_bytes"
    type: typing.ClassVar[typing.Literal["counter"]] = "counter"
    help = "Bytes written over the lifetime of the process"


class ServerConnectionCountStat(Stat):
    name = "server_connection_count"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "Total server sockets"


class ServerConnectionBufferStat(Stat):
    name = "server_connection_buffer"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "Total send/receive buffers for server traffic"


class ClientConnectionCountStat(Stat):
    name = "client_connection_count"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "Total client sockets"


class ClientConnectionBufferStat(Stat):
    name = "client_connection_buffer"
    type: typing.ClassVar[typing.Literal["gauge"]] = "gauge"
    help = "Total send/receive buffers for client traffic"


class ProcessTracker(Tracker):
    def configure(self, conf: typing.Dict[str, str]) -> None:
        self.swift_user = conf.get('user', 'swift')
        self.process_tree: typing.Dict[int, typing.Dict[str, typing.Any]] = {}

        self.clk_tck = int(subprocess.run(
            ['getconf', 'CLK_TCK'],
            check=True,
            capture_output=True,
        ).stdout)

    def get_stats(self) -> WriteOnceStatCollection:
        cmd = [
            'ps', '--no-headers',
            '-o', 'sid,ppid,pid,pcpu,rss,vsize,etimes,times,command',
            '-u', self.swift_user,
        ]
        sids = set()
        new_process_tree: typing.Dict[int, typing.Dict[str, typing.Any]] = {}
        for line in subprocess.run(
            cmd,
            # There may be no swift processes running, causing ps to exit 1
            check=False,
            capture_output=True,
            encoding='utf8',
        ).stdout.strip().split('\n'):
            if not line:
                continue
            sid, ppid, pid, pcpu, rss, vsize, etimes, times, cmdline = \
                (t(v) for v, t in zip(line.split(None, 8), (
                    int, int, int, float, int, int, int, int, str)))
            if cmdline.startswith('['):
                # defunct process (at least, usually?)
                continue

            cmdname, _, args = cmdline.partition(' ')
            if 'python' in cmdname:
                cmdname, _, args = args.partition(' ')
            cmdname = os.path.basename(cmdname)

            try:
                proc_info = open(f'/proc/{pid}/stat').read().split()
                utime = float(proc_info[13]) / self.clk_tck
                ktime = float(proc_info[14]) / self.clk_tck
                starttime = float(proc_info[21]) / self.clk_tck
                # get this fresh for each pid in case there's some delay
                uptime = float(open(f'/proc/uptime').read().split()[0])
            except OSError:
                pass
            else:
                times = utime + ktime
                etimes = uptime - starttime

            if pid in self.process_tree \
                    and self.process_tree[pid]['etimes'] != etimes:
                pcpu = (times - self.process_tree[pid]['times']) / (
                    etimes - self.process_tree[pid]['etimes'])
            else:
                pcpu /= 100

            sids.add(sid)

            pid_dict = new_process_tree.setdefault(pid, {})
            pid_dict.update({
                'ppid': ppid,
                'pid': pid,
                'pcpu': pcpu,
                'rss': rss,
                'vsize': vsize,
                'etimes': etimes,
                'times': times,
                'cmd': cmdname,
                'args': args,
            })
            pid_dict.update(get_disk_io_stats(pid))
            if pid != sid:
                pid_dict.update(get_connection_stats(pid))
            new_process_tree.setdefault(ppid, {}).setdefault(
                'children', {})[pid] = pid_dict

        now = Stat.now()
        old_process_tree, self.process_tree = \
            self.process_tree, new_process_tree
        if not old_process_tree:
            # first run; trust nothing
            return WriteOnceStatCollection()

        stats = WriteOnceStatCollection(itertools.chain.from_iterable(
            make_stats(pid_dict, now)
            for pid, pid_dict in self.process_tree.items()
            if 'pcpu' in pid_dict
        ))
        # zero out stale info
        for pid, pid_dict in old_process_tree.items():
            if pid in self.process_tree or 'pcpu' not in pid_dict:
                continue
            stats.merge(stat.zero() for stat in make_stats(pid_dict, now))
        return stats


def get_disk_io_stats(pid: int) -> typing.Dict[str, int]:
    io_stats = {
        'read_bytes': 0,
        'write_bytes': 0,
    }
    try:
        with open(f'/proc/{pid}/io') as fp:
            for line in fp:
                if line.startswith('read_bytes: '):
                    io_stats['read_bytes'] = int(line.split()[1])
                elif line.startswith('write_bytes: '):
                    io_stats['write_bytes'] = int(line.split()[1])
    except IOError:
        pass
    return io_stats


def get_connection_stats(pid: int) -> typing.Dict[str, typing.Any]:
    result: typing.Dict[str, typing.Any] = {}
    info = subprocess.run([
        'lsof', '-a', '-n', '-P',
        '-p', str(pid),
        '-i',
        '-T', 'sq',
        '-F', 'fnT0',
    ], encoding='utf8', capture_output=True).stdout.strip('\n')
    if not info:
        return result

    sockets = []
    local_addrs = set()
    for line in info.split('\n'):
        if line.startswith('p'):
            assert line == f'p{pid}\x00', f'{line!r} != "p{pid}"'
            continue
        conn: typing.Dict[str, typing.Any] = {}
        for part in line.strip('\x00').split('\x00'):
            if part.startswith('f'):
                conn['fd'] = int(part[1:])
            elif part.startswith('n'):
                local_addr, _, remote_addr = part[1:].partition('->')
                conn['local'] = parse_netloc(local_addr)
                local_addrs.add(conn['local'])
                if remote_addr:
                    conn['remote'] = parse_netloc(remote_addr)
            elif part.startswith('TST='):
                conn['state'] = part[4:]
            elif part.startswith('TQR='):
                conn['recv_buffer'] = int(part[4:])
            elif part.startswith('TQS='):
                conn['send_buffer'] = int(part[4:])
            else:
                raise ValueError(f'cannot parse lsof output: {part!r}')
        sockets.append(conn)

    for conn in sockets:
        if conn.get('remote') in local_addrs:
            # sometimes eventlet talks to itself
            continue
        port = conn['local'][1]
        if is_swift_port(port) or port == RSYNC_PORT:
            port_dict = result.setdefault('server', {}).setdefault(port, {})
        elif 'remote' not in conn:
            print(f'Found unexpected connection {conn}', file=sys.stderr)
            continue
        else:
            port = conn['remote'][1]
            if not is_swift_port(port) and port not in (MEMCACHE_PORT, RSYNC_PORT):
                print(f'Found unexpected connection {conn}', file=sys.stderr)
                continue
            port_dict = result.setdefault('client', {}).setdefault(port, {})

        if conn['state'] not in port_dict:
            for state in ('ESTABLISHED', 'CLOSE_WAIT'):
                port_dict.setdefault(state, {
                    'connections': 0,
                    'recv_buffer': 0,
                    'send_buffer': 0,
                })
            port_dict[conn['state']] = {
                'connections': 0,
                'recv_buffer': 0,
                'send_buffer': 0,
            }
        port_dict[conn['state']]['connections'] += 1
        port_dict[conn['state']]['recv_buffer'] += conn['recv_buffer']
        port_dict[conn['state']]['send_buffer'] += conn['send_buffer']
    return result


def make_stats(
    pid_dict: typing.Dict[str, typing.Any],
    now: int
) -> WriteOnceStatCollection:
    stats = WriteOnceStatCollection((
        PCPUStat(pid_dict['pcpu'], now, (
            ("pid", pid_dict['pid']),
            ("command", pid_dict['cmd']),
        )),
        RSSStat(pid_dict['rss'], now, (
            ("pid", pid_dict['pid']),
            ("command", pid_dict['cmd']),
        )),
        VSizeStat(pid_dict['vsize'], now, (
            ("pid", pid_dict['pid']),
            ("command", pid_dict['cmd']),
        )),
        ReadBytesStat(pid_dict['read_bytes'], now, (
            ("pid", pid_dict['pid']),
            ("command", pid_dict['cmd']),
        )),
        WriteBytesStat(pid_dict['write_bytes'], now, (
            ("pid", pid_dict['pid']),
            ("command", pid_dict['cmd']),
        )),
    ))
    for port, port_dict in pid_dict.get('server', {}).items():
        for state, state_dict in port_dict.items():
            stats.update(
                ServerConnectionCountStat(state_dict['connections'], now, (
                    ("pid", pid_dict['pid']),
                    ("command", pid_dict['cmd']),
                    ('port', str(port)),
                    ('type', categorize_destination_port(port)),
                    ('state', state),
                )),
                ServerConnectionBufferStat(state_dict['recv_buffer'], now, (
                    ("pid", pid_dict['pid']),
                    ("command", pid_dict['cmd']),
                    ('port', str(port)),
                    ('type', categorize_destination_port(port)),
                    ('state', state),
                    ('for', 'rx'),
                )),
                ServerConnectionBufferStat(state_dict['send_buffer'], now, (
                    ("pid", pid_dict['pid']),
                    ("command", pid_dict['cmd']),
                    ('port', str(port)),
                    ('type', categorize_destination_port(port)),
                    ('state', state),
                    ('for', 'tx'),
                )),
            )
    for port, port_dict in pid_dict.get('client', {}).items():
        for state, state_dict in port_dict.items():
            stats.update(
                ClientConnectionCountStat(state_dict['connections'], now, (
                    ("pid", pid_dict['pid']),
                    ("command", pid_dict['cmd']),
                    ('port', str(port)),
                    ('type', categorize_destination_port(port)),
                    ('state', state),
                )),
                ClientConnectionBufferStat(state_dict['recv_buffer'], now, (
                    ("pid", pid_dict['pid']),
                    ("command", pid_dict['cmd']),
                    ('port', str(port)),
                    ('type', categorize_destination_port(port)),
                    ('state', state),
                    ('for', 'rx'),
                )),
                ClientConnectionBufferStat(state_dict['send_buffer'], now, (
                    ("pid", pid_dict['pid']),
                    ("command", pid_dict['cmd']),
                    ('port', str(port)),
                    ('type', categorize_destination_port(port)),
                    ('state', state),
                    ('for', 'tx'),
                )),
            )
    return stats


if __name__ == "__main__":
    ProcessTracker.main()
