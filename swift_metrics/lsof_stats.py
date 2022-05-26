import subprocess
import sys

from . import MEMCACHE_PORT, RSYNC_PORT
from . import is_swift_port
from . import parse_netloc

def lsof(user):
    return subprocess.run([
        'lsof', '-a', '-P',
        '-u', user,
        '-i',
        '-T', 'sq',
        '-F', 'pRfnT0',
    ], encoding='utf-8', capture_output=True).stdout

def split_by_process(out):
    return [
        ('p' + line)
        for line in ('\n' + out + 'p').split('\np')
        if line
    ]

def parse_process(s):
    s = s.split('\n')
    info = parse_fields(s[0])
    info['sockets'] = [parse_fields(_) for _ in s[1:]]
    return info

def parse_fields(s):
    r = {}
    for x in s.strip('\x00').split('\x00'):
        if x[:1] == 'n':
            local, remote = x[1:].partition('->')[::2]
            r['local'] = parse_netloc(local)
            r['remote'] = parse_netloc(remote)
            continue
        elif x[:1] == 'T':
            k, v = x[1:].partition('=')[::2]
        else:
            k, v = x[:1], x[1:]
        k = {
            'p': 'pid',
            'R': 'ppid',
            'f': 'fd',
            'n': 'name',
            'ST': 'state',
            'QR': 'QR',
            'QS': 'QS',
        }[k]
        r[k] = int(v) if v.isdigit() else v
    return r

def prune_sockets(proc_info):
    local_ports = {sock['local'] for sock in proc_info['sockets']}
    # sometimes eventlet likes to talk to itself
    proc_info['sockets'] = [
        sock for sock in proc_info['sockets']
        if sock['remote'] not in local_ports]
    for r in proc_info['sockets']:
        assert (
            is_swift_port(r['local'][1]) or
            is_swift_port(r['remote'][1]) or
            r['remote'][1] in (RSYNC_PORT, MEMCACHE_PORT) or
            r['local'][1] == RSYNC_PORT
        ), f'Found unexpected socket {r!r}'

def lsof_stats(user='swift'):
    data = [parse_process(x) for x in split_by_process(lsof('swift'))]
    managers = {item['ppid'] for item in data}
    result = []
    for item in data:
        if item['pid'] in managers:
            continue
        prune_sockets(item)
        if not item['sockets']:
            continue
        result.append(item)
    return result

