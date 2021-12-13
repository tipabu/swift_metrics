from .lsof_stats import lsof_stats
from .ps_info import merge_proc_info
from .iptables_counters import get_counters
from . import categorize_destination_port
from . import is_swift_port
from . import unpack

import itertools

def get_lsof_stats(prev_proc_infos=None):
    data = lsof_stats()
    proc_infos = merge_proc_info(data, prev_proc_infos)
    for item in data:
        server_sockets = [sock for sock in item['sockets'] if is_swift_port(sock['local'][1])]
        client_sockets = [sock for sock in item['sockets'] if not is_swift_port(sock['local'][1])]
        assert len({sock['local'][1] for sock in server_sockets}) in (0, 1)
        for sock in server_sockets:
            item['server_port'] = sock['local'][1]
            item.setdefault('server_stats', {}).setdefault(
                'ESTABLISHED', {'rx_buffer': 0, 'tx_buffer': 0, 'count': 0})
            item.setdefault('server_stats', {}).setdefault(
                sock['state'], {'rx_buffer': 0, 'tx_buffer': 0, 'count': 0})
            item['server_stats'][sock['state']]['rx_buffer'] += sock['QR']
            item['server_stats'][sock['state']]['tx_buffer'] += sock['QS']
            item['server_stats'][sock['state']]['count'] += 1

        for sock in client_sockets:
            dest = categorize_destination_port(sock['remote'][1])
            item.setdefault('client_stats', {}).setdefault(dest, {}).setdefault(
                sock['state'], {'rx_buffer': 0, 'tx_buffer': 0, 'count': 0})
            item['client_stats'][dest][sock['state']]['rx_buffer'] += sock['QR']
            item['client_stats'][dest][sock['state']]['tx_buffer'] += sock['QS']
            item['client_stats'][dest][sock['state']]['count'] += 1
        item.pop('sockets')

    stats = []
    client_stats = {}
    server_stats = {}
    server_counts = {}
    for item in data:
        labels = {'pid': item['pid'], 'command': item['command']}
        if 'server_port' in item:
            labels['port'] = item['server_port']
        for metric in ('pcpu', 'rss', 'vsize'):
            stats.append((metric, labels, item[metric]))
        for state, metrics in item.get('server_stats', {}).items():
            server_counts.setdefault(item['server_port'], 0)
            if state == 'LISTEN':
                server_counts[item['server_port']] += 1
            server_stats.setdefault(item['server_port'], {}).setdefault(state, {'rx_buffer': 0, 'tx_buffer': 0, 'count': 0})
            for name, metric in metrics.items():
                server_stats[item['server_port']][state][name] += metric
        for dest, state_to_metrics in item.get('client_stats', {}).items():
            client_stats.setdefault(item['command'], {}).setdefault(dest, {})
            for state, metrics in state_to_metrics.items():
                client_stats[item['command']][dest].setdefault(state, {'rx_buffer': 0, 'tx_buffer': 0, 'count': 0})
                for name, metric in metrics.items():
                    client_stats[item['command']][dest][state][name] += metric

    # Finished aggregating; start emitting
    for port, count in server_counts.items():
        stats.append(('server_count', {'port': port, 'type': categorize_destination_port(port)}, count))
    for port, state, metrics in unpack(server_stats, 2):
        labels = {'port': port, 'type': categorize_destination_port(port), 'state': state}
        stats.append(('server_connection_count', labels, metrics['count']))
        labels['for'] = 'rx'
        stats.append(('server_connection_buffer', labels, metrics['rx_buffer']))
        labels['for'] = 'tx'
        stats.append(('server_connection_buffer', labels, metrics['tx_buffer']))

    for client, typ, state, metrics in unpack(client_stats, 3):
        labels = {'command': client, 'type': typ, 'state': state}
        stats.append(('client_connection_count', labels, metrics['count']))
        labels['for'] = 'rx'
        stats.append(('client_connection_buffer', labels, metrics['rx_buffer']))
        labels['for'] = 'tx'
        stats.append(('client_connection_buffer', labels, metrics['tx_buffer']))
    return stats, proc_infos

def get_iptables_stats():
    return [
        (
            'server_net_' + name,
            {'port': port, 'type': categorize_destination_port(port),
             'for': {'INPUT': 'rx', 'OUTPUT': 'tx'}[chain]},
            count,
        )
        for port, chain, name, count in unpack(get_counters(), 3)
        if count
        # else: most likely, an unused object port
    ]

def label_str(labels):
    return '{' + ','.join(f'{label}="{val}"' for label, val in labels.items()) + '}'

def stats_doc(prev_proc_infos=None):
    lsof_stats, proc_infos = get_lsof_stats(prev_proc_infos)
    return '''
# HELP pcpu Current process CPU usage
# TYPE pcpu gauge
# HELP rss Current process RSS
# TYPE rss gauge
# HELP vsize Current process VSZ
# TYPE vsize gauge
# HELP server_processes Server process count
# TYPE server_processes gauge
# HELP server_net_packets Packets sent/received
# TYPE server_net_packets counter
# HELP server_net_bytes Bytes sent/received
# TYPE server_net_bytes counter
# HELP server_connection_count Total server sockets
# TYPE server_connection_count gauge
# HELP server_connection_buffer Total send/receive buffers for server traffic
# TYPE server_connection_buffer gauge
# HELP client_connection_count Total client sockets
# TYPE client_connection_count gauge
# HELP client_connection_buffer Total send/receive buffers for client traffic
# TYPE client_connection_buffer gauge
'''.lstrip() + ''.join(
    f'{name}{label_str(labels)} {value}\n'
    for name, labels, value in itertools.chain(lsod_stats, get_iptables_stats())), proc_infos
