import subprocess

def get_counters():
    out = subprocess.check_output([
        'iptables', '-L', '-n', '-v', '-x',
    ], encoding='utf-8', stderr=subprocess.DEVNULL)
    chains = [x.strip() for x in out.split('Chain') if x.strip()]
    by_port = {}
    for chain in chains:
        name = chain.split(' ', 1)[0]
        for line in chain.split('\n')[2:]:
            pkts, byts, _, _, _, _, _, _, _, port = line.split()
            port = int(port.rsplit(':', 1)[-1])
            by_port.setdefault(port, {})
            by_port[port][name] = {'packets': int(pkts), 'bytes': int(byts)}
    return by_port
