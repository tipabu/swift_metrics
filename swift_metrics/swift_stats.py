import collections
import glob
import os
import pickle
import pathlib
import swift.obj.diskfile
import swift.common.utils
import swift.common.ring


rings = {os.path.basename(r).split('.')[0]: swift.common.ring.Ring(r)
         for r in glob.glob('/etc/swift/*.ring.gz')}

my_ips = set(swift.common.utils.whataremyips())
srv_node = pathlib.Path('/srv/node')
def replication_stats():
    stats = {
        'partitions': {
            'primary': collections.defaultdict(lambda: collections.defaultdict(int)),
            'handoff': collections.defaultdict(lambda: collections.defaultdict(int)),
        },
        'suffixes': {
            'primary': collections.defaultdict(lambda: collections.defaultdict(int)),
            'handoff': collections.defaultdict(lambda: collections.defaultdict(int)),
        },
        #'hashdirs': {
        #    'primary': collections.defaultdict(lambda: collections.defaultdict(int)),
        #    'handoff': collections.defaultdict(lambda: collections.defaultdict(int)),
        #},
        # TODO: would love to add in hashdirs and bytes, but it's going to
        # increase runtime and i/o load -- *way* better if we can get those
        # into hashes.pkl on a rehash
    }
    for disk in srv_node.iterdir():
        for policy in disk.iterdir():
            try:
                ring = rings[policy.name.replace('s', '')]
            except KeyError:
                # tmp or async_pending, most likely
                continue

            try:
                dev = next(dev for dev in ring.devs
                           if dev and dev['device'] == disk.name
                           and (my_ips & {dev['ip'], dev['replication_ip']}))
            except StopIteration:
                # dev not in ring; always handoff
                dev = None

            for part in policy.iterdir():
                try:
                    p = int(part.name)
                except ValueError:
                    continue
                hashes = swift.obj.diskfile.read_hashes(part)
                ph = ('primary' if dev['id'] in [
                    d['id'] for d in ring.get_part_nodes(p)
                ] else 'handoff')
                stats['partitions'][ph][dev['device']][policy.name] += 1
                for h in hashes:
                    if not swift.obj.diskfile.valid_suffix(h):
                        continue
                    stats['suffixes'][ph][dev['device']][policy.name] += 1
                    #stats['hashdirs'][ph][dev['device']][policy.name] += (part / h).stat().st_nlink - 2
    return stats

def get_replication_stats():
    data = replication_stats()
    return [
        (
            stat,
            {'device': disk, 'policy': policy, 'type': typ},
            value,
        )
        for stat in data
        for typ in data[stat]
        for disk in data[stat][typ]
        for policy, value in data[stat][typ][disk].items()
    ]
