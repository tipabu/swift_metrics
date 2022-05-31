import collections
import glob
import os
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
        'partitions': collections.defaultdict(
            lambda: collections.defaultdict(lambda: {'primary': 0, 'handoff': 0})),
        'suffixes': collections.defaultdict(
            lambda: collections.defaultdict(lambda: {'primary': 0, 'handoff': 0})),
        #'hashdirs': collections.defaultdict(
        #    lambda: collections.defaultdict(lambda: {'primary': 0, 'handoff': 0})),
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
                ph = ('primary' if dev['id'] in [
                    d['id'] for d in ring.get_part_nodes(p)
                ] else 'handoff')
                stats['partitions'][dev['device']][policy.name][ph] += 1

                # TODO: this only works for object policies
                hashes = swift.obj.diskfile.read_hashes(part)
                for h in hashes:
                    if not swift.obj.diskfile.valid_suffix(h):
                        continue
                    stats['suffixes'][dev['device']][policy.name][ph] += 1
                    #stats['hashdirs'][dev['device']][policy.name][ph] += (part / h).stat().st_nlink - 2
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
        for disk in data[stat]
        for policy in data[stat][disk]
        for typ, value in data[stat][disk][policy].items()
    ]
