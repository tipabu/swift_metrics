MEMCACHE_PORT = 11211
RSYNC_PORT = 873

def is_swift_port(port):
    return port == 8080 or 6200 <= port <= 6300

def categorize_destination_port(port):
    if port == 6200 or 6203 <= port <= 6300:
        return 'object'
    return {
        6201: 'container',
        6202: 'account',
        8080: 'proxy',
        11211: 'memcache',
        873: 'rsync',
    }.get(port) or f'other ({port!r})'

def parse_netloc(netloc):
    if not netloc:
        return None
    host, port = netloc.partition(':')[::2]
    return (host, int(port))

def unpack(d, lvl=1):
    assert lvl >= 1
    if lvl == 1:
        for key, val in d.items():
            yield key, val
        return
    for key, val in d.items():
        for items in unpack(val, lvl-1):
            yield (key, ) + items
