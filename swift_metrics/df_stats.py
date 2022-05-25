import os
import pathlib
import subprocess

srv_node = pathlib.Path('/srv/node')
df_headers = ('device', 'total', 'used', 'free', '%used', 'mount')

def df_stats():
    mounts = [srv_node / x for x in os.listdir(srv_node)]
    lines = [
        line.split()
        for line in subprocess.run(
            ['df', '-B', '1'] + mounts,
            capture_output=True,
            check=True,
            encoding='utf-8',
        ).stdout.split('\n')
        if line
    ]
    return [
        {k: v for k, v in zip(df_headers, line)}
        for line in lines[1:]
    ]
