import subprocess


def flush():
    subprocess.run(['iptables', '-F', 'INPUT'], check=True)
    subprocess.run(['iptables', '-F', 'OUTPUT'], check=True)


def track(port):
    subprocess.run(['iptables', '-A', 'INPUT', '-p', 'tcp', '--dport', str(port)], check=True)
    subprocess.run(['iptables', '-A', 'OUTPUT', '-p', 'tcp', '--dport' if port == 873 else '--sport', str(port)], check=True)


if __name__ == '__main__':
    flush()
    track(8080)
    track(6201)
    track(6202)
    track(6203)
    track(6204)

    track(11211)  # memcache
    track(873)  # rsync
