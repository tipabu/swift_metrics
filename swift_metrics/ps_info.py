import os
import subprocess

def merge_proc_info(items):
    pids = {x['pid'] for x in items}
    cmd = ['ps', '-o', 'pid,pcpu,rss,vsize,etimes,command']
    for pid in pids:
        cmd.extend(['-p', str(pid)])
    proc_infos = {}
    for line in subprocess.check_output(
            cmd, encoding='utf-8').strip().split('\n')[1:]:
        pid, pcpu, rss, vsize, etimes, cmdline = line.split(None, 5)
        pid = int(pid)
        if 'python' in cmdline:
            cmdline = cmdline.partition(' ')[2]
        cmd, _, args = cmdline.partition(' ')
        cmd = os.path.basename(cmd)
        proc_infos[pid] = {
            'pcpu': float(pcpu),
            'rss': int(rss) * 1024,
            'vsize': int(vsize) * 1024,
            'etimes': int(etimes),
            'command': cmd,
            'args': args,
        }
    for item in items:
        item.update(proc_infos.get(item['pid'], {}))

