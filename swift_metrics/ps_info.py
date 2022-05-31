import os
import subprocess

def calc_pcpu(times, etimes, prev_proc_info, pcpu):
    if prev_proc_info and (float(etimes) - prev_proc_info['etimes']) != 0:
        return ((int(times) - prev_proc_info['times']) /
                (float(etimes) - prev_proc_info['etimes']))
    if etimes and int(etimes):
        return float(times)/int(etimes)
    return float(pcpu)/100


def merge_proc_info(items, prev_proc_infos):
    pids = {x['pid'] for x in items}
    cmd = ['ps', '-o', 'pid,pcpu,rss,vsize,etimes,times,command']
    for pid in pids:
        cmd.extend(['-p', str(pid)])
    proc_infos = {}
    for line in subprocess.check_output(
            cmd, encoding='utf-8').strip().split('\n')[1:]:
        pid, pcpu, rss, vsize, etimes, times, cmdline = line.split(None, 6)
        pid = int(pid)
        prev_proc_info = prev_proc_infos and prev_proc_infos.get(pid)
        if 'python' in cmdline:
            cmdline = cmdline.partition(' ')[2]
        cmd, _, args = cmdline.partition(' ')
        cmd = os.path.basename(cmd)
        proc_infos[pid] = {
            'pcpu': calc_pcpu(times, etimes, prev_proc_info, pcpu),
            'rss': int(rss) * 1024,
            'vsize': int(vsize) * 1024,
            'etimes': int(etimes),
            'times': int(times),
            'command': cmd,
            'args': args,
        }
    for item in items:
        new_proc_info = proc_infos.get(item['pid'])
        if new_proc_info:
            item.update(new_proc_info)
        elif prev_proc_infos:
            item.update(prev_proc_infos.get(item['pid'], {}))
    return proc_infos
