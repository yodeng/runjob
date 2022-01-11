#!/usr/bin/env python
# coding:utf-8
'''
Summary runtime infomation

Usage: python runtime.py <pairfile> <logdir> <outfile>
'''


import os
import re
import sys

from datetime import datetime


def listdir(path):
    for a, b, c in os.walk(path):
        for i in c:
            yield os.path.join(a, i)


def getSample(pairfile):
    samples = []
    with open(pairfile) as fi:
        for line in fi:
            if not line.strip() or line.startswith("#"):
                continue
            line = line.strip().split()
            samples.append(line[0])
    return samples


def sumtime(pairfile, logdir, timefile):
    logfiles = listdir(logdir)
    samples = getSample(pairfile)
    sampletimes = {}
    timeinfo = {}
    for f in logfiles:
        filename = os.path.basename(f)
        for s in samples:
            if s in filename:
                break
        else:
            continue
        with open(f) as fi:
            ctx = fi.read()
        intimes = re.findall(
            "\n\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]", ctx)
        dtime = sorted([datetime.strptime(i, "%Y-%m-%d %X") for i in intimes])
        assert len(dtime) >= 3
        sampletimes.setdefault(s, {}).setdefault("start", []).append(dtime[0])
        sampletimes.setdefault(s, {}).setdefault("end", []).append(dtime[-1])

    for sn, t in list(sampletimes.items()):
        s = sorted(t["start"])[0]
        e = sorted(t["end"])[-1]
        timeinfo[sn] = (s, e)

    with open(timefile, "w") as fo:
        fo.write("Sample\tstart_time\tend_time\ttime_used(hour)\n")
        for sn, t in list(timeinfo.items()):
            s = t[1] - t[0]
            fo.write(sn + "\t" + t[0].strftime("%Y/%m/%d,%X") + "\t" + t[1].strftime(
                "%Y/%m/%d,%X") + "\t" + str(round(s.total_seconds()/3600, 2)) + "\n")


if __name__ == "__main__":
    if len(sys.argv) != 4 or "-h" in sys.argv or "--help" in sys.argv:
        print(__doc__)
        sys.exit(1)
    pairfile, logdir, timefile = sys.argv[1:][:3]
    sumtime(pairfile, logdir, timefile)
