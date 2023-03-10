#!/usr/bin/env python
import random
import sys


def simulate() -> int:
    cnt = 0
    accum = 0.0
    while accum < 1:
        accum += random.uniform(0, 1)
        cnt += 1
    return cnt


for line in sys.stdin.readlines():
    try:
        num_sim, sim_seed = line.split('\t')
        num_sim = int(num_sim)
        sim_seed = float(sim_seed)
        random.seed(sim_seed)
        for _ in range(num_sim):
            ret = simulate()
            print("%d\t%d" % (ret, 1))
    except ValueError:
        exit(0)
