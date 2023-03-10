#!/usr/bin/env python
import sys
import numpy as np

prev_key = None
in_vec = None

for line in sys.stdin.readline():
    k, v = line.split('\t')
    mat_id, ind, inner, val = list(map(int, v.split(":")))
    if in_vec is None:
        in_vec = np.zeros((2, inner), dtype=int)
    if prev_key is not None and k != prev_key:
        res = 0
        for x in range(inner):
            res += in_vec[0][x] * in_vec[1][x]
        print(f'{prev_key}\t{res}')
        res = 0
        in_vec.fill(0)

    prev_key = k
    in_vec[mat_id][ind] = val
