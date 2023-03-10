#!/usr/bin/env python
import sys

for line in sys.stdin.readline():
    mat_id, rep, i, j, val = list(map(int, line.split(',')))
    for k in range(rep):
        if mat_id == 0:
            print(f'{i}:{k}\t{mat_id}:{j}:{val}')
        else:
            print(f'{k}:{j}\t{mat_id}:{i}:{val}')
