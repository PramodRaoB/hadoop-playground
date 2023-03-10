#!/usr/bin/env python
import sys

prev_key = None
in_vec = None


def output_val():
    global in_vec
    res = 0
    for x in range(inner):
        res += in_vec[0][x] * in_vec[1][x]
    print('%s\t%s' % (prev_key, res))
    in_vec = [[0] * inner for _ in range(2)]


for line in sys.stdin.readlines():
    try:
        k, v = line.split('\t')
        try:
            mat_id, ind, inner, val = list(map(int, v.split(",")))
            if in_vec is None:
                in_vec = [[0] * inner for _ in range(2)]
            if prev_key is not None and k != prev_key:
                output_val()

            prev_key = k
            in_vec[mat_id][ind] = val
        except ValueError:
            exit(0)
    except ValueError:
        exit(0)

if prev_key is not None:
    output_val()
