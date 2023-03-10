#!/usr/bin/env python
import sys

prev_key = None
accum = 0


def output_val():
    global accum
    print("%d\t%d" % (prev_key, accum))
    accum = 0


for line in sys.stdin.readlines():
    try:
        k, v = list(map(int, line.split('\t')))
        if prev_key is not None and k != prev_key:
            output_val()

        prev_key = k
        accum += v
    except ValueError:
        exit(0)

if prev_key is not None:
    output_val()
