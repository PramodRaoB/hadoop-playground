#!/usr/bin/env python
import sys

prev_key = None
accum = 0

exp = 0
trials = 0


def end_exec():
    print("Final output")
    print("Trials: %d" % trials)
    print("Expected value: %f" % (exp / trials))
    exit(0)


def output_val():
    global accum
    print("%d\t%d" % (prev_key, accum))
    global exp
    global trials
    exp += prev_key * accum
    trials += accum
    accum = 0


for line in sys.stdin.readlines():
    try:
        k, v = list(map(int, line.split('\t')))
        if prev_key is not None and k != prev_key:
            output_val()

        prev_key = k
        accum += v
    except ValueError:
        end_exec()

if prev_key is not None:
    output_val()

end_exec()
