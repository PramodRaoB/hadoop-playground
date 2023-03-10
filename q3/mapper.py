#!/usr/bin/env python
import sys

for line in sys.stdin.readlines():
    try:
        sim = int(line)
        print(sim)
    except ValueError:
        exit(0)
