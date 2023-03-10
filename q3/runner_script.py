#!/usr/bin/env python
import argparse
import random
import subprocess
import sys

SIMULATION_PER_MAP = 1000000
MAX_MAP_TASKS = 10
MAX_RAND = 1e6
maps = 0


def main():
    # Parse inputs
    parser = argparse.ArgumentParser(description='Python runner script for a hadoop MapReduce application')
    parser.add_argument('streaming_jar', metavar='jar', type=str, help='location to the hadoop streaming jar')
    parser.add_argument('local_in_address', metavar='loc_in', type=str,
                        help='location to the input file in the local FS')
    parser.add_argument('hdfs_in_address', metavar='hdfs_in', type=str,
                        help='location to the input file to be put in HDFS')
    parser.add_argument('hdfs_out_address', metavar='hdfs_out', type=str,
                        help='location to the output directory on HDFS')
    parser.add_argument('hdfs_exec_address', metavar='hdfs_exec', type=str,
                        help='location to the directory on HDFS containing the mapper(s) and reducer(s)')
    args = vars(parser.parse_args())

    # Pre-process (if any)
    def pre_process(input_file: str):
        global maps
        maps = MAX_MAP_TASKS
        sims = 0
        try:
            with open(input_file, "r") as fin:
                sims = int(fin.readline())
        except IOError:
            print('Something went wrong while trying to open the input file on the local FS', file=sys.stderr)
            exit(1)
        for p in range(1, MAX_MAP_TASKS + 1):
            per_map = sims // p
            if per_map <= SIMULATION_PER_MAP:
                maps = p
                break
        for m in range(maps):
            curr = sims // maps + (m < (sims % maps))
            with open(input_file + str(m) + ".tmp", "w") as fout:
                fout.write("%d\t%f" % (curr, random.uniform(0, MAX_RAND)))

    pre_process(args['local_in_address'])
    # End of pre-process

    # Put input on hdfs
    for m in range(maps):
        if subprocess.call(
                ["hdfs", "dfs", "-put", args['local_in_address'] + str(m) + ".tmp", args['hdfs_in_address'] + str(m)]):
            print('Error putting file on HDFS', file=sys.stderr)
            exit(1)

    # Call MapReduce
    mapper = args["hdfs_exec_address"] + "mapper.py"
    reducer = args["hdfs_exec_address"] + "reducer.py"
    if subprocess.call(
            ["hadoop", "jar", args['streaming_jar'], "-D mapred.reduce.tasks=1", "-files", mapper + "," + reducer,
             "-input", args['hdfs_in_address'], "-output", args['hdfs_out_address'], "-mapper", mapper, "-combiner",
             reducer, "-reducer", reducer]):
        print('Error running MapReduce application', file=sys.stderr)
        exit(1)


if __name__ == "__main__":
    main()
