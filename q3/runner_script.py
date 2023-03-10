#!/usr/bin/env python
import argparse
import subprocess
import sys


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
    def pre_process(input_file: str, output_file: str):
        pass

    # pre_process(args['local_in_address'], args['local_in_address'] + ".tmp")
    # args['local_in_address'] += ".tmp"
    # End of pre-process

    # Put input on hdfs
    if subprocess.call(["hdfs", "dfs", "-put", args['local_in_address'], args['hdfs_in_address'] + "in"]):
        print('Error putting file on HDFS', file=sys.stderr)
        exit(1)

    # Call MapReduce
    mapper = args["hdfs_exec_address"] + "mapper.py"
    reducer = args["hdfs_exec_address"] + "reducer.py"
    if subprocess.call(
            ["hadoop", "jar", args['streaming_jar'], "-D mapred.reduce.tasks=1", "-files", mapper + "," + reducer,
             "-input",
             args['hdfs_in_address'] + "in", "-output", args['hdfs_out_address'], "-mapper", mapper,
             "-reducer", reducer]):
        print('Error running MapReduce application', file=sys.stderr)
        exit(1)


if __name__ == "__main__":
    main()
