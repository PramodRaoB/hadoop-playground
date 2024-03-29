#!/usr/bin/env python
import argparse
import os
import subprocess
import sys
import random
import time

MAX_RAND = 1000000000
secret_id = 0


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
    random.seed(time.time())
    global secret_id
    secret_id = random.randint(0, MAX_RAND)

    def pre_process(input_file: str, output_file: str):

        def get_dims(input_file: str):
            try:
                with open(input_file, "r") as fin:
                    m, n = [int(x) for x in fin.readline().split()]
                    for i in range(m):
                        fin.readline()
                    n, p = [int(x) for x in fin.readline().split()]
            except IOError:
                print('Something went wrong while trying to open the input file on the local FS', file=sys.stderr)
                exit(1)
            return m, n, p

        def convert_input(fin, fout, matrix_id: int, dims: list):
            m, n = [int(x) for x in fin.readline().split()]
            for i in range(m):
                row = list(map(int, fin.readline().split()))
                for j in range(n):
                    fout.write('%d,%d,%d,%d,%d,%d\n' % (matrix_id, dims[matrix_id ^ 1], dims[2], i, j, row[j]))

        m, n, p = get_dims(input_file)
        try:
            with open(output_file, "w") as fout:
                try:
                    with open(input_file, "r") as fin:
                        for num_matrices in range(2):
                            convert_input(fin, fout, num_matrices, [m, p, n])
                except IOError:
                    print('Something went wrong while trying to open the input file on the local FS', file=sys.stderr)
                    exit(1)
        except IOError:
            print('Something went wrong while opening the HDFS input file directory', file=sys.stderr)
            exit(1)

    pre_process(args['local_in_address'], args['local_in_address'] + str(secret_id) + ".tmp")
    args['local_in_address'] += str(secret_id) + ".tmp"
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
             "-input", args['hdfs_in_address'] + "in", "-output", args['hdfs_out_address'], "-mapper", mapper,
             "-reducer", reducer]):
        print('Error running MapReduce application', file=sys.stderr)
        exit(1)

    # Cleaning up
    os.remove(args['local_in_address'])


if __name__ == "__main__":
    main()
