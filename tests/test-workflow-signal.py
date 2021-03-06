#!/usr/bin/env python3

import sys
import time
import signal

from common_workflow import run_workflow, show_results


if __name__ == '__main__':
    nruns = 10
    ncodes = 2
    max_procs = 4
    max_nodes = None
    processes_per_node = None
    timeout = 10
    kill_on_partial_failure = False
    if len(sys.argv) > 1:
        nruns = int(sys.argv[1])
    if len(sys.argv) > 2:
        ncodes = int(sys.argv[2])
    if len(sys.argv) > 3:
        max_procs = int(sys.argv[3])
    if len(sys.argv) > 4:
        timeout = int(sys.argv[4])
    if len(sys.argv) > 5:
        max_nodes = max_procs
        max_procs = None
        processes_per_node = int(sys.argv[5])
    if len(sys.argv) > 6:
        kill_on_partial_failure = True

    p = run_workflow(nruns, ncodes, max_procs, max_nodes, processes_per_node,
                     timeout, kill_on_partial_failure)
    time.sleep(2)
    p.send_signal(signal.SIGTERM)
    #p.send_signal(signal.SIGINT)
    p.wait()
    show_results()
