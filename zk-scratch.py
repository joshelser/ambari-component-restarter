#!/usr/bin/env python

import random, subprocess, sys, time
from ambari import start, stop, getHostsForComponent

def get_state(server):
    # Try a few times to check on the state of a single ZK server
    details = "Failed to make any ZK server check on %s" % (server)
    for x in range(5):
        # TODO This script is a wrapper around `echo srvr | nc "$1" 2181 | fgrep Mode | awk '{print $2}'
        proc = subprocess.run(['/Users/jelser/check-single-zookeeper.sh', server], stdout=subprocess.PIPE)
        #print('stdout=%s' % (proc))
        # Throws an exception if return code is non-zero
        proc.check_returncode()
        mode = proc.stdout.decode('UTF-8').strip()
        #print('mode=%s' % (mode))
        if mode == 'follower' or mode == 'leader':
            return mode
        details = "Unhandled state: %s" % (mode)
        time.sleep(1)
    raise Exception('Failed to find ZK server in expected mode', details)

def timed_op(action, servers):
    start_time = time.perf_counter()
    action(servers)
    end_time = time.perf_counter()
    print('Operation Elapsed time', (end_time-start_time))

def make_check_all_states():
    def check_all_states(servers):
        for server in servers:
            # Will throw an exception if bad
            state = get_state(server)
            print("%s => %s" % (server, state))
    return check_all_states

def make_restart_servers():
    def restart_servers(server):
        for server in servers:
            start_time = time.perf_counter()

            print("\nProcessing", server)
            stop(ambari_server, server, component)
            start(ambari_server, server, component)
            # Will throw an exception if bad
            state = get_state(server)
            print("%s => %s" % (server, state))

            end_time = time.perf_counter()
            print('Server Restart Elapsed time', (end_time-start_time))
    return restart_servers

def make_async_restart_servers():
    def restart_servers(server):
        for server in servers:
            start_time = time.perf_counter()

            print("\nProcessing", server)
            stop(ambari_server, server, component)
            start(ambari_server, server, component, async=True)
            # Can't check state of ZK server if we don't wait for the service to start

            end_time = time.perf_counter()
            print('Server Restart Elapsed time', (end_time-start_time))
    return restart_servers


if len(sys.argv) < 2:
    print("Usage: zk-restarter.py https://ambari_host:port")
    sys.exit(1)

ambari_server=sys.argv[1]
service='ZOOKEEPER'
component='ZOOKEEPER_SERVER'

servers = getHostsForComponent(service, component, ambari_server)
random.shuffle(servers)

print("ZooKeepers", servers)

print("\nInitial ZK server states")
timed_op(make_check_all_states(), servers)

print("\nRestarting ZK servers")
timed_op(make_async_restart_servers(), servers)

print("\nFinal ZK server states")
timed_op(make_check_all_states(), servers)
