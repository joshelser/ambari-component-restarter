#!/usr/bin/env python

import sys, time
from ambari import start, stop, getHostsForComponent
import subprocess

def get_state(server):
    proc = subprocess.run(['/Users/jelser/check-single-zookeeper.sh', server], stdout=subprocess.PIPE)
    #print('stdout=%s' % (proc))
    # Throws an exception if return code is non-zero
    proc.check_returncode()
    mode = proc.stdout.decode('UTF-8').strip()
    #print('mode=%s' % (mode))
    if mode == 'follower' or mode == 'leader':
        return mode
    details = "Unhandled state: %s" % (mode)
    raise Exception('Failed to find ZK server in expected mode', details)

if len(sys.argv) < 2:
    print("Usage: zk-restarter.py https://ambari_host:port")
    sys.exit(1)

ambari_server=sys.argv[1]
service='ZOOKEEPER'
component='ZOOKEEPER_SERVER'

servers = getHostsForComponent(service, component, ambari_server)

print("ZooKeepers=%s", servers)

#print("Stopping ZooKeepers")
#for server in servers:
#    stop(ambari_server, server, component)
#
#time.sleep(15)
#
#print("Starting ZooKeepers")
#for server in servers:
#    start(ambari_server, server, component)

print("Checking ZK server state")
for server in servers:
    state = get_state(server)
    print("%s %s" % (server, state))
