#!/usr/bin/env python

import sys, time
from ambari import start, stop, getHostsForComponent

if len(sys.argv) < 2:
    print "Usage: restarter.py https://ambari_host:port"
    sys.exit(1)

server=sys.argv[1]
MASTER='HBASE_MASTER'
RS='HBASE_REGIONSERVER'

masters = getHostsForComponent(MASTER, server)
regionservers = getHostsForComponent(RS, server)

print "Masters=%s" % (masters)
print "RegionServers=%s" % (regionservers)

for rs in regionservers:
    stop(server, rs, RS)

for master in masters:
    stop(server, master, MASTER)

time.sleep(15)

for master in masters:
    start(server, master, MASTER)
for rs in regionservers:
    start(server, rs, RS)
