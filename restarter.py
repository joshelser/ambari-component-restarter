#!/usr/bin/env python

import sys
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

for master in masters:
    stop(server, master, MASTER)
    start(server, master, MASTER)

for rs in regionservers:
    stop(server, rs, RS)
    start(server, rs, RS)
