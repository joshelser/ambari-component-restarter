#!/usr/bin/env python

from ambari import start, stop, getHostsForComponent

server='https://172.27.22.201:8443'
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
