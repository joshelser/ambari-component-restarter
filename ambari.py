#!/usr/bin/env python

import logging, requests
import json as jsonlib
from requests.auth import HTTPBasicAuth
import time, urllib3

urllib3.disable_warnings()

username='admin'
password='admin'
cluster='hadoop'

def stop(ambari_url, host, component):
    u = "%s/api/v1/clusters/%s/hosts/%s/host_components/%s" % ( ambari_url, cluster, host, component )
    h = { 'X-Requested-By': 'ambari', 'Content-Type': 'text/plain' }
    d = '{"RequestInfo":{"context":"Stop %s on %s"}, "HostRoles": { "state": "INSTALLED" } }' %(component,host)
    a = HTTPBasicAuth(username, password)
    #print("Initiating stop of %s at host %s, request url is: %s" % (component, host, u))
    print("Initiating stop of %s at host %s" % (component, host))
    r = requests.put( u, data=d, headers=h, auth=a, verify=False )
    assert ( r.status_code == 200 or r.status_code == 202 ), "Failed to stop component %s on host %s, status=%d" % ( component, host, r.status_code )
    if r.status_code == 202:
        json_data = r.json()
        request_id = json_data['Requests']['id']
        status = wait_until_request_complete(request_id, ambari_url)
        assert status == 'COMPLETED', "Failed to stop component %s on host %s, status=%s" % (component, host, status)
    print("Completed stop of %s at host %s" % (component, host))

def start(ambari_url, host, component):
    u = "%s/api/v1/clusters/%s/hosts/%s/host_components/%s" % ( ambari_url, cluster, host, component )
    h = { 'X-Requested-By': 'ambari', 'Content-Type': 'text/plain' }
    d = '{"RequestInfo":{"context":"Stop %s on %s"}, "HostRoles": { "state": "STARTED" } }' %(component,host)
    a = HTTPBasicAuth(username, password)
    #print("Initiating start of %s at host %s, request url is: %s" % (component, host, u))
    print("Initiating start of %s at host %s" % (component, host))
    r = requests.put( u, data=d, headers=h, auth=a, verify=False )
    assert ( r.status_code == 200 or r.status_code == 202 ), "Failed to start component %s on host %s, status=%d" % ( component, host, r.status_code )
    if r.status_code == 202:
        json_data = r.json()
        request_id = json_data['Requests']['id']
        status = wait_until_request_complete(request_id, ambari_url)
        assert status == 'COMPLETED', "Failed to start component %s on host %s, status=%s" % (component, host, status)
    print("Completed start of %s at host %s" % (component, host))

def wait_until_request_complete(request_id, ambari_url, timeout=300, interval=10):
    starttime = time.time()
    status = 'PENDING'
    while (time.time() - starttime) < timeout:
      status = get_request_current_state(request_id, ambari_url)
      if status in ('COMPLETED', 'FAILED'):
        break
      time.sleep(interval)
    return status

def get_request_current_state(request_id, ambari_url):
    url = '/api/v1/clusters/' + cluster + '/requests/' + str(request_id)
    response = http_get_request(url, ambari_url)
    json_data = jsonlib.loads(response._content)
    return json_data['Requests']['request_status']

def http_get_request(uri, ambari_url):
    url = ambari_url + uri
    basic_auth = HTTPBasicAuth(username, password)
    return requests.get(url=url, auth=basic_auth, verify=False)

def getHostsForComponent(service, component, ambari_url):
    path = "/api/v1/clusters/%s/services/%s/components/%s" % (cluster, service, component)
    #print("Initiating fetch of hosts for %s, request url is: %s%s" % (component, ambari_url, path))
    response = http_get_request(path,ambari_url)
    if response.status_code != 200:
      print("Failed to fetch components for %s, content=%s" % (component, response.body))
      return None
    json = response.json()
    items = json['host_components']
    hosts = []
    for item in items:
      #print("%s" % (item))
      if 'HostRoles' in item and 'host_name' in item["HostRoles"]:
        hosts.append(item["HostRoles"]["host_name"])
    #print("List of hosts for %s: %s" % (component, ",".join(hosts)))
    return hosts
