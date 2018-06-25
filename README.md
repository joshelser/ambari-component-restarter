## Setup

```
$ virtualenv e
$ source e/bin/activate
$ pip install -r requirements.txt
```

## Restart HBase services

```
$ ./restarter.py
```

Or, modify the program to restart services per your desire.

## Validate system tables are reachable

```
$ kinit ...
$ hbase shell query-system-tables.txt
```

Success is if you do not see "region is not online at ..."
