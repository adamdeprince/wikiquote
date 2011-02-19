#!/usr/bin/env python
from pycassa import SystemManager 

sys = SystemManager('127.0.0.1:9160')
sys.create_keyspace('Articles', replication_factor=1)
for cf in ['RawData', 'PolishedData', 'index']:
    sys.create_column_family('Articles', cf)

