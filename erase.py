#!/usr/bin/env python
 
import common 

common.open_redis_connection().delete('pending')
pool, _raw, _polished, _index = common.open_cassandra_connections()
for cf in ('RawData', 'PolishedData', 'Index'):
    print "Truncating ", cf 
    pool.truncate(cf)
