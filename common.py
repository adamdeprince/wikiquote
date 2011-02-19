#!/usr/bin/env python
import redis
import pycassa

def open_redis_connection():
    return redis.Redis(host='localhost', port=6379, db=0)

def open_cassandra_connections():
    pool = pycassa.connect('Articles')
    raw = pycassa.ColumnFamily(pool, 'RawData')
    polished = pycassa.ColumnFamily(pool, 'PolishedData')
    index = pycassa.ColumnFamily(pool, 'index')
    return pool, raw, polished, index

class Que:
    def __init__(self):
        self.cxn = open_redis_connection()
    
    def push(self, item):
        self.cxn.rpush('pending', item)
    
    def pop(self):
        return self.cxn.lpop('pending')

    def unpop(self, item):
        self.cxn.lpush('pending', item)


class BatchQue(Que):
    def __init__(self):
        Que.__init__(self)
        self.cxn = self.cxn.pipeline()

    def save(self):
        self.cxn.execute()



