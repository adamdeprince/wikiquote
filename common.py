#!/usr/bin/env python

def open_redis_connection():
    return redis.Redis(host='localhost', port=6379, db=0)

def open_cassandra_connections():
    pool = pycassa.connect('Articles')
    raw = pycassa.ColumnFamily(pool, 'RawData')
    polished = pycassa.ColumnFamily(pool, 'PolishedData')
    index = pycassa.ColumnFamily(pool, 'Index')
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

