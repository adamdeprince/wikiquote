#!/usr/bin/env python
 
import common 
import itertools 
import hashlib
import sys

HASH_SIZE = 96 / 4 # 96 bits of hash in nibbles 

BLOCK_SIZE = 5000



class Reader:
    def __init__(self):
        self.que = common.BatchQue()
        self.pool, self.raw, _, _ = common.open_cassandra_connections()
        self.raw = self.raw.batch()
        self.counter = 1 

    def occationally_save(self, interval=BLOCK_SIZE):
        self.counter += 1 
        if self.counter > interval:
            self.save()
            self.counter = 0 

    def save(self):
        self.raw.send()
        self.que.save()

    @staticmethod
    def digest(s):
        return hashlib.md5(s).hexdigest()[:HASH_SIZE]

    @staticmethod 
    def lines_from_page(input):
        while True:
            line = input.next()
            yield line 
            if '</page>' in line:
                return 
    @staticmethod
    def create_page_key_and_body(input):
        data = '\n'.join(Reader.lines_from_page(input))
        return Reader.digest(data), data 

    def upload_page(self, key, body ):
        self.que.push(key)
        self.raw.insert(key, dict(body=body))
        self.occationally_save()

    def split_into_pages(self, input):
        for line in input:
            if '<page>' in line:
                
                # Put the line containing <page> back at the front of the
                # iter before passing to upload_page
                
                yield self.create_page_key_and_body(itertools.chain(iter(['<page>']), input))

    def run(self, input):
        try:
            for key, body in self.split_into_pages(input):
                self.upload_page(key, body)
        except KeyboardInterrupt:
            self.save()
            return 

if __name__ == "__main__":
    Reader().run(sys.stdin)
