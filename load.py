#!/usr/bin/env python
 
import common 
import itertools 
import hashlib
import sys

HASH_SIZE = 96 / 4 # 96 bits of hash in nibbles 

BLOCK_SIZE = 5000



class Reader:
    """Read and splitup a wikiquote dump."""
    def __init__(self):
        self.que = common.BatchQue()
        self.pool, self.raw, _, _ = common.open_cassandra_connections()
        self.raw = self.raw.batch()
        self.counter = 1 

    def occationally_save(self, interval=BLOCK_SIZE):
        """Called after each update.  Saves every BLOCK_SIZE invocations."""
        self.counter += 1 
        if self.counter > interval:
            self.save()
            self.counter = 0 

    def save(self):
        """Write batched data to Redis and Cassandra."""
        self.raw.send()
        self.que.save()

    @staticmethod
    def digest(s):
        """Create a truncqted 96 bit md5 hash of a string.

        Args:
          s: String to hash
          
        Returns:
          24 character long base hexdigest of s.
        """
        return hashlib.md5(s).hexdigest()[:HASH_SIZE]

    @staticmethod 
    def lines_from_page(input):
        """Extract all of the lines from inside a <page></page> pair.
        
        Yields:
          Lines upto and including </page>

        Cavets:
          <page> and </page> are expected to be on lines by themselves. 
        """
        while True:
            line = input.next()
            yield line 
            if '</page>' in line:
                return 
    @staticmethod
    """For input stream return the hash (key) and body of the next page."""
    def create_page_key_and_body(input):
        data = '\n'.join(Reader.lines_from_page(input))
        return Reader.digest(data), data 

    def upload_page(self, key, body ):
        """Upload a raw block of xml

        Creates a cassandra row indexed on key with the body stored
        under the column "body."  

        Also inserts key into the Redis que.  

        Args:
          key: This block's key.  Usuallly the trunctaed md5 from digest.
          body: The xml of this block
        """
        self.que.push(key)
        self.raw.insert(key, dict(body=body))
        self.occationally_save()

    def split_into_pages(self, input):
        """Split a large XML file into pages.

        Processes each page with create_page_key_and_body 
        
        Args:
          input: An inter that returns lines from a file.
                 Ex: iter(sys.stdin)

        Yields:
          key, body

          key: The partial md5sum from digest
          body: The entire xml source code of "this" page.

        Cavets:  We take advantage of the fact the wikiquotes dump foramt 
        places <page> and </page> alone on lines by themselves. 
        """
        for line in input:
            if '<page>' in line:
                
                # Put the line containing <page> back at the front of the
                # iter before passing to upload_page
                
                yield self.create_page_key_and_body(
                    itertools.chain(iter(['<page>']), input))
    
    def run(self, input):
        """Slurp in a wikimedia file and store its data and keys.

        Example: Reader().run(sys.stdin)
        """
        try:
            for key, body in self.split_into_pages(input):
                self.upload_page(key, body)
        except KeyboardInterrupt:
            self.save()
            return 

if __name__ == "__main__":
    Reader().run(sys.stdin)
