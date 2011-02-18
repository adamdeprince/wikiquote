#!/usr/bin/env python


import re 
import sys
import redis
import pycassa  
import hashlib
import xml
import xml.sax
import xml.sax.handler

RE_LETTERS_ONLY = re.compile("[\"']*(^[\w']*)[;.!?,\"]*$")

LETTER = re.compile('\w')

COMMON = set(['', 'the', 'of', 'and', 'to', 'a', 'in', 'for', 'is', 'on', 
              'that', 'by', 'with', 'I', 'or', 'not', 'you', 'be', 'are', 
              'this', 'at', 'is', 'are', 'from', 'your', 'have', 'as', 
              'from', 'all', 'can', 'more', 'has'])

def letters_only(s):
    return ''.join(c for c in s if LETTER.match(c))

def dict_merge(*dicts ):
    keys = set()
    for dict in dicts:
        map(keys.add, dict.keys())

    merged = {}
    for key in keys:
        merged[key] = {}
        for dict in dicts:
            merged[key].update(dict.get(key,{}))
    return merged

def extract_words(s):
    for word in s.split():
        word = word.lower()
        match = RE_LETTERS_ONLY.match(word)
        if match:
            word = match.groups()[0]
            word = letters_only(word)
            if word not in COMMON:
                yield word 

def parse_article(article_text):
    handler = WikiCommentHandler()
    article_text = "<page>" + article_text
    xml.sax.parseString(article_text, handler, IgnoreErrors())
    data = {}

    data['text'] = "\n".join(filter(None, (s.strip() for s in handler.text_data['page'][0].encode('UTF8').split('\n'))))
    data['id'] = handler.text_data['page-id'][0].encode('UTF8')
    data['title'] = handler.text_data['page-title'][0].encode('UTF8')

    i = "%08d" % int(data['id'])
    data['id'] = i 
    words = {}
    for field in ['title', 'text']:
        for word in extract_words(data[field]):
            words[word] = {i: 1}
    return i, data, words

def open_redis_connection():
    return redis.Redis(host='localhost', port=6379, db=0)

def open_cassandra_connection():

    pool = pycassa.connect('Articles')
    raw = pycassa.ColumnFamily(pool, 'RawData')
    polished = pycassa.ColumnFamily(pool, 'PolishedData')
    index = pycassa.ColumnFamily(pool, 'index')
    return pool, raw, polished, index


class IgnoreErrors(xml.sax.handler.ErrorHandler):
    def error(self, exception):
        pass

    def warning(self, exception):
        pass

class WikiCommentHandler(xml.sax.handler.ContentHandler):
    def __init__(self, *args, **kwargs):
        xml.sax.handler.ContentHandler.__init__(self, *args, **kwargs)
        self.character_data = []
        self.text_data = {}
        self.parse_level = []

    def characters(self, content):
        self.character_data.append(content)

    def startElement(self, name, huh=None):
        self.character_data = []
        self.parse_level.append(name)

    def endElement(self, name):
        self.text_data.setdefault('-'.join(self.parse_level), []).append(''.join(self.character_data))
        self.parse_level.pop()


def process(r, pool, raw, polished, ):
    try:
        while True:
            md5 = r.rpop('pending')
            if not md5: return False 

            data = raw.get(md5, ['body'])['body']
            # r.rpush('pending', md5)
            key, polished_data, keywords = parse_article(data)

            polished.insert(key, polished_data)
            return keywords

    except KeyboardInterrupt, e:
        if md5: r.rpush('pending', md5)
        return None
    except Exception, e:
        if md5: r.lpush('error', md5)
        raise e 

        


if __name__ == "__main__":
    r = open_redis_connection()
    pool, raw, polished, index = open_cassandra_connection()
    polished = polished.batch()
    batched_keywords = []
    
    counter = 0
    while True:
        keywords = process(r, pool, raw, polished)
        if keywords is None:
            index.insert(dict_merge(*batched_keywords))
            polished.send()
            break
        counter += 1 
        sys.stdout.write('.')
        if counter > 500:
            keywords = dict_merge(*batched_keywords)
            index.batch_insert(keywords)
            polished.send()
            counter = 0 
        sys.stdout.flush()

    print "done"
