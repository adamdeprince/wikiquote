#!/usr/bin/env python

import common 
import hashlib
import pycassa  
import re 
import redis
import sys
import xml
import xml.sax
import xml.sax.handler

BATCH_SIZE = 500 

RE_LETTERS_ONLY = re.compile("[\"'*]*([\w']+)[*;.!?,\"]*")

LETTER = re.compile('\w')

COMMON = set(['', 'the', 'of', 'and', 'to', 'a', 'in', 'for', 'is', 'on', 
              'that', 'by', 'with', 'I', 'or', 'not', 'you', 'be', 'are', 
              'this', 'at', 'is', 'are', 'from', 'your', 'have', 'as', 
              'from', 'all', 'can', 'more', 'has'])

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
        match = RE_LETTERS_ONLY.search(word.lower())
        if match:
            word = match.groups()[0]
            if word not in COMMON:
                yield word 

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

    def __getitem__(self, key):
        return self.text_data[key][0].encode('UTF8')

    @staticmethod
    def as_ident_string(i):
        return "%08d" % int(i)

    def as_dict(self):
        return dict(text=self['page'],
                    ident = self.as_ident_string(self['page-id']),
                    title = self['page-title'])

def parse_article(article_text):
    handler = WikiCommentHandler()
    xml.sax.parseString(article_text, handler, IgnoreErrors())

    data = handler.as_dict()

    words = {}
    for field in ['title', 'text']:
        for word in extract_words(data[field]):
            words[word] = {data['ident']: 1}
    return data['ident'], data, words



def process_one_element(que, pool, raw, polished ):
    try:
        while True:
            md5 = que.pop()
            if not md5: return False 
            data = raw.get(md5, ['body'])['body']
            key, polished_data, keywords = parse_article(data)
            polished.insert(key, polished_data)
            return keywords

    except KeyboardInterrupt, e:
        if md5: que.unpop(md5)
        return None
    except Exception, e:
        if md5: que.push(md5)
        raise e 

def process(status=lambda x:None):
    q = common.Que()
    pool, raw, polished, index = common.open_cassandra_connections()
    polished = polished.batch()
    batched_keywords = []
    
    counter = 0
    try:
        while True:
            keywords = process_one_element(q, pool, raw, polished)
            if keywords is None:
                index.insert(dict_merge(*batched_keywords))
                polished.send()
                break
            counter += 1 
            status('.')
            if counter > BATCH_SIZE:
                status('!')
                keywords = dict_merge(*batched_keywords)
                index.batch_insert(keywords)
                polished.send()
                counter = 0 
        status('done\n')
    except KeyboardInterrupt:
        polished.send()
        
if __name__ == "__main__":
    def log(s):
        sys.stdout.write(s)
        sys.stdout.flush()

    process(status=log)
