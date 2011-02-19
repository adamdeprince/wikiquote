#!/usr/bin/env python

"Parses and indexes outstanding wikiquote articles"

import common 
import hashlib
import pycassa  
import re 
import redis
import sys
import xml.sax.handler

BATCH_SIZE = 500 

RE_LETTERS_ONLY = re.compile("[\"'*]*([\w']+)[*;.!?,\"]*")

LETTER = re.compile('\w')

COMMON = set(['', 'the', 'of', 'and', 'to', 'a', 'in', 'for', 'is', 'on', 
              'that', 'by', 'with', 'I', 'or', 'not', 'you', 'be', 'are', 
              'this', 'at', 'is', 'are', 'from', 'your', 'have', 'as', 
              'from', 'all', 'can', 'more', 'has'])

def dict_merge(*dicts ):
    """dict_merge

    Merge dictionaries of dictionaries.  

    >>> dict_mergre(dict(a={1:1}, b={1:1}),
                    dict(b={2:1}, c={2:1}))
    {'a':{1:1}, 'b':{1:1, 2:1}, 'c':{2:1}}

    Args:
      *dicts: Dictionaries to merge
      
    Returns:
      Single merged dictionary
    """
    keys = reduce(set.union, map(set, dicts))

    merged = {}
    for key in keys:
        merged[key] = {}
        for d in dicts:
            merged[key].update(d.get(key,{}))
    return merged

def extract_words(wikiwiki_text):
    """Extract words from a noisy segment of wikiwiki text.
    
    >>> list(extract_words('**Hello World!**'))
    ['hello', 'world']
    
    Args:
      wikiwiki_text: Wikiwiki style text
    Returns:
      List of keywords
    """

    for word in wikiwiki_text.split():
        match = RE_LETTERS_ONLY.search(word.lower())
        if match:
            word = match.groups()[0]
            if word not in COMMON:
                yield word 

class IgnoreErrors(xml.sax.handler.ErrorHandler):
    """Ignore all ignorable XML parsing errors.""" 
    def error(self, exception):
        pass
    def warning(self, exception):
        pass

class WikiCommentHandler(xml.sax.handler.ContentHandler):
    """Make XML text segments indexable

    >>> article_text = "<a>Hello<b>World</b></a>"
    >>> handler = WikiCommentHandler()
    >>> xml.sax.parseString(article_text, handler, IgnoreErrors())
    >>> handler['a']
    Hello
    >>> handler['a-b']
    World

    Also provides as_dict() method for generating cassandra payloads 
    """
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
        current_list = self.text_data.setdefault('-'.join(self.parse_level), [])
        current_list.append(''.join(self.character_data))
        self.parse_level.pop()

    def __getitem__(self, key):
        return self.text_data[key][0].encode('UTF8')

    @staticmethod
    """Writes an integer as an 8 letter string.

    >>> as_ident_string(1)
    00000001 
    """
    def as_ident_string(i):
        return "%08d" % int(i)

    """Returns the current parsed enwikiquote <page> segment as a
    cassandra parsable field. 
    
    Returns:
      dict(text= ...       # Body of the article
           ident = 0000... # Article ID number 
           title = ...     # Article title 
    """
    def as_dict(self):
        return dict(text=self['page'],
                    ident = self.as_ident_string(self['page-id']),
                    title = self['page-title'])


def parse_article(article_text):
    """Parse an article.

    Given an aritcle's XML text erturns the id number, its body and a
    transposes list of keywords sutiable for cassandra.

    Args:
      article_text: XML of the wikiquotes page 
    
    Returns:
      id, data, words

      id: The article's id # for use as a cassandra key 
      data: text, ident and title from WikiCommentHandler.as_dict

      words: A transposed text index with keywords as rows markers and
        article ids as columns.

    Cavets: Note this doesn't scale very well in the long run,
        cassandra's ram requirements are proportional to the number of
        columns, and this creates a column for each document, but for
        now, it works.)
    """
    handler = WikiCommentHandler()
    xml.sax.parseString(article_text, handler, IgnoreErrors())

    data = handler.as_dict()

    words = {}
    for field in ['title', 'text']:
        for word in extract_words(data[field]):
            words[word] = {data['ident']: 1}
    return data['ident'], data, words


def process_one_element(que, pool, raw, polished ):
    """Process the next item in the Redis que
    
    Args:
      que: common.Que object holding list of raw file ids to parse
      pool: Cassandra pool
      raw: Cassandra ColumnStore from which to retrieve raw data
      polished: Cassandra Column store to write parsed data.  This may
        be a batch object.

    Returns:
      Keyword to document id matching

    Notes:

      There is a lot of keyword overlap between articles, so instead
      of forciing cassandra to notice that we updating the same rows
      over and over again, these items are returned so that the
      connsoluation can be performed in python on an "interarticle"
      basis.
    """
    md5 = None
    try:
        while True:
            md5 = que.pop()
            if not md5: return False 
            data = raw.get(md5, ['body'])['body']
            key, polished_data, keywords = parse_article(data)
            polished.insert(key, polished_data)
            return keywords
    except KeyboardInterrupt:
        if md5: que.unpop(md5)
        return None
    except Exception, exc:
        if md5: que.push(md5)
        raise exc 

def process(status=lambda x:None):

    """Process all of the outstanding raw messages

    Args: 
      status: Function that accepts a string to report status.  Right
        now "status" is just "." for each processed item and "!" for
        each batched save.
    Todo:
      We shuld be using multigets to reduce the load on cassandra.  
    """
    q = common.Que()
    pool, raw, polished, index = common.open_cassandra_connections()
    polished = polished.batch()
    batched_keywords = []
    
    counter = 0
    try:
        while True:
            keywords = process_one_element(q, pool, raw, polished)
            if not keywords:
                if batched_keywords:
                    index.insert(dict_merge(*batched_keywords))
                polished.send()
                break
            counter += 1 
            status('.')
            if counter > BATCH_SIZE:
                status('!')
                if batched_keywords:
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
