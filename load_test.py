#!/usr/bin/env python

import unittest
import load 

class PageKeyAndBodyTest(unittest.TestCase):
    def setUp(self):
        self.key, self.body = load.Reader.create_page_key_and_body(iter(['<page>', '</page>']))

    def test_key(self):
        self.assertEquals(self.key, '8e42de09729275e9879ba4a2') 

    def test_key_length_is_96_bits(self):
        self.assertEquals(len(self.key) * 4, 96)

    def test_body(self):
        self.assertEquals(self.body, '<page>\n</page>')

class SplitIntoPagesTest(unittest.TestCase):
    def setUp(self):
        input = ['<page>', '1', '</page>', '<page>', '2', '</page>']        
        self.pages = list(load.Reader().split_into_pages(iter(input)))
    def test_split(self):
        self.assertEquals(self.pages,
                          [('a8d138cd0795788b5439095f', '<page>\n1\n</page>'), 
                           ('1e0df78514aa31dec5efe701', '<page>\n2\n</page>')] )

if __name__ == "__main__":
    unittest.main()
    
