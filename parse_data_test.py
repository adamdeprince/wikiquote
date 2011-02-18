#!/usr/bin/env python
# coding=UTF-8

import parse_data
import unittest


class DictUpdateTest(unittest.TestCase):
    def test_merge(self):
        a = {'a':{1:1},
             'b':{1:1}}
        b = {'b':{2:1},
             'c':{2:1}}
        expected = {'a':{1:1},
                    'b':{1:1, 2:1},
                    'c':{2:1}}
        self.assertEqual(parse_data.dict_merge(a, b), expected)


class ExtractWordsTest(unittest.TestCase):
    def test_extract_from_wikiwiki(self):
        expected = ['hello', 'wiki', 'world']
        actual = list(parse_data.extract_words('** Hello wiki World!**'))
        self.assertEquals(expected, actual)

    def test_extract_skips_common_words(self):
        expected = ['hello', 'world']
        actual = list(parse_data.extract_words('** Hello this is a World!**'))
        self.assertEquals(expected, actual)

    def test_extract_possessive(self):
        expected = ['hello', 'adam\'s', 'world']
        actual = list(parse_data.extract_words("*Hello this is Adam's World!*"))
        self.assertEquals(expected, actual)

class ParseArticleTest(unittest.TestCase):
    def setUp(self):
        SOURCE = open("parse_data_test.xml").read()
        self.ident, self.data, self.key_words = parse_data.parse_article(SOURCE)

    def test_ident(self):
        self.assertEqual(self.ident, '00000002')
    
    def test_data(self):
        self.assertEqual(self.data['title'], 'Albert Einstein')

    def test_key_words(self):
        self.assertTrue('destiny' in self.key_words, {})

if __name__ == "__main__":
    unittest.main()
