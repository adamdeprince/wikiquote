#!/usr/bin/env python

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
    def test_extract(self):
        expected = ['hello', 'world']
        actual = list(parse_data.extract_words('<h1> Hello World! </h1>'))
        self.assertEquals(expected, actual)

    def test_extract_skips_common_words(self):
        expected = ['hello', 'world']
        actual = list(parse_data.extract_words('<h1> Hello this is a World! </h1>'))
        self.assertEquals(expected, actual)

if __name__ == "__main__":
    unittest.main()
