import unittest
# from unittest.mock import patch
import sys
import os.path
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../")
import promotion
import logging


class PromotionTest(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_setupLogging(self):
        loglevel = 'WRONG'
        self.assertRaises(ValueError, promotion.setupLogging, LOGLEVEL=loglevel)
        loglevel = 'DEBUG'
        logger = promotion.setupLogging(loglevel)
        self.assertEqual(logger.name, 'promotion')
        self.assertTrue(logger.hasHandlers())
        self.assertEqual(logging.getLevelName(logger.getEffectiveLevel()), loglevel)
        

    def test_get_stems(self):
        sentence = ("""I shall deviate from my habitual norm and attempt to flourish my writing with eloquence and ratiocination sanguinely attempting to enlighten the populous of Quora all the while heeding to the concise literary format present in my conventional manner of speech""")
        correct_stem = ("""I shall deviat from my habitu norm and attempt to flourish my write with eloqu and ratiocin sanguin attempt to enlighten the popul of quora all the while heed to the concis literari format present in my convent manner of speech""")
        stem_sent = promotion.get_stems(sentence)
        self.assertEqual(stem_sent, correct_stem)

    def test_generate_path(self):
        # myfakepath = 'C:/Users/Desktop/Robin/this/is/my/directory'
        pass

    def test_bagofwords(self):
        # sentence_words = ['this', 'is', 'a', 'big', 'bag', 'of', 'words']
        pass

    def test_construct_vocab(self):
        # words1 = ['yo', 'dawg', 'we', 'heard', 'you', 'like', 'sneaky', 'tunnel', 'spying', 'so',
        #           'we', 'put', 'a', 'tunnel', 'in' 'your', 'tunnel', 'so', 'you', 'could', 'see', 'a', 'tunnel']
        # words2 = ['
        pass

    def test_preprocess(self):
        sentence = """I shall deviate from my habitual norm and attempt to flourish my writing with eloquence and ratiocination sanguinely attempting to enlighten the populous of Quora all the while heeding to the concise literary format present in my conventional manner of speech"""
        pass

    def test_dot_prod(self):
        a = [1, 1, 0, 0, 1]
        b = [0, 0, 1, 0, 1]
        print(promotion.dot_prod(a,b))
        

if __name__ == '__main__':
    unittest.main()
