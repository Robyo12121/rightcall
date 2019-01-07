import unittest
# from unittest.mock import patch
import sys
import os.path
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../")
import promotion
import logging


class PromotionTest(unittest.TestCase):

    def setUp(self):
        self.sentence = ("""I shall deviate from my habitual norm and attempt to flourish my writing with eloquence and ratiocination sanguinely attempting to enlighten the populous of Quora all the while heeding to the concise literary format present in my conventional manner of speech""")
        self.correct_stem = ("""I shall deviat from my habitu norm and attempt to flourish my write with eloqu and ratiocin sanguin attempt to enlighten the popul of quora all the while heed to the concis literari format present in my convent manner of speech""")
        self.stem_words = ['shall', 'deviat', 'habitu', 'norm', 'attempt', 'flourish', 'write', 'eloqu', 'ratiocin', 'sanguin', 'attempt', 'enlighten', 'popul', 'quora', 'heed', 'concis', 'literari', 'format', 'present', 'convent', 'manner', 'speech']

        # Following words are not stemmed 
        self.words1 = ['yo', 'dawg', 'we', 'heard', 'you', 'like', 'sneaky', 'tunnel', 'spying', 'so', 'virtual-assist']         
        self.words2 = ['we', 'put', 'a', 'tunnel', 'in', 'your', 'tunnel', 'so', 'you', 'could', 'see', 'a', 'tunnel']
        self.correct_vocab = {'yo': 1, 'dawg': 1, 'we': 1, 'heard': 1, 'you': 1, 'like': 1, 'sneaky': 1, 'tunnel': 1, 'spying': 1, 'so': 1, 'virtual-assist': 2, 'put': 1, 'a': 1, 'in': 1, 'your': 1, 'could': 1, 'see': 1}

        self.dog1 = ['thi', 'dog', 'like', 'dan']
        self.dog2 = ['dan', 'like', 'dog']
        self.dog_vocab = {'thi': 1, 'like': 1, 'dog': 1, 'dan': 1}
        self.correct_dog1_vec = [1, 1, 1, 1]
        self.correct_dog2_vec = [0, 1, 1, 1]
        
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
        stem_sent = promotion.get_stems(self.sentence)
        self.assertEqual(stem_sent, self.correct_stem)

    def test_generate_path(self):
        # myfakepath = 'C:/Users/Desktop/Robin/this/is/my/directory'
        pass

    def test_bagofwords(self):
        self.assertEqual(promotion.bagofwords(self.dog1, self.dog_vocab), self.correct_dog1_vec)
        self.assertEqual(promotion.bagofwords(self.dog2, self.dog_vocab), self.correct_dog2_vec)

    def test_preprocess(self):
        self.assertEqual(promotion.preprocess(self.sentence), self.stem_words)

    def test_normalize_tf(self):
        self.assertRaises(ValueError, promotion.normalize_tf, 6)
        self.assertRaises(ValueError, promotion.normalize_tf, 'string')
        self.assertRaises(ValueError, promotion.normalize_tf, {'my': 'dict'})

    def test_dot_prod(self):
        a = [1, 1, 0, 0, 1]
        b = [0, 0, 1, 0, 1]
        c = [-1, -2, 3]
        d = [4, 0, -8]
        self.assertEqual(promotion.dot_prod(a,b), 1)
        a = 'this is not a list'
        self.assertRaises(ValueError, promotion.dot_prod, a, b)
        self.assertEqual(promotion.dot_prod(c,d), -28)

    def test_construct_vocab(self):
        """This function should:
            - remove duplicate words
            - assign a weight of 1 to all words except 'virtual-assist' which should get 2
        """
        vocab = promotion.construct_vocab(self.words1, self.words2)
        self.assertEqual(vocab, self.correct_vocab)
        self.assertRaises(ValueError, promotion.construct_vocab, 6, self.words1)
        self.assertRaises(ValueError, promotion.construct_vocab, 'six', self.words2)
        self.assertRaises(ValueError, promotion.construct_vocab, self.words1, {'hello':'world'})
        
    def test_get_sentences(self):
        """Dependencies:
                Function: 'tokanize_aws_transcript'"""
        

    def test_sentence_similarity(self):
        pass

    def test_count_hits_in_sentence(self):
        pass

    def test_document_similarity(self):
        pass

    def test_Promotion(self):
        pass


        
   
        

if __name__ == '__main__':
    unittest.main()
