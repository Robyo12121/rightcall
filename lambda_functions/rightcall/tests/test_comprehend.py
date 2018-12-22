import unittest
from unittest.mock import patch
import sys
import os.path
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../")
import comprehend


class TestComprehend(unittest.TestCase):

    def test_sum_sentiments(self):
        pass

    def test_chunkify(self):
        pass

    def test_best_sentiment(self):
        pass

    def test_create_set(self):
        ent_list = [{'Text': 'hello'}, {'Text': 'there'}, {'Text': 'there'}]
        ent_set = comprehend.create_set(ent_list)
        self.assertEqual(ent_set, {'hello': {'Text': 'hello', 'Count': 1}, 'there': {'Text': 'there', 'Count': 2}})

    def test_get_sentiment(self):
        pass

    def test_get_entities(self):
        pass

    def get_key_phrases(self):
        pass


if __name__ == '__main__':
    unittest.main()
