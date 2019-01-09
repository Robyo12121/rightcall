import unittest
import sys
import os.path
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../")
import script


class PromotionTest(unittest.TestCase):

    def test_check_greeting(self):
        pass


if __name__ == '__main__':
    unittest.main()
