import unittest
import sys
import os.path
import logging
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../")
import dynamodb_tools


class DynamoDBToolsTest(unittest.TestCase):

    def setUp(self):
        self.table = dynamodb_tools.RightcallTable('eu-west-1', 'mysillytable')

    def test_sanitize_data(self):
        data = {"Name": "f48a46TVd00773", "SVI session": "5bf48a35Vel1d00003050101", "Application": "oij07", "NAS": 497313888045, "Number": 497314020.0, "Reason": "T", "Note": None, "Date": "2018-11-20 23:27:18", "Length": 1202, "Skill": "ISRO_TEVA_US_EN"}
        self.table.sanitize_data(data)
        self.assertRaises(AttributeError)

if __name__ == '__main__':
    unittest.main()
