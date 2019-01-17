import unittest
import sys
import os.path
# import logging
from decimal import Decimal
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../")
import dynamodb_tools


class DynamoDBToolsTest(unittest.TestCase):
    """This doesn't mock out init of dynamodb_table
        Current Test Case runtime: >1 second ->bad"""
    def setUp(self):
        self.mytable = dynamodb_tools.RightcallTable('eu-west-1', 'mysillytable')
        self.single_data = {"Name": "f48a46TVd00773", "SVI session": "5bf48a35Vel1d00003050101", "Application": "oij07", "NAS": 497313888045, "Number": 497314020.0, "Reason": "T", "Note": None, "Date": "2018-11-20 23:27:18", "Length": 120, "Skill": "ISRO_TEVA_US_EN"}
        self.multi_data = {"demo_metadata": [{"Name": "f48a46TVd00773", "SVI session": "5bf48a35Vel1d00003050101", "Application": "oij07", "NAS": 497313888045, "Number": 497314020.0, "Reason": "T", "Note": None, "Date": "2018-11-20 23:27:18", "Length": 120, "Skill": "ISRO_TEVA_US_EN"},
                                             {"Name": "f48a46TVd007734", "SVI session": "5bf48a35Vel1d00003050101", "Application": "oij07", "NAS": 497313888045, "Number": 497314020.0, "Reason": "T", "Note": None, "Date": "2019-01-17 12:00:18", "Length": 1202, "Skill": "ISRO_TEVA_US_EN"}]}
        self.expected_clean_single_data = {"referenceNumber": "f48a46TVd00773",
                                           "skill": "ISRO_TEVA_US_EN",
                                           "length": 120,
                                           "date": "2018-11-20 23:27:18"}
        self.expected_minute_single_data = {"referenceNumber": "f48a46TVd00773",
                                            "skill": "ISRO_TEVA_US_EN",
                                            "length": Decimal(str(2)),
                                            "date": "2018-11-20 23:27:18"}

    def test_sanitize_data(self):
        data_list = list(self.single_data)
        self.assertRaises(TypeError, self.mytable.sanitize_data, data_list)
        clean_data = self.mytable.sanitize_data(self.single_data)
        self.assertEqual(clean_data, self.expected_clean_single_data)
        self.assertRaises(Exception, self.mytable.sanitize_data, self.multi_data)

    def test_seconds2minutes(self):
        self.assertRaises(KeyError, self.mytable.seconds2minutes, {'missing': 'length_key'})
        self.assertRaises(TypeError, self.mytable.seconds2minutes, {'length': '200'})
        self.assertRaises(TypeError, self.mytable.seconds2minutes, {'length': 200.0})
        converted_data = self.mytable.seconds2minutes(self.expected_clean_single_data)
        self.assertEqual(converted_data, self.expected_minute_single_data)

    def test_batch_write(self):
        pass


if __name__ == '__main__':
    unittest.main()
