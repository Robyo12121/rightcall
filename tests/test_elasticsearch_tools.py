import unittest
from unittest.mock import Mock, MagicMock, patch
import sys, os
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../")
import elasticsearch_tools


class TestElasticsearch(unittest.TestCase):

    def setUp(self):
        self.es = elasticsearch_tools.Elasticsearch('some_host.robinisbadatpython.com',
                                                    'silly_index',
                                                    'onthetrain')

    def test_constructor(self):
        self.assertEqual(self.es.host, 'some_host.robinisbadatpython.com')
        self.assertEqual(self.es.index, 'silly_index')
        self.assertEqual(self.es.base_url, 'https://some_host.robinisbadatpython.com/silly_index')
        self.assertIsNone(self.es.awsauth)

    def test_put_item(self):
        pass
        # with patch('requests.request') as mocked_request:
            # mocked_request.return_value = {"statusCode": 200,
            #                                "headers": {"Access-Control-Allow-Origin": '*'},
            #                                "isBase64Encoded": False,
            #                                "body": '{"item": "some_silly_return_item"}'}
            
