import requests
from requests_aws4auth import AWS4Auth
import boto3
import json


class Elasticsearch:
    """Custom Elasticsearch to abstract away talking to elasticsearch
    domain. Uses HTTP requests."""
    def __init__(self, host, index, region, auth=None):
        self.host = host
        self.index = index
        self.base_url = 'https://' + self.host + '/' + self.index
        self.awsauth = auth

    def put_item(self, doc_id, item):
        method = 'PUT'
        url = self.base_url + '/' + '_doc' + '/' + doc_id
        print(url)
        return self.make_request(method, url, item)

    def get_item(self, doc_id):
        method = 'GET'
        url = self.base_url + '/' + '_doc' + '/' + doc_id
        print(url)
        return self.make_request(method, url)

    def search(self, query):
        url = self.base_url + '/_search'
        method = 'GET'
        results = json.loads(self.make_request(method, url, query)['body'])
        hits = [item['_source'] for item in results['hits']['hits']]
        return hits

    def get_hit_fields(self, es_resp_obj_dict):
        """Returns a list of they fields in an elasticsearch search response
            hit object."""
        if es_resp_obj_dict['hits']['hits']:
            fields = [key for key in es_resp_obj_dict['hits']['hits'][0]['_source'].keys()]
        else:
            return []
        return fields

    def make_request(self, method, url, data=None):
        headers = {"Content-Type": "application/json"}
        r = requests.request(method, url, auth=self.awsauth, headers=headers, data=json.dumps(data))
        response = {"statusCode": 200,
                    "headers": {"Access-Control-Allow-Origin": '*'},
                    "isBase64Encoded": False}
        response['body'] = r.text
        return response

    def load_call_record(self, db_item, s3_item):
        """Takes two dictionaries (one from dynamo database record,
        the other an object from an s3 bucket and combines them into
        a single dictionary before indexing it to elasticsearch."""
        # module_logger.debug(f"""load_call_record called with DB record: {db_item['referenceNumber']}, S3 Object:{s3_item['referenceNumber']} on {index}""")
        data = {**db_item, **s3_item}
        try:
            self.put_item(data['referenceNumber'], data)
        except Exception as e:
            # module_logger.error(e)
            raise e
        else:
            return True

    def exists(self, referenceNumber):
        """Checks if a document exists within an elasticsearch index
        Returns boolean"""
        if '.json' in referenceNumber:
            raise ValueError(f'{referenceNumber} is wrong format: contains ".json"')
        result = json.loads(self.get_item(referenceNumber)['body'])
        if result['found']:
            return True
        else:
            return False

    def fully_populated_in_elasticsearch(self, referenceNumber):
        """
        Checks if a given document present in a given
        index is missing its's metadata fields
        Returns boolean
        """
        # Query the index for the document associated with that reference number
        resp = self.search(referenceNumber)
        # Check what fields the doc has (all call metadata or not?)

        db_meta_data_fields = ['date', 'length', 'skill']
        es_fields = self.get_hit_fields(resp)

        if set(db_meta_data_fields).issubset(set(es_fields)):
            # module_logger.debug(f"All: {db_meta_data_fields} contained in {es_fields}")
            return True
        else:
            # module_logger.debug(f"""{referenceNumber} elasticsearch document missing
            #     one or more fields from {db_meta_data_fields}""")
            return False

    def rename(self, dictionary):
        """Receives a dictionary and renames replaces any keys
        that also appear in the 'fields' dictionary with the appropriate
        value"""
        # module_logger.debug(f"rename called with {dictionary.keys()}")
        fields = {'ref': 'referenceNumber',
                  'reference_number': 'referenceNumber',
                  'promo': 'promotion',
                  'key_phrases': 'keyPhrases',
                  'keyphrases': 'keyPhrases'}
        if type(dictionary) is not dict:
            # module_logger.error("input is not dictionary. ERROR")
            raise TypeError("input is not dictionary. ERROR")

        for field in dictionary.keys():
            if field in fields.keys():
                # module_logger.debug(f"Renaming {field} to {fields[field]}")
                dictionary[fields[field]] = dictionary[field]
                # module_logger.debug(f"Deleting {field}")
                del dictionary[field]
        return dictionary


if __name__ == '__main__':
    service = 'es'
    REGION = 'eu-west-1'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key,
                       credentials.secret_key,
                       REGION,
                       service,
                       session_token=credentials.token)
    es = Elasticsearch('search-rightcall-445kqimzhyim4r44blgwlq532y.eu-west-1.es.amazonaws.com', "rightcall", "eu-west-1", auth=awsauth)

    doc = {
        "referenceNumber": "c47a6eTVd00882",
        "text": "Mhm, hello, you reached their service. This i'm speaking. Can you provide me your full name or previous-ticket-number, please, um, and the ticket-number h i n c, just a second, please. Sorry, just a second, please i n c one free 1 3, 6 1 7 9 1. Okay, just a second while i check the stick it to see. So you need a password. Reset for the s a p okay, just the second, ma'am. So ah, oh, oh, no, very sick. Your boss word. Mhm! Yeah. Yeah. Hm. Yeah. Yeah. Yeah. Oh! Mhm! Okay. Yeah. Yeah, okay, yeah. Okay, okay. Mhm, mhm. Your user name for some copies is devorah n t e e r e u x. And yeah. Yeah, okay, yeah, yeah. Hm. Okay, i am locked your account. Thank you. Need the ah, also, password reset. Okay, okay, just a second. Oh! Oh! Oh, yeah. Yeah, yeah. So, uh, your new temporary password is. Have i want to free four with the capital ? T can you try to be very much ? O.K. ? Oh! Yeah, okay. Yeah. Mhm, mhm. Yeah. Oh, yeah. Yeah, okay, thank you. Okay, would you like to write the ticket-number ? No that's. Okay, thank you very much, and have a lot of the day. Bye. Okay.",
        "sentiment": "neutral",
        "promotion": "fail",
        "entities": [
            "second",
            "one",
            "1",
            "3",
            "6",
            "7",
            "9",
            "four"
        ],
        "keyphrases": [
            "Mhm",
            "their service",
            "This i",
            "your full name",
            "previous-ticket-number",
            "um",
            "the ticket-number h i n c",
            "just a second",
            "i n c",
            "one free 1 3, 6 1 7 9 1",
            "the stick",
            "a password",
            "Reset",
            "the s a p",
            "just the second",
            "ma'am",
            "Your boss word",
            "mhm",
            "Your user name",
            "some copies",
            "devorah n t e e r e u x",
            "yeah",
            "your account",
            "the ah",
            "password reset",
            "your new temporary password",
            "the capital",
            "O.K.",
            "the ticket-number",
            "a lot",
            "the day"
        ]
    }

    result = es.exists(doc['referenceNumber'])
    print(result)
