import requests
from requests_aws4auth import AWS4Auth
import boto3
import json
import logging
import decimal

module_logger = logging.getLogger('rightcall.elasticsearch_tools')


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class Elasticsearch:
    """Custom Elasticsearch to abstract away talking to elasticsearch
    domain. Uses HTTP requests."""
    def __init__(self, host, region, index=None, auth=None):
        self.host = host
        self.index = index
        self.base_url = 'https://' + self.host + '/'
        self.index_url = self.base_url + self.index
        self.awsauth = auth

    def make_request(self, method, url, data=None):
        module_logger.info(f"{self.make_request.__name__} called with {method}, {url}")
        module_logger.debug(f"...with data: {data}")
        headers = {"Content-Type": "application/json"}
        r = requests.request(method, url, auth=self.awsauth, headers=headers, data=json.dumps(data, cls=DecimalEncoder))

        # response = {"statusCode": r.status_code,
        #             "headers": {"Access-Control-Allow-Origin": '*'},
        #             "isBase64Encoded": False}
        # response['body'] = r.json()
        response = r.json()
        module_logger.debug(f"Response: {response}")
        return response

    def put_item(self, doc_id, item):
        method = 'PUT'
        url = self.index_url + '/' + '_doc' + '/' + doc_id
        return self.make_request(method, url, item)

    def get_item(self, doc_id):
        method = 'GET'
        url = self.index_url + '/' + '_doc' + '/' + doc_id
        return self.make_request(method, url)

    def create_index(self, name, mapping=None, set_as_current_index=False):
        """Create an elasticsearch index with the given name, mapping and settings
        INPUT: (str) name - name of index
               (dict) mapping - mapping for fields in index
               (bool) set_as_current_index - set this index as the current working index
        """
        # Check if index already exists with that name
        url = self.base_url + name
        response = self.make_request('GET', url)
        if name in response.keys():
            module_logger.warning(f"Index {name} already exists. Aborting create index call...")
            if set_as_current_index:
                module_logger.warning(f"Current working index changed from {self.index} to {name}")
                self.index = name
                self.index_url = self.base_url + self.index
            else:
                module_logger.warning(f"Current working index remains: {self.index}")
                raise Exception("Working index did not change when requested")
            return

        module_logger.debug(f"Creating {name} index...")
        data = {}
        if mapping is not None:
            data = mapping
        response = self.make_request('PUT', url, data=data)
        module_logger.debug(response)
        if set_as_current_index and response['acknowledged']:
            module_logger.warning(f"Current working index changed from {self.index} to {name}")
            self.index = name
            self.index_url = self.base_url + self.index
        else:
            module_logger.warning(f"Current working index remains: {self.index}")
            raise Exception("Working index did not change when requested")
        return response

    def reindex(self, old_index_name, new_index_name):
        url = self.base_url + '_reindex'
        data = {}
        data['source'] = {'index': old_index_name}
        data['dest'] = {'index': new_index_name}
        response = self.make_request('POST', url, data=data)
        return response

    def delete_index(self, index_name):
        url = self.base_url + index_name
        response = self.make_request('DELETE', url)
        return response

    def search(self, query):
        """Search index using query
        INPUT: expects elasticsearch query in full.
        OUTPUT: list of dicts each containing a search hit
        """
        url = self.index_url + '/_search'
        method = 'GET'
        results = self.make_request(method, url, query)
        hits = [item['_source'] for item in results['hits']['hits']]
        return hits

    def search_by_ref(self, referenceNumber):
        if '.json' in referenceNumber:
            module_logger.error(f"{self.search_by_ref.__name__}: '.json' found in {referenceNumber}")
            raise ValueError(f"{self.search_by_ref.__name__}: {referenceNumber} is wrong format: contains '.json'")
        query = {"query": {"match": {"referenceNumber": referenceNumber}}}
        module_logger.info(f"Query: {query}")
        hits = self.search(query)
        module_logger.debug(hits)
        if len(hits) > 1:
            module_logger.warning(f"{self.search_by_ref.__name__}: More than one hit found!")
        if not hits:
            module_logger.error(f"{self.search_by_ref.__name__}: No hits: {hits}")
            raise Exception(f"{self.search_by_ref.__name__}: Nothing found!")
        if type(hits[0]) is not dict:
            module_logger.error(f"Return value is not a dictionary!")
            raise ValueError(f"{self.search_by_ref.__name__}: Return value is not a dictionary!")
        return hits[0]

    def load_call_record(self, db_item, s3_item):
        """Takes two dictionaries (one from dynamo database record,
        the other an object from an s3 bucket and combines them into
        a single dictionary before indexing it to elasticsearch."""
        # module_logger.debug(f"""load_call_record called with DB record: {db_item['referenceNumber']}, S3 Object:{s3_item['referenceNumber']} on {index}""")
        data = {**db_item, **s3_item}
        try:
            self.put_item(data['referenceNumber'], data)
        except Exception as e:
            module_logger.error(e)
            raise e
        else:
            return True

    def exists(self, referenceNumber):
        """Checks if a document exists within an elasticsearch index
        Returns boolean"""
        if '.json' in referenceNumber:
            raise ValueError(f'{referenceNumber} is wrong format: contains ".json"')
        result = self.get_item(referenceNumber)
        module_logger.debug(f"Exists: {result}")
        if result['found']:
            module_logger.debug("exists: Returning True")
            return True
        else:
            module_logger.debug("exists: Returning False")
            return False

    def fully_populated_in_elasticsearch(self, referenceNumber):
        """
        Checks if a given document present in a given
        index is missing its's metadata fields
        Returns boolean
        """
        # Query the index for the document associated with that reference number
        resp = self.search_by_ref(referenceNumber)
        # Check what fields the doc has (all call metadata or not?)

        data_fields = ['skill', 'referenceNumber', 'date', 'sentiment', 'keyPhrases', 'promotion', 'text', 'length', 'entities']
        # es_fields = self.get_hit_fields(resp)
        # module_logger.debug(es.fields)
        if set(data_fields).issubset(set(resp.keys())):
            module_logger.debug(f"All fields present")
            return True
        else:
            module_logger.warning(f"Missing {set(data_fields).difference(set(resp.keys()))} from {data_fields}")
            return False

    def rename(self, dictionary):
        """Receives a dictionary and renames replaces any keys
        that also appear in the 'fields' dictionary with the appropriate
        value"""
        module_logger.debug(f"rename called with {dictionary.keys()}")
        mapping = {
            'ref': 'referenceNumber',
            'reference_number': 'referenceNumber',
            'promo': 'promotion',
            'key_phrases': 'keyPhrases',
            'keyphrases': 'keyPhrases'}

        if type(dictionary) is not dict:
            module_logger.error("input is not dictionary. ERROR")
            raise TypeError("input is not dictionary. ERROR")

        for field in dictionary.keys():
            if field in mapping.keys():
                module_logger.debug(f"Renaming {field} to {mapping[field]}")
                dictionary[mapping[field]] = dictionary[field]
                module_logger.debug(f"Deleting {field}")
                del dictionary[field]
        return dictionary

    def reindex_with_correct_mappings(self, MAPPING):
        """ Function to use incase mapping of index gets
        messed up.
        Creates a tempoarary index with a correct mapping,
            Reindexs the documents from the old index to temp one.
            Deletes the original index,
            Recreates the old index with strict mapping
            Reindexes temp index to old index
            Deletes temp index

            Needs some error checking"""
        temp_name = self.index + '_temp'
        module_logger.info(f"Creating index with name: {temp_name}")
        self.create_index(temp_name, MAPPING)
        module_logger.info(f"Reindexing {self.index} into {temp_name}")
        self.reindex(self.index, temp_name)
        module_logger.info(f"Deleting: {self.index}")
        self.delete_index(self.index)
        module_logger.info(f"Creating index {self.index} with correct mappings")
        self.create_index(self.index, MAPPING)
        module_logger.info(f"Reindexing {temp_name} into {self.index}")
        self.reindex(temp_name, self.index)
        module_logger.info(f"Deleting: {temp_name}")
        self.delete_index(temp_name)

    def get_hit_fields(self, es_resp_obj_dict):
        """Returns a list of they fields in an elasticsearch search response
            hit object."""
        return es_resp_obj_dict.keys()


if __name__ == '__main__':
    module_logger = logging.getLogger('elasticsearch_tools')
    module_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    module_logger.addHandler(ch)

    service = 'es'
    REGION = 'eu-west-1'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(credentials.access_key,
                       credentials.secret_key,
                       REGION,
                       service,
                       session_token=credentials.token)
    es = Elasticsearch('search-rightcall-445kqimzhyim4r44blgwlq532y.eu-west-1.es.amazonaws.com', "rightcall", "eu-west-1", auth=awsauth)
    mapping = {
        "mappings": {
            "_doc": {
                "properties": {
                    "referenceNumber": {"type": "keyword"},
                    "text": {"type": "text"},
                    "sentiment": {"type": "keyword"},
                    "promotion": {"type": "keyword"},
                    "entities": {"type": "keyword"},
                    "keyPhrases": {"type": "keyword"},
                    "date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                    "skill": {"type": "keyword"},
                    "length": {"type": "integer"}
                }
            }
        }
    }
    response = es.create_index('temp')

