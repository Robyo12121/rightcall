import requests
from requests_aws4auth import AWS4Auth
import boto3
import json
import logging
import decimal

# module_logger = logging.getLogger('rightcall.elasticsearch_tools')


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class CustomException(Exception):
    pass


class Elasticsearch:
    """Custom Elasticsearch to abstract away talking to elasticsearch
    domain. Uses HTTP requests."""
    def __init__(self,
                 host='search-rightcall-445kqimzhyim4r44blgwlq532y.eu-west-1.es.amazonaws.com',
                 region='eu-west-1',
                 index='rightcall',
                 auth=None):
        self.host = host
        self.region = region
        self.index = index
        if self.host is not None:
            self.base_url = 'https://' + self.host + '/'
        if self.host is not None and self.index is not None:
            self.index_url = self.base_url + self.index
        self.awsauth = auth
        self.data_fields = ['country', 'referenceNumber', 'date', 'sentiment', 'keyPhrases', 'promotion', 'text', 'length', 'entities']
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger('rightcall.dynamodb_tools')

    def __str__(self):
        return f"Host: {self.host}, Index: {self.index}, Region: {self.region}, Auth: {self.awsauth}"

    def make_request(self, method, url, data=None):
        self.logger.info(f"{self.make_request.__name__} called with {method}, {url}")
        self.logger.debug(f"...with data: {data}")
        headers = {"Content-Type": "application/json"}
        r = requests.request(method, url, auth=self.awsauth, headers=headers, data=json.dumps(data, cls=DecimalEncoder))
        response = r.json()
        self.logger.debug(f"Response: {response}")
        return response

    def put_item(self, doc_id, item):
        method = 'PUT'
        url = self.index_url + '/' + '_doc' + '/' + doc_id
        return self.make_request(method, url, item)

    def delete_item(self, doc_id, dryrun=False):
        method = 'DELETE'
        url = self.index_url + '/' + '_doc' + '/' + doc_id
        if dryrun:
            self.logger.warning(f"Delete Index:{doc_id}")
        else:
            return self.make_request(method, url)

    def get_item(self, doc_id):
        method = 'GET'
        url = self.index_url + '/' + '_doc' + '/' + doc_id
        return self.make_request(method, url)

    def update(self, doc_id, item):
        method = 'POST'
        url = self.index_url + '/' + '_doc' + '/' + doc_id + '/' + '_update'
        data = {'doc': {**item}}
        return self.make_request(method, url, data)

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
            self.logger.warning(f"Index {name} already exists. Aborting create index call...")
            if set_as_current_index:
                self.logger.warning(f"Current working index changed from {self.index} to {name}")
                self.index = name
                self.index_url = self.base_url + self.index
            else:
                self.logger.warning(f"Current working index remains: {self.index}")
                raise Exception("Working index did not change when requested")
            return

        self.logger.debug(f"Creating {name} index...")
        data = {}
        if mapping is not None:
            data = mapping
        response = self.make_request('PUT', url, data=data)
        self.logger.debug(response)
        if set_as_current_index and response['acknowledged']:
            self.logger.warning(f"Current working index changed from {self.index} to {name}")
            self.index = name
            self.index_url = self.base_url + self.index
        else:
            self.logger.warning(f"Current working index remains: {self.index}")
            raise Exception("Working index did not change when requested")
        return response

    def reindex(self, old_index_name, new_index_name):
        url = self.base_url + '_reindex'
        data = {}
        data['source'] = {'index': old_index_name}
        data['dest'] = {'index': new_index_name}
        response = self.make_request('POST', url, data=data)
        return response

    def delete_index(self, index_name, dryrun=True):
        url = self.base_url + index_name
        if dryrun:
            self.logger.info(f"Dryrun: Delete Index:{index_name}")
            return f"(dryrun): Delete Index:{index_name}"
        else:
            response = self.make_request('DELETE', url)
            return response

    def search(self, query, return_metadata=False):
        """Search index using query
        INPUT: expects elasticsearch query in full.
        OUTPUT: list of dicts each containing a search hit
        """
        url = self.index_url + '/_search'
        method = 'GET'
        results = self.make_request(method, url, query)
        if not return_metadata:
            results = [item['_source'] for item in results['hits']['hits']]
        return results

    def search_by_ref(self, referenceNumber):
        if '.json' in referenceNumber:
            self.logger.error(f"{self.search_by_ref.__name__}: '.json' found in {referenceNumber}")
            raise ValueError(f"{self.search_by_ref.__name__}: {referenceNumber} is wrong format: contains '.json'")
        query = {"query": {"match": {"referenceNumber": referenceNumber}}}
        self.logger.info(f"Query: {query}")
        hits = self.search(query)
        self.logger.debug(hits)
        if len(hits) > 1:
            self.logger.warning(f"{self.search_by_ref.__name__}: More than one hit found!")
        if not hits:
            self.logger.error(f"{self.search_by_ref.__name__}: No hits: {hits}")
            raise Exception(f"{self.search_by_ref.__name__}: Nothing found!")
        if type(hits[0]) is not dict:
            self.logger.error(f"Return value is not a dictionary!")
            raise ValueError(f"{self.search_by_ref.__name__}: Return value is not a dictionary!")
        return hits[0]

    def delete_by_query(self, query, dryrun=False):
        method = 'POST'
        url = self.index_url + '/' + '_delete_by_query'
        if dryrun:
            response = self.search(query)
            self.logger.info(f"Dryrun: Delete :{response}")
            return f"(dryrun): Delete {response}"
        else:
            response = self.make_request(method, url, query)
        return response

    def load_call_record(self, db_item, s3_item, dryrun=False):
        """Takes two dictionaries (one from dynamo database record,
        the other an object from an s3 bucket and combines them into
        a single dictionary before indexing it to elasticsearch."""
        data = {**db_item, **s3_item}
        data = self.sanitize(data)
        if dryrun:
            self.logger.debug(f"(dryrun) Loading {data} into index: {self.index}")
            return data
        try:
            self.put_item(data['referenceNumber'], data)
        except Exception as e:
            self.logger.error(e)
            raise e
        else:
            return True

    def exists(self, referenceNumber):
        """Checks if a document exists within an elasticsearch index
        Returns boolean"""
        if '.json' in referenceNumber:
            raise ValueError(f'{referenceNumber} is wrong format: contains ".json"')
        result = self.get_item(referenceNumber)
        self.logger.debug(f"Exists: {result}")
        if result['found']:
            self.logger.debug("exists: Returning True")
            return True
        else:
            self.logger.debug("exists: Returning False")
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
        if set(self.data_fields).issubset(set(resp.keys())):
            self.logger.debug(f"All fields present")
            return True
        else:
            self.logger.warning(f"Missing {set(self.data_fields).difference(set(resp.keys()))} from {self.data_fields}")
            return False

    def rename(self, dictionary):
        """Receives a dictionary and renames replaces any keys
        that also appear in the 'fields' dictionary with the appropriate
        value"""
        self.logger.debug(f"rename called with {dictionary.keys()}")
        mapping = {
            'ref': 'referenceNumber',
            'reference_number': 'referenceNumber',
            'promo': 'promotion',
            'key_phrases': 'keyPhrases',
            'keyphrases': 'keyPhrases'}

        if type(dictionary) is not dict:
            self.logger.error("input is not dictionary. ERROR")
            raise TypeError("input is not dictionary. ERROR")

        for field in dictionary.keys():
            if field in mapping.keys():
                self.logger.debug(f"Renaming {field} to {mapping[field]}")
                dictionary[mapping[field]] = dictionary[field]
                self.logger.debug(f"Deleting {field}")
                del dictionary[field]
        return dictionary

    def modify_by_search(self, query_dict, function, *args, dryrun=True):
        """
        INPUT: es - elasticsearch_tools.Elasticsearch() instance
            query_dict - dictionary containing elasticsearch query
            function - function to apply to each search result
                        (must return complete document to be reindexed)
            *args - any arguments to be passed to the function
        """
        results = self.search(query_dict)
        self.logger.info(f"Number of items retreived: {len(results)}")
        for i, item in enumerate(results):
            if not dryrun:
                self.logger.info(f"Working on {i}")
            else:
                self.logger.info(f"(dryrun) Working on {i}")
            try:
                item = function(item, *args)
            except KeyError as k_err:
                self.logger.error(f"KeyError: {k_err}, Item: {item}")
            except Exception as e:
                self.logger.error(f"Something went wrong with: Item: {item}, Error: {e}")
            if not dryrun:
                self.logger.warning(f"NOT DRYRUN. WRITING TO ELASTICSEARCH INDEX")
                self.put_item(item['referenceNumber'], item)

    def sanitize(self, item_dict):
        keys = item_dict.keys()
        # If skill is present, extract country from it
        if 'skill' in keys:
            item_dict = self.skill2country(item_dict)
        # Ensure all keys from datafields are present in keys
        for item in self.data_fields:
            if item not in keys:
                self.logger.error(f"item is missing key: {item}")
                raise CustomException(f"item is missing key: {item}")
        # Ensure there are no extra keys present in keys, not present in data_fields
        for item in keys:
            if item not in self.data_fields:
                self.logger.error(f"item contains unrecognised key: {item}")
        return item_dict

    def remove_keywords(self, somedict, keywords):
        count = 0
        kps = somedict['keyPhrases']
        for item in keywords:
            if item in kps:
                kps.remove(item)
                count += 1
        self.logger.info(f"Removed {count} items from {somedict['referenceNumber']}")
        somedict['keyPhrases'] = kps
        return somedict

    def skill2country(self, item_dict):
        """skill format: 'ISRO_TEVA_GB_EN'
           country format: 'GB'"""
        skill = item_dict['skill']
        country = skill.split('_')[2]
        self.logger.debug(f"Got {country} from {skill}. Adding to dictionary.")
        item_dict['country'] = country
        self.logger.debug(f"Deleting skill: {skill} from dictionary")
        del item_dict['skill']
        self.logger.debug(f"Returning modified dict: {item_dict}")
        return item_dict

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
        self.logger.info(f"Creating index with name: {temp_name}")
        self.create_index(temp_name, MAPPING)
        self.logger.info(f"Reindexing {self.index} into {temp_name}")
        self.reindex(self.index, temp_name)
        self.logger.info(f"Deleting: {self.index}")
        self.delete_index(self.index)
        self.logger.info(f"Creating index {self.index} with correct mappings")
        self.create_index(self.index, MAPPING)
        self.logger.info(f"Reindexing {temp_name} into {self.index}")
        self.reindex(temp_name, self.index)
        self.logger.info(f"Deleting: {temp_name}")
        self.delete_index(temp_name)


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
    # es = Elasticsearch('search-rightcall-445kqimzhyim4r44blgwlq532y.eu-west-1.es.amazonaws.com', "rightcall", "eu-west-1", auth=awsauth)
    es = Elasticsearch(None, None, None, None)
    print(es)
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
    # response = es.create_index('temp')
