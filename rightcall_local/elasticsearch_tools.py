from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import boto3
import json
from os import path
import logging
##import sys
##sys.path.append('../')
import dynamodb_tools
module_logger = logging.getLogger('Rightcall.elasticsearch_tools')



directory = r'C:\Users\RSTAUNTO\Desktop\Python\projects\rightcall_robin'
with open(directory + '/data/elasticsearch/mapping.json', 'r') as file:
    MAPPING = json.load(file)
s3 = boto3.client('s3')

directory = path.dirname(__file__)

COMPREHEND = 'comprehend.rightcall'
REGION = 'eu-central-1'

DB_ENDPOINT = 'http://localhost:8000'
TABLE_NAME = 'Rightcall'

es = Elasticsearch([{'host': 'localhost',
                     'port': 9200}])
INDEX_NAME = 'rightcall'
TYPE_NAME = '_doc'

dynamodb = boto3.resource('dynamodb',
                          region_name=REGION,
                          endpoint_url = DB_ENDPOINT)
table = dynamodb.Table(TABLE_NAME)


def create_index(index_name, request_body):
    """Create an elasticsearch index with name <index_name>(str)
        and mappings/settings contained withing <request_body>(dict)
        if an index with that name doesn't already exist"""
    if es.indices.exists(INDEX_NAME):
        module_logger.info(f"Deleting {INDEX_NAME} index ...")
        res = es.indices.delete(index=INDEX_NAME)
        module_logger.info(f"Response: {res}")
    module_logger.info(f"Creating {INDEX_NAME} index...")
    res = es.indices.create(index=index_name, body=request_body, ignore=400)
    module_logger.info(f"Response: {res}")


def rename(dictionary):
    """Receives a dictionary and renames replaces any keys
    that also appear in the 'fields' dictionary with the appropriate
    value"""
    module_logger.debug(f"rename called with {dictionary.keys()}")
    fields = {'ref': 'referenceNumber',
              'reference_number': 'referenceNumber',
              'promo': 'promotion',
              'key_phrases': 'keyPhrases',
              'keyphrases': 'keyPhrases'}
    if type(dictionary) is not dict:
        module_logger.error("input is not dictionary. ERROR")
        raise TypeError
    
    for field in dictionary.keys():
        if field in fields.keys():
            module_logger.debug(f"Renaming {field} to {fields[field]}")
            dictionary[fields[field]]= dictionary[field]
            module_logger.debug(f"Deleting {field}")
            del dictionary[field]
    return dictionary

def load_call_record(db_item, s3_item, es, index, doctype='_doc'):
    """Takes two dictionaries (one from dynamo database record,
    the other an object from an s3 bucket and combines them into
    a single dictionary before indexing it to elasticsearch."""
    module_logger.debug(f"""load_call_record called with DB record: {db_item['referenceNumber']}, S3 Object:{s3_item['referenceNumber']} on {index}""")
    data = {**db_item, **s3_item}
    try:
        es.index(index=index, doc_type=doctype, id=data['referenceNumber'], body=data)
    except Exception as e:
        module_logger.error(e)
        raise e
    else:
        return True

def load_bucket_recordings(es, index, bucket, doctype='_doc'):
    """Retreive all files in 'comprehend.rightcall' bucket
        Query the local DynamoDB database with the reference number
        to retreive related data
        Add data to object downloaded from s3 and load into ES."""
    obs = s3.list_objects_v2(Bucket = bucket)
    for ob in obs['Contents']:
        ob_id = ob['Key']
        print(ob_id)
        resp = json.loads(
            s3.get_object(
                Bucket=bucket,
                Key= ob_id)['Body'].read().decode('utf-8'))
        s3_item = rename(resp)
        db_item = dynamodb_tools.get_db_item(ob_id, table)
        if db_item is not None:
            load_call_record(db_item, s3_item, es, index)
        else:
            print("That item doesn't exist in the database")
            print("Exiting")
            return False


def exists(es, index, referenceNumber):
    """Checks if a document exists within an elasticsearch index
    Returns boolean"""
    if '.json' in referenceNumber:
        raise ValueError(f'{referenceNumber} is wrong format: contains ".json"')
    
    result = es.exists(index, doc_type='_doc',id=referenceNumber)
    return result

def reindex(es, old_index, new_index):
    payload = {"source": {
                "index": old_index
              },
            "dest": {
                "index": new_index}}
    result = es.reindex(body=payload)
    return result

def delete_index(es, index_name):
    result = es.delete(index_name)
    return result

def search_by_ref(referenceNumber, es, index_name):
    """Searches index for a document with a referenceNumber field
    matching <refereceNumber>
    Returns a dictionary of hits"""
    if '.json' in referenceNumber:
        raise ValueError(f'{referenceNumber} is wrong format: contains ".json"')
    
    s = Search(using=es, index=index_name) \
        .query("match", referenceNumber=referenceNumber)
    
    response = s.execute()
    
    return response.to_dict()

def get_hit_fields(es_resp_obj_dict):
    """Returns a list of they fields in an elasticsearch search response
        hit object."""
    if es_resp_obj_dict['hits']['hits']:
        fields = [key for key in es_resp_obj_dict['hits']['hits'][0]['_source'].keys()]
    else:
        return []
    return fields

def get_mappings(es, index_name):
    # SMELLY CODE IN HERE - query is a field mapping in the es cluster but shouldn't be
    response = [field for field in es.indices.get_mapping(index_name)['rightcall'] \
                ['mappings']['_doc']['properties'] if field != 'query']
    return response       

def fully_populated_in_elasticsearch(referenceNumber, es, INDEX_NAME):
    """
    Checks if a given document present in a given
    index is missing its's metadata fields
    Returns boolean
    """

    # Query the index for the document associated with that reference number
    resp = search_by_ref(referenceNumber, es, INDEX_NAME)
    # Check what fields the doc has (all call metadata or not?)
    
    db_meta_data_fields = ['date', 'length', 'skill']
    es_fields = get_hit_fields(resp)
    
    if set(db_meta_data_fields).issubset(set(es_fields)):
        module_logger.debug(f"All: {db_meta_data_fields} contained in {es_fields}")
        return True
    else:
        module_logger.debug(f"""{referenceNumber} elasticsearch document missing
            one or more fields from {db_meta_data_fields}""")
        return False
            


if __name__ == '__main__':
##    resp = search_by_ref('b76993TOd10547', es, INDEX_NAME)
##    hits = get_hit_fields(resp)
##    print(hits)
    mappings = get_mappings(es, INDEX_NAME)
    print(set(mappings).issuperset(set(['date', 'length', 'skill'])))

    
