from elasticsearch import Elasticsearch
import boto3
import json
from os import path
from .import dynamodb_tools
import logging

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
    if es.indices.exists(INDEX_NAME):
        module_logger.info(f"Deleting {INDEX_NAME} index ...")
        res = es.indices.delete(index=INDEX_NAME)
        module_logger.info(f"Response: {res}")
    module_logger.info(f"Creating {INDEX_NAME} index...")
    res = es.indices.create(index=index_name, body=request_body, ignore=400)
    module_logger.info(f"Response: {res}")


def rename(dictionary):
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

if __name__ == '__main__':
##    print(exists(es, INDEX_NAME, 'c4707fTOd00126'))
    oldINDEX_NAME = 'rightcall_plusplus'
    newINDEX_NAME = 'rightcall'
    if es.indices.exists(oldINDEX_NAME):
        res = delete_index(es, oldINDEX_NAME)
    print(res)
##    result = reindex(es, oldINDEX_NAME, newINDEX_NAME)
##    print(result)
            
##    load_recording(es, INDEX_NAME, COMPREHEND)

