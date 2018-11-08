from elasticsearch import Elasticsearch
import boto3
import json
from os import path
import dynamodb_tools


def setup():
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
        print(f"Deleting {INDEX_NAME} index ...")
        res = es.indices.delete(index=INDEX_NAME)
        print(f"Response: {res}")
    print(f"Creating {INDEX_NAME} index...")
    res = es.indices.create(index=index_name, body=request_body, ignore=400)
    print(f"Response: {res}")


def rename(dictionary):
    fields = {'ref': 'referenceNumber',
              'reference_number': 'referenceNumber',
              'promo': 'promotion',
              'key_phrases': 'keyPhrases',
              'keyphrases': 'keyPhrases'}
    
    for field in dictionary.keys():
        if field in fields.keys():
            dictionary[fields[field]]= dictionary[field]
            del dictionary[field]
    return dictionary

def load_call_record(db_record, s3_item, es, index, doctype='_doc'):
    data = {**db_item, **s3_item}
    try:
        es.index(index=index, doc_type=doctype, id=ob_id.split('--')[0], body=data)
    except Exception as e:
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
##            data = {**db_item, **s3_item}
##            es.index(index=index, doc_type=doctype, id=ob_id.split('--')[0], body=data)
        else:
            print("That item doesn't exist in the database")
            print("Exiting")
            return False


if __name__ == '__main__':
    setup()
    if not es.indices.exists(INDEX_NAME):
        create_index(INDEX_NAME, MAPPING)
    load_recording(es, INDEX_NAME, COMPREHEND)

