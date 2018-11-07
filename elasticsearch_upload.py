from elasticsearch import Elasticsearch
import boto3
import json
from os import path
s3 = boto3.client('s3')

directory = path.dirname(__file__)

COMPREHEND = 'comprehend.rightcall'
es = Elasticsearch([{'host': 'localhost',
                     'port': 9200}])

with open(directory + '/data/elasticsearch/mapping.json', 'r') as file:
    MAPPING = json.load(file)

INDEX_NAME = 'rightcall'
TYPE_NAME = '_doc'


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

def load_recording(es, index, bucket, doctype='_doc', ):                       
    obs = s3.list_objects_v2(Bucket = bucket)
    for ob in obs['Contents']:
        ob_id = ob['Key']
        print(ob_id)
        data = json.loads(s3.get_object(Bucket=bucket, Key= ob_id)['Body'].read().decode('utf-8'))
        data = rename(data)
        es.index(index=index, doc_type=doctype, id=ob_id.split('--')[0], body=data) 

def search(es, index, num_results, 

if __name__ == '__main__':
    if not es.indices.exists(INDEX_NAME):
        create_index(INDEX_NAME, MAPPING)
    load_recording(es, INDEX_NAME, COMPREHEND)

