from elasticsearch import Elasticsearch
import boto3
import json

s3 = boto3.client('s3')

COMPREHEND = 'comprehend.rightcall'
es = Elasticsearch([{'host': 'localhost',
                     'port': 9200}])

def rename(dictionary):
    fields = {'ref': 'reference_number',
              'promo': 'promotion'}
    
    for field in dictionary.keys():
        if field in fields.keys():
            dictionary[fields[field]]= dictionary[field]
            del dictionary[field]
    return dictionary

def load_recording(es, index, doctype, bucket):                       
    obs = s3.list_objects_v2(Bucket = bucket)
    for ob in obs['Contents']:
        ob_id = ob['Key']
        data = json.loads(s3.get_object(Bucket=bucket, Key= ob_id)['Body'].read().decode('utf-8'))
        data = rename(data)
        es.index(index=index, doc_type=doctype, id=ob_id.split('--')[0], body=data) 

    
if __name__ == '__main__':
    load_recording(es, 'rightcall', 'recording', COMPREHEND)

