""" Reads a CSV file of call records, adds it to local
    DynamoDB, combines it with
    data from s3 if present and uploads to local Elasticsearch index
"""

import dynamodb_tools
import elasticsearch_tools
import odigo_robin
import s3 as s3py
import pandas as pd
import boto3
import json

RC_DIR = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/'
CSV_DIR = RC_DIR + 'data/csvs/'
MP3_DIR = RC_DIR + '/data/mp3s/'
CSV = CSV_DIR + '20181107-161633.csv'
REGION = 'eu-central-1'

DB_ENDPOINT = 'http://localhost:8000'
TABLE_NAME = 'Rightcall'

BUCKET = 'comprehend.rightcall'

INDEX_NAME = 'rightcall'
TYPE_NAME = '_doc'

dynamodb = boto3.resource('dynamodb',
                          region_name=REGION,
                          endpoint_url=DB_ENDPOINT)
s3 = boto3.client('s3')
es = elasticsearch_tools.Elasticsearch([{'host': 'localhost',
                     'port': 9200}])
table = dynamodb.Table(TABLE_NAME)

def parse_csv(path_to_file):
    file = pd.read_csv(path_to_file, sep=';')
    json_file = file.to_json(orient='records')
    data = json.loads(json_file)
    return data

def RightcallLocal():
    json_data = parse_csv(CSV)
    print(f"Num records to process: {len(json_data)}")
    keys = s3.list_objects_v2(Bucket=BUCKET)
    for call_record in json_data:
        dynamodb_tools.put_call(call_record, table)
        if call_record['Name'] in keys:
            # Download json from s3
            s3_item = rename(s3py.get_bucket_item(call_record['Name']))
            # Get item from dynamodb
            db_item = dynamodb_tools.get_db_item(call_record['Name'], table)
            # Upload to elasticsearch
            result = elasticsearch_tools.load_call_record(db_item, s3_item, es, INDEX_NAME)
            if result:
                del json_data[call_record]
            else:
                print("Couldn't upload to elasticsearch")
        else:
            print(f"No record with name {call_record['Name']}")
    print(f"Num records unsuccessfully processed: {len(json_data)}")
    df = pd.DataFrame.from_dict(json_data)
    df.to_csv('to_download.csv', sep=';', index=False)
    
    

            # output a file with reference numbers of calls to get with
            # odigo and upload to s3. 
            # download mp3 from prosodie
##            s, username, passwd, driver = odigo_robin.setup()
##            odigo.download_mp3_by_ref(s,
##                                      username,
##                                      passwd,
##                                      call_record['Name'],
##                                      path=MP3_DIR)
            # upload file to s3 bucket
            


if __name__ == '__main__':
    to_download = RightcallLocal()













if __name__ == '__main__':
    pass
