import pandas as pd
from os import path
import boto3
import json

import os.path  


##    datapath = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/csvs/'
##    path_to_file = datapath + '20181107-161633.csv'
datapath = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/comprehend/'
DB_ENDPOINT = 'http://localhost:8000'
REGION = 'eu-central-1'
TABLE_NAME = 'Rightcall'
dynamodb = boto3.resource('dynamodb',
                          region_name=REGION,
                          endpoint_url=DB_ENDPOINT)

table = dynamodb.Table(TABLE_NAME)


def write_csv_to_db(csv_filepath, table):
    file = pd.read_csv(csv_filepath, sep=';')
    json_file = file.to_json(orient='records')
    json_obj = json.loads(json_file)
    print(len(json_obj))
    for call in json_obj:
        if check_exists(call['Name'], table):
            response = put_call(call, table)
        else:
            # update existing db 
            print(f"Item {call['Name']} already exists")


def put_call(call, table):
    try:
        response = table.put_item(Item={'referenceNumber': call['Name'],
                       'date': call['Date'],
                       'length': call['Length'],
                       'skill': call['Skill']})
    except Exception as e:
        raise e
    

def get_db_item(reference_number, table):
    try:
        response = table.get_item(Key={
            'referenceNumber': reference_number,
            })

    except ClientError as e:
            print(e.response['Error']['Message'])
    else:
        try:
            item = response['Item']
            print("Get Item succeeded!")
            return item
        except KeyError as k_err:
            print("Item not in db")

def check_exists(reference_number, table):
    try:
        response = table.get_item(Key={
            'referenceNumber': reference_number,
            })

    except ClientError as e:
            print(e.response['Error']['Message'])
    else:
        try:
            item = response['Item']
            return True
        except KeyError as k_err:
            print("Item not in db")
            return False


if __name__ == '__main__':
    for file in os.listdir(datapath):
        check_exists(file.split('--')[0], table)

    
##    response = write_csv_to_db(path_to_file, table)
##    response = get_db_item('b76993TOd10547', table)
##    from elasticsearch_upload import get_bucket_item
##    comp = get_bucket_item(response['referenceNumber'], 'comprehend.rightcall')
##    print(response)
    
