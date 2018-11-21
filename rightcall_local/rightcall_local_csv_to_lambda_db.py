import os
import boto3
import pandas as pd
import json
import odigo_robin
import dynamodb_tools
from dotenv import load_dotenv

FILE = 'odigo4isRecorder_20181121-123619.csv'
rightcall_data = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/'
csvs = rightcall_data + 'csvs/'
path_to_file = csvs + FILE

DB_ENDPOINT = 'http://localhost:8000'
REGION = 'eu-central-1'
TABLE_NAME = 'Rightcall'
dynamodb = boto3.resource('dynamodb',
                          region_name=REGION,
                          endpoint_url=DB_ENDPOINT)

table = dynamodb.Table(TABLE_NAME)

load_dotenv()
username = os.environ.get('PROSODIE_USERNAME')
passwd = os.environ.get('PROSODIE_PASSWORD')
driver = r'C:\Users\RSTAUNTO\Desktop\chromedriver.exe'

s = odigo_robin.setup()

# 1. Read CSV file
file = pd.read_csv(path_to_file, sep=';')
records_list = json.loads(file.to_json(orient='records'))

# 2. For each ref in csv
for record in records_list:
    #   a. Download the mp3 from prosodie
    
    #   b. Add the metadata to dynamodb
    response = dynamodb_tools.put_call(record, table)

##odigo_robin.download_mp3_by_csv(s, username, passwd, path_to_file, rightcall_data + 'mp3s/')
# 5. When all mp3s are downloaded:
    # a. upload mp3s to s3
# 6. When all lambdas are finished:
    # a. Download and process files from comprehend.rightcall
