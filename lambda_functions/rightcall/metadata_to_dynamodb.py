""" Retrieves demo call metadata and uploads it into dynamodb table
    Assumes metadata for demo files exists in 'demo.rightcall/metadata' s3 bucket location in JSON
    format:
        {
            "demo_metadata":
                {"referenceNumber": "3c9153TVd10403", "date": "2019-01-14 14:40:35", "length": 220, "skill": "ISRO_TEVA_US_EN"},
                {"referenceNumber": "3c90c3TVd10371", "date": "2019-01-14 14:38:11", "length": 155, "skill": "ISRO_TEVA_US_EN"},
                ...
        }
"""
import dynamodb_tools
import os
import boto3
import json
import logging
logging.basicConfig(level=logging.INFO)
if os.environ.get('AWS_EXECUTION_ENV') is not None:
    TABLE_NAME = os.environ.get('TABLE_NAME')
    DEMO = os.environ.get('DEMO')
else:
    DEMO = 'demo.rightcall'
    TABLE_NAME = 'rightcall_metadata'

REGION = 'eu-west-1'
s3 = boto3.client('s3')

# Retrieve demo_metadata.json file from 'demo.rightcall/metadata' s3 bucket
filename = 'metadata/metadata.json'
data = s3.get_object(Bucket=DEMO, Key=filename)['Body'].read().decode('utf-8')
# logging.info(data)
# logging.info(type(data))

data = json.loads(data)
logging.info(data)
# Upload each call record to dynamodb
logging.info(f"Type data: {type(data)}")
logging.info(f"Keys: {data.keys()}")
logging.info(data['demo_metadata'])

for i, item in enumerate(data['demo_metadata']):
    logging.info(f"Item {i}: {item}")

rtable = dynamodb_tools.RightcallTable(REGION, TABLE_NAME)
rtable.batch_write(data['demo_metadata'])
