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

filename = 'metadata/metadata.json'

REGION = 'eu-west-1'

# Retrieve demo_metadata.json file from 'demo.rightcall/metadata' s3 bucket
# Upload each call record to dynamodb


def Metadata2Dynamodb(bucket, filename, tablename, region='eu-west-1'):
    """
    INPUTS:
    - bucket (str) : name of bucket in s3 - eg. 'demo.rightcall'
    - filename (str) : name of filepath/name within s3 - eg. 'metadata/metedata.json'
    - tablename (str) : name of dynamodb table withing aws - eg. 'rightcall_metadata'
    - region (str) : name of region in which dynamodb table is located eg. 'eu-west-1'
    """
    s3 = boto3.client('s3')
    data = s3.get_object(Bucket=bucket, Key=filename)['Body'].read().decode('utf-8')
    data = json.loads(data)
    rtable = dynamodb_tools.RightcallTable(region, tablename)
    failed_items = rtable.batch_write(data['demo_metadata'])
    if failed_items is None:
        response = {'success': True}
    else:
        response = {'success': False,
                    'response': failed_items}
    return response


if __name__ == '__main__':
    result = Metadata2Dynamodb(DEMO, filename, TABLE_NAME, region=REGION)
    print(result['success'])

