#! /usr/bin/python
"""This script runs the 'Comprehend' portion of the lambda function.
    It processes transcript.json files in transcribe.rightcall s3 bucket
    into comprehend.rightcall bucket and stores results in comprehend.rightcall s3 bucket.
    Without having to actually re-run the transcribe job which takes
    a long time.
"""

import boto3
import json
import logging
import sys
import datetime

sys.path.append('../')
from lambda_functions.rightcall.lambda_function import Comprehend

logging.basicConfig()
logger = logging.getLogger('Comprehend_Only')
logger.setLevel(logging.DEBUG)

s3 = boto3.client('s3')

TRANSCRIBE_BUCKET = 'transcribe.rightcall'
COMPREHEND_BUCKET = 'comprehend.rightcall'


def get_bucket_item(partial_key, bucket_name):
    """Retreive json file from s3 given key or partial key"""
    keys = s3.list_objects_v2(Bucket=bucket_name)
    for key in keys['Contents']:
        if partial_key in key['Key']:
            match = key['Key']
            break
    else:
        return False
    resp = json.loads(s3.get_object(Bucket=bucket_name,
                                    Key=match)['Body'].read().decode('utf-8'))
    return resp

def main():
    keys = s3.list_objects_v2(Bucket=TRANSCRIBE_BUCKET)
    for key in keys['Contents']:
        event = {'detail': {'TranscriptionJobName': None}}
        if '.json' in key['Key'] and not key['Key'].startswith('.'):
            logger.debug(key['Key'])
            event['detail']['TranscriptionJobName'] = key['Key'].split('.')[0]
            logger.debug(event)
            Comprehend(event)

if __name__ == '__main__':
    main()
