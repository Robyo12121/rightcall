#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging
import boto3
import json

if __name__ == '__main__':
    logging.basicConfig()
    module_logger = logging.getLogger('S3')
    module_logger.setLevel(logging.DEBUG)
else:
    module_logger = logging.getLogger('Rightcall.s3')

bucket_name = 'mp3.rightcall'


def get_first_matching_item(partial_key, bucket_name):
    """Retreive json file from s3 given key or partial key"""
    module_logger.debug(f"get_bucket_item called with Key: {partial_key} on Bucket: {bucket_name}")
    s3 = boto3.client('s3')
    module_logger.debug(f"Getting list of items from {bucket_name}")
    keys = s3.list_objects_v2(Bucket=bucket_name)
    module_logger.debug(f"Received {type(keys)} from {bucket_name}")
    module_logger.debug(f"Looking for match of {partial_key} in {type(keys)}")
    for key in keys['Contents']:
        if partial_key in key['Key']:
            module_logger.debug(f"{partial_key} match found: {key['Key']}")
            match = key['Key']
            module_logger.debug("Breaking for loop")
            break
    else:
        module_logger.debug("No match found, returning False")
        return False
    module_logger.debug(f"Getting {match} from {bucket_name}")
    resp = json.loads(s3.get_object(Bucket=bucket_name,
                                    Key=match)['Body'].read().decode('utf-8'))
    module_logger.debug(f"Received {type(resp)} item, returning it")
    return resp


def remove_duplicates_keep_latest(bucket_name):
    """Items in some rightcall related buckets have name format:
        <reference-number>--<6digituniqeID>
        This function checks the entire bucket for items with the
        same reference number and removes all but one, keeping the
        one with the latest 'last modified' field.
    """
    s3 = boto3.client('s3')
    keys = s3.list_objects_v2(Bucket=bucket_name)['Contents']
    unique_dict = {}
    for key in keys:
        module_logger.debug("-------------------------")
        module_logger.debug(f"Working on {key['Key']}")
        ref = key['Key'].split('--')[0]
        module_logger.debug(f"{key['Key']} last modified on {key['LastModified']}")
        # If not encountered before
        if ref not in unique_dict:
            module_logger.debug(f"{ref} not encountered. Adding to record.")
            unique_dict[ref] = {'LastModified': key['LastModified']}

            if '--' in key['Key'] or '.json' not in key['Key']:
                module_logger.debug(f"{key['Key']} not desired format. Copying to {ref +'.json'}")
                s3.copy_object(Bucket=bucket_name,
                               CopySource={'Bucket': bucket_name, 'Key': key['Key']},
                               Key=ref + '.json')
                module_logger.debug(f"Deleting {key['Key']}")
                s3.delete_object(
                    Bucket=bucket_name,
                    Key=key['Key'])
            else:
                module_logger.debug(f"{key['Key']} already in correct format. No action needed.")

        # Ref already in dict
        else:
            module_logger.debug(f"{ref} already recorded. Checking if newer than current.")
            # If newer, replace it and copy whole object to that ref
            if unique_dict[ref]['LastModified'] < key['LastModified']:
                module_logger.debug(f"Newer version encountered. {key['LastModified']} newer than {unique_dict[ref]['LastModified']}")
                unique_dict[ref]['LastModified'] = key['LastModified']
                module_logger.debug(f"Copying {key['Key']} to {ref+'.json'}")
                s3.copy_object(Bucket=bucket_name,
                               CopySource={'Bucket': bucket_name, 'Key': key['Key']},
                               Key=ref + '.json')
            # Not newer than current version
            else:
                module_logger.debug(f"Current record is newer {unique_dict[ref]['LastModified']}.")

            module_logger.debug(f"Deleting {key['Key']}")
            s3.delete_object(
                Bucket=bucket_name,
                Key=key['Key'])

# Example. Upload '/tmp/example.mp3' file to Amazon S3 'examplebucket' bucket as
# 'example.mp3' file
# upload_file('/tmp/example.mp3', 'examplebucket')

# Example. Upload '/tmp/example.mp3' file to Amazon S3 'examplebucket' bucket as
# 'new.mp3' file
# upload_file('/tmp/example.mp3', 'examplebucket', 'new.mp3')

# Example. Upload '/tmp/' directory (recursively) to Amazon S3
# 'examplebucket' bucket
# upload_dir('/tmp/', 'examplebucket')


if __name__ == '__main__':
    remove_duplicates_keep_latest('transcribe.rightcall')
