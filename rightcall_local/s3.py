#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from os import walk
from os.path import basename, exists, isfile, join, dirname
import sys
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

def upload_dir(dir_abs_path, bucket_name):
    """Upload all files from directory (recursively) to Amazon S3 bucket.
    Input:
        dir_abs_path -- directory absolute path (required | type: str).
                        Example: '/tmp/' or '/tmp';
        bucket_name -- Amazon S3 bucket name (required | type: str).

    """

    if not dir_abs_path.endswith('/'):
        dir_abs_path += '/'
    length = sum([len(fn) for dp, dn, fn in walk(dir_abs_path)])
    output = walk(dir_abs_path, topdown=True, onerror=None, followlinks=False)
    i = 0
    for dir_path, dir_names, file_names in output:
       for file_name in file_names:
           if not file_name.strip().endswith('~'):
               file_abs_path = join(dir_path, file_name)
               key_name = join(dir_path.replace(dir_abs_path, ''), file_name)
               i += 1
               sys.stdout.write('\r')
               sys.stdout.write('uploading: %s/%s' % (i, length))
               sys.stdout.flush()
               upload_file(file_abs_path, bucket_name, key_name)
    sys.stdout.write('\n')
    sys.stdout.flush()

def upload_file(file_abs_path, bucket_name, key_name=None):
    """Upload file to Amazon S3 bucket. If no `key_name`, file name used as
       `key_name` (example: `file_abs_path` is '/tmp/example.mp3' and `key_name`
       is None, that `key_name` is 'example.mp3').
    Input:
        file_abs_path -- file abs path (required | type: str);
        bucket_name -- Amazon S3 bucket name (required | type: str);
        key_name -- Amazon S3 bucket dst file abs path (not required |
                    type: str).

    """
    
    if not key_name:
        key_name = basename(file_abs_path)
    # Let's use Amazon S3
    s3 = boto3.client('s3')
    if exists(file_abs_path) and isfile(file_abs_path):
        # Upload file to Amazon S3 bucket
        try:
            s3.upload_file(file_abs_path, bucket_name, key_name)
        except Exception as exception:
            raise exception
    else:
        return False

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

def remove_duplicates(bucket_name):
    """Items in some rightcall related buckets have name format:
        <reference-number>--<6digituniqeID>
        This function checks the entire bucket for items with the
        same reference number and removes all but one.

        This should be modified to keep the latest object
    """
    s3 = boto3.client('s3')
    # key all keys in bucket
    keys = s3.list_objects_v2(Bucket=bucket_name)['Contents']
    bucket_keys = {key['Key'] for key in keys}
    ref_set = set()
    module_logger.debug(ref_set)
    # for each item in bucket:
    for key in keys:
        # get ref number
        ref = key['Key'].split('--')[0]
        module_logger.debug(f"Working on {key['Key']}")
        # If not in set:
        if ref not in ref_set:
            module_logger.debug(f"{ref} not in set, adding.")
            # add it
            ref_set.add(ref)
            # If it's not already a file in the bucket
            if ref not in bucket_keys:
                # copy object to new object named just with ref number
                module_logger.debug(f"Copying {key['Key']} to {ref}")
                s3.copy_object(Bucket=bucket_name,
                               CopySource={'Bucket': bucket_name, 'Key': key['Key']},
                               Key=ref+'.json')
            else:
                module_logger.debug(f"{ref} in bucket already")
        # IF in set already:
        else:
            module_logger.debug(f"{ref} in set already")
            # Delete it
            module_logger.debug(f"Deleting {key['Key']}")
            response = s3.delete_object(
                Bucket=bucket_name,
                Key=key['Key'])

def remove_all_but_latest(bucket_name):
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
            unique_dict[ref] = {'LastModified':key['LastModified']}
            
            if '--' in key['Key'] or '.json' not in key['Key']:
                module_logger.debug(f"{key['Key']} not desired format. Copying to {ref +'.json'}")
                s3.copy_object(Bucket=bucket_name,
                               CopySource={'Bucket': bucket_name, 'Key': key['Key']},
                               Key=ref+'.json')
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
                               Key=ref+'.json')
            # Not newer than current version
            else:
                module_logger.debug(f"Current record is newer {unique_dict[ref]['LastModified']}.")
                module_logger.debug(f"Deleting {key['Key']}")
                response = s3.delete_object(
                        Bucket=bucket_name,
                        Key=key['Key'])
                    
# Example. Upload '/tmp/example.mp3' file to Amazon S3 'examplebucket' bucket as
# 'example.mp3' file
#upload_file('/tmp/example.mp3', 'examplebucket')

# Example. Upload '/tmp/example.mp3' file to Amazon S3 'examplebucket' bucket as
# 'new.mp3' file
#upload_file('/tmp/example.mp3', 'examplebucket', 'new.mp3')

# Example. Upload '/tmp/' directory (recursively) to Amazon S3
# 'examplebucket' bucket
#upload_dir('/tmp/', 'examplebucket')

if __name__ == '__main__':
    remove_all_but_latest('comprehend.rightcall')
