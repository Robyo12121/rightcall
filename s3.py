#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from os import walk
from os.path import basename, exists, isfile, join, dirname
import sys
import logging
import boto3
import json
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

def get_bucket_item(partial_key, bucket_name):
    """Retreive json file from s3 given key or partial key"""
    module_logger.debug(f"get_bucket_item called with {partial_key}, {bucket_name}")
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
    path = dirname(__file__)
    cont_dir = 'mp3s'
    path = join(path, cont_dir).replace('\\', '/')
    upload_dir(path, bucket_name)
