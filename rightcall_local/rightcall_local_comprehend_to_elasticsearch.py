""" Ensures all objects in the comprehend.rightcall s3 bucket
    are added, along with their metadata to the local elasticsearch index.

    Metadata is stored in local dynamodb database.

    Flow:
        If ref from s3 object exists in elasticsearch index with all its meta data:
            Do Nothing
        If exists without metadata add ref to list csv file of refs for which metadata
            is needed.
        If doesn't exist in index, download it and try to get metadata
    
"""

import dynamodb_tools
import elasticsearch_tools
import s3 as s3py
import pandas as pd
import boto3
import json
import logging
import datetime


RC_DIR = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/'
CSV_DIR = RC_DIR + 'data/csvs/'
MP3_DIR = RC_DIR + '/data/mp3s/'
CSV = CSV_DIR + 'to_download.csv'
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

LOGLEVEL = 'DEBUG'

# Logging
levels=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
if LOGLEVEL not in levels:
    raise ValueError(f"Invalid log level choice {LOGLEVEL}")
logger = logging.getLogger('Rightcall')
logger.setLevel(LOGLEVEL)
# create console handler and set level to LOGLEVEL
ch = logging.StreamHandler()
ch.setLevel(LOGLEVEL)
# create file handler and set level to DEBUG
fh = logging.FileHandler('rightcall_local.log')
fh.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
fh.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)
logger.addHandler(fh)


def parse_csv(path_to_file):
    file = pd.read_csv(path_to_file, sep=';')
    json_file = file.to_json(orient='records')
    data = json.loads(json_file)
    return data

def write_to_csv(ref_list, path):
    logger.debug(ref_list)
    df = pd.DataFrame.from_dict({'col': ref_list})
    logger.debug(f"Writing {df} records back to csv")
    try:
        df.to_csv(path, sep=';', header=False,index=False)
    except PermissionError:
        logger.info(df)

def get_reference_number_from_object_name(object_name_string):
    """ Given s3 object name: 'e23413582523--QUIDP.json' or 'e23413582523P.json':
            return just 'e23413582523'
    """
    logger.debug(f"Received: {object_name_string}")
    if '--' in object_name_string:
        reference_number = object_name_string.split('--')[0]
    elif '.json' in object_name_string:
        reference_number =  object_name_string.split('.')[0]
    else:
        reference_number = object_name_string
    logger.debug(f"Ref Num: {reference_number}")
    if '--' in reference_number or '.json' in reference_number:
        raise ValueError(f"Invalid characters detected in reference number: {object_name_string}")
    return reference_number        
    

def get_all_refs_from_s3_objects(bucket_name):
    """Given an s3 bucket name, returns a list of the reference numbers
    contained in the names of all objects in that bucket
    
    Input: <string> 'comprehend.rightcall'
    Output: <list> ['b310f08130r3', 'c210935j22239', ...]
    """
    logger.info(f"Getting objects from {BUCKET}")
    keys = s3.list_objects_v2(Bucket=bucket_name)
    logger.debug(f"Received {len(keys['Contents'])} objects from {bucket_name}")
    list_of_reference_numbers = []
    for key in keys['Contents']:
        ref = get_reference_number_from_object_name(key['Key'])
        list_of_reference_numbers.append({'Name': ref})
    return list_of_reference_numbers

def RightcallLocal(json_data=None):
    """"""
    if json_data is None:
        json_data = []
    else:
        logger.info(f"Num records to process: {len(json_data)}")
        logger.error(f"json_data is not 'None': {json_data}")
        raise NotImplementedError

    refs = get_all_refs_from_s3_objects(BUCKET)
    get_meta_data = []

    # For each reference number:
    for i, call_record in enumerate(refs):
        s3_item = None
        db_item = None
        logger.debug('---------------------------------------')
        logger.debug(f"Working on {i} : {call_record['Name']}")
        
        ref = call_record['Name']
        # Exists in index?
        if elasticsearch_tools.exists(es, INDEX_NAME, ref):
            # In ES already
            logger.debug(f"{ref} already in {INDEX_NAME} index")               

        else:
            logger.debug(f"{ref} not in {INDEX_NAME} index")
            
            # Download from s3
            logger.debug(f"Checking {BUCKET} bucket for {call_record['Name']}")
            s3_item = s3py.get_first_matching_item(ref, BUCKET)
            
            # Prepare data for ES
            logger.debug(f"cleaning data")
            s3_item = elasticsearch_tools.rename(s3_item)

        # Check if fully populated with metadata
        if elasticsearch_tools.fully_populated_in_elasticsearch(ref, es, INDEX_NAME):
            logger.debug(f"{ref} fully populated in {INDEX_NAME}")
            continue
        else:
            logger.debug(f"{ref} missing metadata")
                   
        # Check if metadata exists in database
        logger.debug(f"Checking {table} database for missing metadata")
        db_item = dynamodb_tools.get_db_item(ref, table)
        if not db_item:
            # Add ref to csv
            logger.debug(f"Adding {ref} to 'get_meta_data'")
            get_meta_data.append(ref)
            continue
        else:
            logger.debug(f"Data present in {table} database: {db_item}")
              
        # Upload to elasticsearch
        
        if s3_item is None:
            logger.debug(f"Ensuring object is downloaded from {BUCKET}")
            s3_item = s3py.get_first_matching_item(ref, BUCKET)
            # Prepare data for ES
            logger.debug(f"cleaning data")
            s3_item = elasticsearch_tools.rename(s3_item)
            
        logger.debug(f"Combining data for {ref} from {table} and {BUCKET} and adding to {INDEX_NAME} index")

        result = elasticsearch_tools.load_call_record(
                db_item,
                s3_item,
                es,
                INDEX_NAME)
        if result:
            logger.debug(f"{ref} successfully added to {INDEX_NAME} index")
        else:
            logger.error(f"Couldn't upload to elasticsearch: {result}")

    logger.debug(f"Refs without metadata {get_meta_data}")
    return get_meta_data      


if __name__ == '__main__':
##    json_data = parse_csv(CSV)
    get_meta_data = RightcallLocal(None)
    write_to_csv(get_meta_data, RC_DIR + '/data/csvs/'+ 'to_download.csv')

