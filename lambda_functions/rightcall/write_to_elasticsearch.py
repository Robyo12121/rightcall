# Get data from comprehend.rightcall and from dynamodb database,
# combine it and load it into elasticsearch

import elasticsearch_tools
import s3 as s3py
import logging
import boto3
import dynamodb_tools
from requests_aws4auth import AWS4Auth

LOGLEVEL = 'DEBUG'
# Logging
levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
if LOGLEVEL not in levels:
    raise ValueError(f"Invalid log level choice {LOGLEVEL}")
logger = logging.getLogger('rightcall')
logger.setLevel(LOGLEVEL)
ch = logging.StreamHandler()
ch.setLevel(LOGLEVEL)
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def get_reference_number_from_object_name(object_name_string):
    """ Given s3 object name: 'e23413582523--QUIDP.json' or 'e23413582523P.json':
            return just 'e23413582523'
    """
    logger.debug(f"Received: {object_name_string}")
    if '--' in object_name_string:
        reference_number = object_name_string.split('--')[0]
    elif '.json' in object_name_string:
        reference_number = object_name_string.split('.')[0]
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
    logger.info(f"get_all_refs_from_s3_objects: Getting objects from {bucket_name}")
    keys = s3.list_objects_v2(Bucket=bucket_name)
    logger.info(f"Received {len(keys['Contents'])} objects from {bucket_name}")
    list_of_reference_numbers = []
    for key in keys['Contents']:
        ref = get_reference_number_from_object_name(key['Key'])
        list_of_reference_numbers.append({'Name': ref})
    return list_of_reference_numbers


def add_new_or_incomplete_items(bucket_name, es, table):
    """Ensures elasticsearch index has all the records that exist in comprehend.rightcall bucket
        and that they are fully populated with as much information as possible.
    Pulls objects down from comprehend.rightcall bucket.
        For each object:
            Checks if it exists in elasticsearch already.
            Checks if it has all the required fields populated with data.
            If so - moves on to next item
            If not - Checks if that missing data can be found in dynamodb
                if so - grabs it from dynamodb, combines it with s3 obeject data
                    and uploads to elasticsearch index
                if not - adds the filename (refNumber) to csv file to be returned.
    INPUTS:
        bucket_name (str) - name of bucket - json file source
        es (elasticsearch_tools.Elasticsearch object) - destination
        table (dynamodb_tools.RightcallTable object) - metadata source"""

    refs = get_all_refs_from_s3_objects(bucket_name)
    get_meta_data = []

    # For each reference number:
    for i, call_record in enumerate(refs):
        s3_item = None
        db_item = None
        logger.info('---------------------------------------')
        logger.info(f"Working on {i} : {call_record['Name']}")
        ref = call_record['Name']
        # Check if it is in the ES index
        logger.info(f"Checking if {ref} is in {es.index}")
        if es.exists(ref):
            logger.info(f"{ref} already in {es.index} index")
            logger.info(f"Checking if {ref} fully populated in {es.index} index")
            # Check if es document has all its fields
            if es.fully_populated_in_elasticsearch(ref):
                logger.info(f"{ref} fully populated in {es.index}")
                continue
            else:
                # Exists in ES but missing fields
                logger.info(f"{ref} missing metadata")

        # If not, get object from s3 in order to construct object to index
        else:
            logger.info(f"{ref} not in {es.index} index")

        logger.info(f"Checking {es.index} database for missing metadata")
        db_item = rtable.get_db_item(ref)
        if not db_item:
            logger.info(f"Adding {ref} to 'get_meta_data'")
            get_meta_data.append(ref)
            continue
        else:
            logger.info(f"Data present in {es.index} database: {db_item}")

        # Upload to elasticsearch
        if s3_item is None:
            logger.info(f"Ensuring object is downloaded from {bucket_name}")
            s3_item = s3py.get_first_matching_item(ref, bucket_name)
            logger.info(f"{ref} found in {bucket_name}")
            logger.info(f"Preparing data: old data: {s3_item.keys()}")
            s3_item = es.rename(s3_item)
            logger.info(f"Cleaned data: {s3_item.keys()}")
        logger.info(f"Combining data for {ref} from {rtable} and {bucket_name} and adding to {es.index} index")

        result = es.load_call_record(db_item, s3_item)
        if result:
            logger.info(f"{ref} successfully added to {es.index} index")
        else:
            logger.error(f"Couldn't upload to elasticsearch: {result}")

    return get_meta_data


if __name__ == '__main__':
    region = 'eu-west-1'
    dynamodb_table_name = 'rightcall_metadata'
    BUCKET = 'comprehend.rightcall'
    s3 = boto3.client('s3')
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        'es',
        session_token=credentials.token)
    es = elasticsearch_tools.Elasticsearch(
        'search-rightcall-445kqimzhyim4r44blgwlq532y.eu-west-1.es.amazonaws.com',
        region,
        index='rightcall',
        auth=awsauth)
    rtable = dynamodb_tools.RightcallTable(region, dynamodb_table_name)
    mapping = {
        "mappings": {
            "_doc": {
                "properties": {
                    "referenceNumber": {"type": "keyword"},
                    "text": {"type": "text"},
                    "sentiment": {"type": "keyword"},
                    "promotion": {"type": "keyword"},
                    "entities": {"type": "keyword"},
                    "keyPhrases": {"type": "keyword"},
                    "date": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                    "skill": {"type": "keyword"},
                    "length": {"type": "integer"}
                }
            }
        }
    }
    es.create_index('demo', mapping=mapping, set_as_current_index=True)
    calls_missing_meta_data = add_new_or_incomplete_items(BUCKET, es, rtable)
    logger.info(f"Refs without metadata {calls_missing_meta_data}")
