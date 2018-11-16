""" Reads a CSV file of call records, adds it to local
    DynamoDB, combines it with
    data from s3 if present and uploads to local Elasticsearch index
"""

import dynamodb_tools
import elasticsearch_tools
import s3 as s3py
import pandas as pd
import boto3
import json
import logging


LOGLEVEL = 'DEBUG'
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


# Logging
levels=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
if LOGLEVEL not in levels:
    raise ValueError(f"Invalid log level choice {LOGLEVEL}")
logger = logging.getLogger('Rightcall')
logger.setLevel(LOGLEVEL)
# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(LOGLEVEL)
# create formatter
formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)


def parse_csv(path_to_file):
    file = pd.read_csv(path_to_file, sep=';')
    json_file = file.to_json(orient='records')
    data = json.loads(json_file)
    return data

def RightcallLocal():
    json_data = parse_csv(CSV)
    logger.info(f"Num records to process: {len(json_data)}")
    logger.info(f"Getting objects from {BUCKET}")
    keys = s3.list_objects_v2(Bucket=BUCKET)
    logger.debug(f"Received {len(keys['Contents'])} objects from {BUCKET}")
    json_data = []
    for key in keys['Contents']:
        json_data.append({'Name': key['Key'].split('--')[0]})
    
    # For each record found in JSON file:
    for i, call_record in enumerate(json_data):
        logger.info('---------------------------------------')
        logger.info(f"Working on {i} : {call_record['Name']}")

        # Check if exists in elasticsearch already
        if elasticsearch_tools.exists(es, INDEX_NAME, call_record['Name']):
            logger.info(f"{call_record['Name']} already in {INDEX_NAME} index")
            logger.info(f"Deleting {call_record['Name']} from csv")
            json_data.remove(call_record)
            continue
        else:
            logger.info(f"{call_record['Name']} not in {INDEX_NAME} index")
        
        # Check if it exsits in db already
        exists = dynamodb_tools.check_exists(call_record['Name'], table)
        if not exists:
            logger.info(f"{call_record['Name']} not in {table} database. Adding...")
            # Upload it to DynamoDB 
            r = dynamodb_tools.put_call(call_record, table)
            if r['ResponseMetadata']['HTTPStatusCode'] == 200:
                logger.debug("Successful response from 'put_item' to database")
            else:
                logger.warning("Failed to put item in database")
        else:
            logger.info(f"{call_record['Name']} already in {table} database")
            logger.info("Skipping database add...")

        logger.info(f"Checking {BUCKET} bucket for {call_record['Name']}")
        s3_item = s3py.get_first_matching_item(call_record['Name'], BUCKET)
        if not s3_item:
            logger.warning(f"{call_record['Name']} not found in {BUCKET}")
            logger.info("Skipping...")
            continue
        else:
            logger.info(f"{call_record['Name']} in {BUCKET}")

        # Clean data before going to ES
        logger.info(f"cleaning data")
        s3_item = elasticsearch_tools.rename(s3_item)
        
        # Get item from dynamodb
        logger.info(f"Retreiving {call_record['Name']} from {table}")
        db_item = dynamodb_tools.get_db_item(call_record['Name'], table)
        
        # Upload to elasticsearch
        logger.info(f"Combining data for {call_record['Name']} and adding to {INDEX_NAME} index")
        result = elasticsearch_tools.load_call_record(
                db_item,
                s3_item,
                es,
                INDEX_NAME)
        if result:
            logger.info(f"{call_record['Name']} successfully added. Deleting from csv")
            json_data.remove(call_record)
        else:
            logger.error("Couldn't upload to elasticsearch")

    logger.info(f"Num records unsuccessfully processed: {len(json_data)}")
    df = pd.DataFrame.from_dict(json_data)
    logger.info(f"Writing remaining records back to csv")
    df.to_csv(RC_DIR + '/data/csvs/'+ 'to_download.csv', sep=';', index=False)

            


if __name__ == '__main__':
    data = RightcallLocal()
