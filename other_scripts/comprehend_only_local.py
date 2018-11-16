# Goal, overwrite files in elasticsearch with new ones from comprehend
# As they have new keyphrases and entities
import sys
import boto3
import logging
sys.path.append('../')
from rightcall_local import elasticsearch_tools
from rightcall_local import dynamodb_tools
from rightcall_local import s3 as s3py

logging.basicConfig()
logger = logging.getLogger('Comprehend_Only')
logger.setLevel(logging.DEBUG)

s3 = boto3.client('s3')
es = elasticsearch_tools.Elasticsearch([{'host': 'localhost',
                     'port': 9200}])
REGION = 'eu-central-1'
DB_ENDPOINT = 'http://localhost:8000'
TABLE_NAME = 'Rightcall'
dynamodb = boto3.resource('dynamodb',
                          region_name=REGION,
                          endpoint_url=DB_ENDPOINT)

COMPREHEND_BUCKET = 'comprehend.rightcall'
INDEX_NAME = 'rightcall'
TYPE_NAME = '_doc'
table = dynamodb.Table(TABLE_NAME)


# download all files from comprehend s3 bucket
keys = s3.list_objects_v2(Bucket=COMPREHEND_BUCKET)['Contents']
# For each item
for key in keys:
    logger.debug(f"Key = {key['Key']}")
    if '--' in key['Key']:
        ref = key['Key'].split('--')[0]
    else:
        ref = key['Key'].split('.')[0]
    logger.debug(f"Working on {key['Key']}")
    logger.debug(f"Ref: {ref}")
    # Download item from s3
    logger.debug(f"Downloading {key['Key']} from {COMPREHEND_BUCKET}")
    s3_item = s3py.get_first_matching_item(ref, COMPREHEND_BUCKET)
    # Clean data before going to ES
    logger.debug(f"Ensuring data in correct format")    
    s3_item = elasticsearch_tools.rename(s3_item)
    
    # Try to get corresponding item from database
    logger.debug(f"Trying to get {ref} from database")
    db_item = dynamodb_tools.get_db_item(ref, table)
    logger.debug(f"Got {db_item} from database")
    if db_item is None:
        continue
##        db_item = {'referenceNumber': ref}
    # Combine items and upload to ES (gets replaced if already exists)
    logger.debug(f"Combining database item: {db_item} and S3 Item: {s3_item} into {INDEX_NAME}")    
    result = elasticsearch_tools.load_call_record(db_item, s3_item, es, INDEX_NAME)
    if result:
        print("Success")
        
    

     
    

        
