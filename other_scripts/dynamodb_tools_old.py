#! /usr/bin/python
import pandas as pd
import boto3
import json
import logging
from decimal import Decimal
module_logger = logging.getLogger(__name__)


datapath = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/csvs/'
path_to_file = datapath + 'odigo4isRecorder_20181121-123619.csv'
# datapath = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/comprehend/'
DB_ENDPOINT = 'http://localhost:8000'
REGION = 'eu-central-1'
TABLE_NAME = 'Rightcall'
dynamodb = boto3.resource('dynamodb',
                          region_name=REGION,
                          endpoint_url=DB_ENDPOINT)

table = dynamodb.Table(TABLE_NAME)


def write_csv_to_db(csv_filepath, table):
    file = pd.read_csv(csv_filepath, sep=';')
    records_list = json.loads(file.to_json(orient='records'))
    print(len(records_list))
    for call in records_list:
        if not get_db_item(call['Name'], table, check_exists=True):
            put_call(call, table)
        else:
            # update existing db
            print(f"Item {call['Name']} already exists")


def put_call(call, table, minutes=False):
    """Put a call item (call metadata) into dynamodb database
    INPUTS: call - dictionary containing call meta data info including, at minimum:
        'Name', 'Date', 'Length', 'Skill' fields
            table - the dynamodb table object to put data into
            minutes - A flag which, if True, will convert the 'Length' filed from seconds into minutes"""
    module_logger.debug(f"put_call called with {call['Name']} on {table}")
    try:
        response = table.put_item(Item={'referenceNumber': call['Name'],
                                        'date': call['Date'],
                                        'length': Decimal(call['Length'] / 60) if minutes else Decimal(call['Length']),
                                        'skill': call['Skill']})
    except Exception as e:
        raise e
    else:
        return response


def get_db_item(reference_number, table, check_exists=False):
    if '.json' in reference_number:
        raise Exception('reference_number is wrong format: contains ".json"')
    module_logger.debug(f"get_db_item called with {reference_number} on {table}")
    try:
        response = table.get_item(Key={
            'referenceNumber': reference_number})

    except Exception as e:
            module_logger.error(f"Error: {e.response['Error']['Message']}")
    else:
        try:
            item = response['Item']
            module_logger.debug(f"Item exists")
            if check_exists:
                return
            else:
                module_logger.debug(f"Successful. Returning {type(item)}")
                return item
        except KeyError:
            module_logger.error(f"Item not in db: {reference_number}")
            return False


if __name__ == '__main__':
    # item = get_db_item('b76993TOd10547', table)
    # print(item)
    # response = table.put_item(Item=item)
    # for file in os.listdir(datapath):
    #     check_exists(file.split('--')[0], table)

    response = write_csv_to_db(path_to_file, table)
#    response = get_db_item('b76993TOd10547', table)
#    from elasticsearch_upload import get_bucket_item
#    comp = get_bucket_item(response['referenceNumber'], 'comprehend.rightcall')
#    print(response)
