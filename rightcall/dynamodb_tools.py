import boto3
from decimal import Decimal
import logging
import json
import pandas as pd


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


class RightcallTable:
    def __init__(self,
                 region='eu-west-1',
                 table_name='rightcall',
                 endpoint=None):
        self.logger = logging.getLogger('rightcall.dynamodb_tools')
        self.region = region
        self.table_name = table_name
        self.endpoint = endpoint
        self.logger.debug(f"Initing with endpoint: {self.endpoint}")
        if region and table_name:
            if endpoint != 'None':
                self.dynamodb = boto3.resource('dynamodb', region_name=self.region, endpoint_url=self.endpoint)
            else:
                self.dynamodb = boto3.resource('dynamodb', region_name=self.region)
            self.table = self.dynamodb.Table(self.table_name)

    def __repr__(self):
        return f"rtable = dynamodb_tools.RightcallTable('{REGION}'', '{TABLE_NAME}')"

    def __str__(self):
        return f"RightcallTable - TABLE NAME: {self.table_name}, REGION: {self.region}, TABLE: {self.table}, ENDPOINT: {self.endpoint}"

    def sanitize_data(self, data):
        self.logger.debug(f"sanitize_data called with {data}")
        if type(data) is not dict:
            raise TypeError(f"Incorrect type. Function only accepts 'dict' objects \
                            Received: {type(data)}")
        if any(item not in data.keys() for item in ['Name', 'Skill', 'Length', 'Date']):
            raise Exception("Missing required Key")
        clean_data = {}
        clean_data['referenceNumber'] = data['Name']
        clean_data['skill'] = data['Skill']
        clean_data['length'] = data['Length']
        clean_data['date'] = data['Date']
        self.logger.debug(f"Clean data: {data}")
        return clean_data

    def seconds2minutes(self, item):
        self.logger.debug(f"Converting to minutes: {item}")
        if 'length' not in item.keys():
            raise KeyError("'length' key not found. Check case.")
        if type(item['length']) is not int:
            raise TypeError(f"Type 'int' expected for 'item['length']'. Received {type(item['length'])}")
        in_minutes = round(item['length'] / 60, 3)
        item['length'] = Decimal(str(in_minutes))
        return item

    def batch_write(self, data):
        """
        Input: List of dicts --eg:
         '[{"Name": "3c9153TVd10403", "Date": "2019-01-14 14:40:35", "Length": 220, "Skill": "ISRO_TEVA_US_EN"},
           {"Name": "3c90c3TVd10371", "Date": "2019-01-14 14:38:11", "Length": 155, "Skill": "ISRO_TEVA_US_EN"},"}]'
        """
        self.logger.debug(f"Batch write called with data: {data}")
        self.logger.info(f"Number of records: {len(data)}")
        clean_list = [self.sanitize_data(item) for item in data]
        self.logger.debug(f"Clean list: {clean_list}")
        mins_list = [self.seconds2minutes(item) for item in clean_list]
        self.logger.debug(f"Minute list: {mins_list}")
        failed_items = []
        with self.table.batch_writer() as batch:
            for i, item in enumerate(mins_list):
                self.logger.debug(f"Working on item: {i}")
                try:
                    batch.put_item(Item=item)
                except Exception as e:
                    failed_items.append((item, e))
                    self.logger.error(f"Failed to put item: {i}, {item}", exc_info=True)
        if not failed_items:
            return failed_items
        else:
            return 'success'

    def get_db_item(self, referenceNumber, check_exists=False):
        """Access item in database. Return it, unless check_exists flag is True"""
        if '.json' in referenceNumber:
            raise Exception('referenceNumber is wrong format: contains ".json"')
        self.logger.debug(f"get_db_item called with {referenceNumber}")
        try:
            response = self.table.get_item(Key={
                'referenceNumber': referenceNumber})

        except Exception as e:
                self.logger.error(f"Error: {e.response['Error']['Message']}", exc_info=True)
        else:
            self.logger.debug(f"DB response: {response}")
            item = response.get('Item')
            self.logger.debug(f"Item from response: {item}")
            self.logger.debug(f"Item is None: {item is None}")
            if item is None:
                self.logger.warning(f"{referenceNumber} not found!")
                return False
            if check_exists and item:
                self.logger.debug(f"Returning True")
                return True
            else:
                item['length'] = float(item['length'])
                self.logger.debug(f"Successful. Returning {type(item)}")
                return item

    def write_csv_to_db(self, csv):
        self.logger.debug(f"write csv to db called with: {csv}")
        file = pd.read_csv(csv, sep=';')
        records_list = json.loads(file.to_json(orient='records'))
        self.logger.debug(f"Retrieved records: {records_list}")
        result = self.batch_write(records_list)
        self.logger.debug(f"Result: {result}")


if __name__ == '__main__':
    logging.basicConfig()
    logger = logging.getLogger('rightcall')
    logger.setLevel('DEBUG')
    TABLE_NAME = 'rightcall_metadata'
    REGION = 'eu-west-1'
    rtable = RightcallTable(REGION, TABLE_NAME)
    # rtable.get_db_item('f7d183TVd10930', check_exists=True)
    # print(rtable.get_db_item('0153c7TVd10148', check_exists=False))
    from pathlib import Path
    csv_file = Path(r'C:\Users\RSTAUNTO\Desktop\Python\projects\rightcall_robin\data\csvs\odigo4isRecorder_20181123-112236.csv')
    rtable.write_csv_to_db(csv_file)
