import boto3
from decimal import Decimal
import logging

TABLE_NAME = 'rightcall_metadata'
REGION = 'eu-west-1'

logger = logging.getLogger(__name__)


class RightcallTable:
    def __init__(self, region, table_name):
        self.region = region
        self.dynamodb = boto3.resource('dynamodb',
                                       region_name=self.region)
        self.table_name = table_name
        self.table = self.dynamodb.Table(self.table_name)

    def __repr__(self):
        return f"rtable = dynamodb_tools.RightcallTable('{REGION}'', '{TABLE_NAME}')"

    def __str__(self):
        return self.table_name

    def sanitize_data(self, data):
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
        return clean_data

    def seconds2minutes(self, item):
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
        with self.table.batch_writer() as batch:
            failed_items = []
            for item in data:
                try:
                    item = self.sanitize_data(item)
                    item = self.seconds2minutes(item)
                    batch.put_item(Item=item)
                except Exception as e:
                    failed_items.append((item, e))
                    return failed_items

    def get_db_item(self, reference_number, check_exists=False):
        """Access item in database. Return it, unless check_exists flag is True"""
        if '.json' in reference_number:
            raise Exception('reference_number is wrong format: contains ".json"')
        logger.debug(f"get_db_item called with {reference_number}")
        try:
            response = self.table.get_item(Key={
                'referenceNumber': reference_number})
        except Exception as e:
                logger.error(f"Error: {e.response['Error']['Message']}")
        else:
            logger.debug(f"Item exists")
            if check_exists:
                    return
            try:
                item = response['Item']
                logger.debug(f"Successful. Returning {type(item)}")
                return item
            except KeyError:
                logger.error(f"Item not in db: {reference_number}")
                return False


if __name__ == '__main__':
    rtable = RightcallTable(REGION, TABLE_NAME)
    print(rtable.table.creation_date_time)
    print(rtable)
