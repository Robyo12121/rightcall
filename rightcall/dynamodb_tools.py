import boto3
from decimal import Decimal
import logging


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

    def get_db_item(self, referenceNumber, check_exists=False):
        """Access item in database. Return it, unless check_exists flag is True"""
        if '.json' in referenceNumber:
            raise Exception('referenceNumber is wrong format: contains ".json"')
        self.logger.debug(f"get_db_item called with {referenceNumber}")
        try:
            response = self.table.get_item(Key={
                'referenceNumber': referenceNumber})

        except Exception as e:
                self.logger.error(f"Error: {e.response['Error']['Message']}")
        else:
            self.logger.debug(f"DB response: {response}")

            if check_exists and response.get('Item') is not None:
                    return True
            try:
                item = response['Item']
                self.logger.debug(f"Successful. Returning {type(item)}")
                return item
            except KeyError:
                self.logger.info(f"Item not in db: {referenceNumber}")
                return f"{referenceNumber} not found!"


if __name__ == '__main__':
    logging.basicConfig()
    logger = logging.getLogger('rightcall')
    logger.setLevel('DEBUG')
    TABLE_NAME = 'rightcall_metadata'
    REGION = 'eu-west-1'
    rtable = RightcallTable(REGION, TABLE_NAME)
    rtable.get_db_item('b76152TVd00246')
