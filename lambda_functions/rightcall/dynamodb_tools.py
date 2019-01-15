import boto3
from decimal import Decimal, getcontext
import logging

logging.basicConfig(level=logging.INFO)
TABLE_NAME = 'rightcall_metadata'
REGION = 'eu-west-1'
getcontext().prec = 2


def seconds2minutes(item):
    logging.info(f"{__name__}: {item}")
    if 'length' not in item.keys():
        raise AttributeError
    if type(item['length']) is not int:
        raise TypeError("length is not int")
    in_minutes = round(item['length'] / 60, 3)
    item['length'] = Decimal(str(in_minutes))
    logging.info(f"{__name__}: New Item: {item}")
    return item


class RightcallTable:
    def __init__(self, region, table_name):
        self.region = region
        self.dynamodb = boto3.resource('dynamodb',
                                       region_name=self.region)
        self.table_name = table_name
        self.table = self.dynamodb.Table(self.table_name)

    def batch_write(self, data):
        with self.table.batch_writer() as batch:
            for item in data:
                item = seconds2minutes(item)
                batch.put_item(Item=item)


if __name__ == '__main__':
    rtable = RightcallTable(REGION, TABLE_NAME)
    print(rtable.table.creation_date_time)
