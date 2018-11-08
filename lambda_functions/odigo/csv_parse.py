import pandas as pd
from os import path
import boto3
import json

datapath = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/csvs/'
path_to_file = datapath + '20181107-161633.csv'

dynamodb = boto3.resource('dynamodb', region_name='eu-central-1', endpoint_url = 'http://localhost:8001')

table = dynamodb.Table('Rightcall')


def write_csv_to_db(csv_filepath, table):
    file = pd.read_csv(csv_filepath, sep=';')
    json_file = file.to_json(orient='records')
    json_obj = json.loads(json_file)
    print(len(json_obj))
    for call in json_obj:
        if check_exists(call['Name'], table):
            response = put_call(call, table)
        else:
            print(f"Item {call['Name']} already exists")


def put_call(call, table):
    try:
        response = table.put_item(Item={'referenceNumber': call['Name'],
                       'date': call['Date'],
                       'length': call['Length'],
                       'skill': call['Skill']})
    except Exception as e:
        raise e
    

def get_item(reference_number, table):
    try:
        response = table.get_item(Key={
            'referenceNumber': reference_number,
            })

    except ClientError as e:
            print(e.response['Error']['Message'])
    else:
        try:
            item = response['Item']
            print("Get Item succeeded!")
            return item
        except KeyError as k_err:
            print("Item not in db")

def check_exists(reference_number, table):
    try:
        response = table.get_item(Key={
            'referenceNumber': reference_number,
            })

    except ClientError as e:
            print(e.response['Error']['Message'])
    else:
        try:
            item = response['Item']
            return True
        except KeyError as k_err:
            print("Item not in db")
            return False

# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if abs(o) % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)

if __name__ == '__main__':
##    response = write_csv_to_db(path_to_file, table)
    print(get_item('e2ff56TVd10422', table))
