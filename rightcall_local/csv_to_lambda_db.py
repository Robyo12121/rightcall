# import boto3
import os
import pandas as pd
import json
import odigo_robin
# import dynamodb_tools
from dotenv import load_dotenv
from pathlib import Path
import logging


# DB_ENDPOINT = 'http://localhost:8000'
# REGION = 'eu-west-1'
# TABLE_NAME = 'Rightcall'
# dynamodb = boto3.resource('dynamodb',
#                           region_name=REGION,
#                           endpoint_url=DB_ENDPOINT)

# table = dynamodb.Table(TABLE_NAME)

load_dotenv()
username = os.environ.get('PROSODIE_USERNAME')
passwd = os.environ.get('PROSODIE_PASSWORD')

# s = odigo_robin.setup()

# for record in records_list:
#     response = dynamodb_tools.put_call(record, table)

# odigo_robin.download_mp3_by_csv(s, username, passwd, path_to_file, rightcall_data + 'mp3s/')


def parse_csvs_from_path(path, target=None):
    """Given path to directory of csv files
    Retrieves records from each file and combines them
    INPUT: path to directory of csv files
    OUTPUT: list of dictionary records"""
    logging.debug(f"parse_csvs_from_path called with Path {path} and Target: {target}")
    csv_file_list = [item.name for item in list(path.glob('*.csv'))]
    logging.debug(f"Num Files: {len(csv_file_list)}")

    records = {"demo_metadata": []}
    for i, file in enumerate(csv_file_list):
        f = path / file
        logging.info(f"Working on {i}")
        logging.debug(f"Path: {f}")
        try:
            data = pd.read_csv(f, sep=';')
        except pd.errors.EmptyDataError as e:
            logging.warning(f"Error: {e}, File: {f}")
            continue
        record = json.loads(data.to_json(orient='records'))
        logging.debug(f"{record}")
        records['demo_metadata'].append(record[0])

    if target is None:
        pass
    elif target.suffix == '.csv':
        df = pd.DataFrame(records['demo_metadata'])
        df.to_csv(target, sep=';', index=False)
        logging.info(f"Writing records to :{target}")
    else:
        logging.warning(f"Unknown target: {target}")
    return records


def save_to_file(data, target_path):
    with open(target_path, 'w') as file:
        json.dump(data, file)


def save_to_csv(data, target_path):
    logging.debug(f"save_to_csv called with data: {data}, path: {target_path}")
    # record_dict = {k, v for item.items() in data['demo_metadata']}
    df = pd.DataFrame(data['demo_metadata'])
    df.to_csv(target_path, sep=';')


if __name__ == '__main__':
    demo_dir_path = Path(r'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/csvs/demo/')
    logging.basicConfig(level='DEBUG')
    records = parse_csvs_from_path(demo_dir_path, target=demo_dir_path.parent / 'metadata.csv')
    # logging.info(records)
    # target = demo_dir_path.parent / 'metadata.json'


    # csv_path = Path(r'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/csvs/metadata.json')
    # rightcall_data = Path(r'C:\Users\RSTAUNTO\Desktop\Python\projects\rightcall_robin\data\mp3s\demo')
    # s = odigo_robin.setup()
    # odigo_robin.download_mp3_by_csv(s, username, passwd, csv_path, rightcall_data)

    # save_to_csv(records, demo_dir_path.parent / 'demo_call')