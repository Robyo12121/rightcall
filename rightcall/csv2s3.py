import boto3
import pandas as pd
import json


def csv2s3(csv_path, bucket_name):
    file = pd.read_csv(csv_path, sep=';')
    records_list = {}
    records_list['demo_metadata'] = json.loads(file.to_json(orient='records'))
    metadata = json.dumps(records_list)
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    bucket.put_object(Key='metadata/metadata.json', Body=metadata)


if __name__ == '__main__':
    csv2s3(r"C:\Users\RSTAUNTO\Desktop\Python\projects\rightcall_robin\data\csvs\odigo4isRecorder_20181121-123619.csv",
           "demo.rightcall")
