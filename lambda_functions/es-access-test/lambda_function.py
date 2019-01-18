import os
import boto3
import json
import requests
from requests_aws4auth import AWS4Auth

REGION = os.environ.get('REGION')
service = 'es'
credentials = boto3.Session().get_credentials()
print(f"USING CREDENTIALS: ACCESS_KEY: {credentials.access_key} SECRET_KEY: {credentials.secret_key}")
awsauth = AWS4Auth(credentials.access_key,
                   credentials.secret_key,
                   REGION,
                   service,
                   session_token=credentials.token)

host = 'search-rightcall-445kqimzhyim4r44blgwlq532y.eu-west-1.es.amazonaws.com'
index = 'rightcall'
url = 'https://' + host + '/' + index + '/_search'


def lambda_handler(event, context):
    headers = {"Content-Type": "application/json"}
    r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(event['q']))
    response = {"statusCode": 200,
                "headers": {"Access-Control-Allow-Origin": '*'},
                "isBase64Encoded": False}
    response['body'] = r.text
    return response
