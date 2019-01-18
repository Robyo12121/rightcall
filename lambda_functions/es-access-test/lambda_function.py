import boto3
import json
import requests
from requests_aws4auth import AWS4Auth


REGION = 'eu-west-1'
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key,
                   credentials.secret_key,
                   REGION,
                   service,
                   session_token=credentials.token)

host = 'search-rightcall-445kqimzhyim4r44blgwlq532y.eu-west-1.es.amazonaws.com'
index = 'rightcall'
base_url = 'https://' + host + '/' + index
search_url = 'https://' + host + '/' + index + '/_search'


def put_item(item, doc_id):
    method = 'PUT'
    url = base_url + '/' + '_doc' + '/' + doc_id
    print(url)
    return make_request(method, url, item)


def get_item(doc_id):
    method = 'GET'
    url = base_url + '/' + '_doc' + '/' + doc_id
    print(url)
    return make_request(method, url)


def search(query):
    url = search_url
    method = 'GET'
    results = json.loads(make_request(method, url, query)['body'])
    hits = [item['_source'] for item in results['hits']['hits']]
    return hits


def make_request(method, url, data=None):
    headers = {"Content-Type": "application/json"}
    r = requests.request(method, url, auth=awsauth, headers=headers, data=json.dumps(data))
    response = {"statusCode": 200,
                "headers": {"Access-Control-Allow-Origin": '*'},
                "isBase64Encoded": False}
    response['body'] = r.text
    return response


def lambda_handler(event, context):
    headers = {"Content-Type": "application/json"}
    r = requests.get(search_url, auth=awsauth, headers=headers, data=json.dumps(event['q']))
    response = {"statusCode": 200,
                "headers": {"Access-Control-Allow-Origin": '*'},
                "isBase64Encoded": False}
    response['body'] = r.text
    return response


if __name__ == '__main__':
    doc = {
        "referenceNumber": "c46e2bTOd10513",
        "text": "Thank you for calling triple services.",
        "sentiment": "neutral",
        "promotion": "none",
        "entities": [
            "one month",
            "one second",
            "Today"
        ],
        "keyphrases": [
            "triple services",
            "My name",
            "your last name",
            "my name"
        ]
    }
    # print(put_item(doc, doc['referenceNumber']))
    # print(get_item("c46e2bTOd10513"))
    print(search({"query": {"match_all": {}}}))
