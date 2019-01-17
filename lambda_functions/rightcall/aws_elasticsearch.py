from elasticsearch import Elasticsearch, RequestsHttpConnection
import requests
from requests_aws4auth import AWS4Auth
import boto3

REGION = 'eu-west-1'
es_host = 'search-rightcall-445kqimzhyim4r44blgwlq532y.eu-west-1.es.amazonaws.com/'
service = 'es'
# auth = AWSRequestsAuth(aws_host=es_host,
#                            aws_region=REGION,
#                            aws_service='es')
# AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
# AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')
creds = boto3.Session().get_credentials()

awsauth = AWS4Auth(creds.access_key, creds.secret_key, REGION, service)

# use the requests connection_class and pass in our custom auth class
es = Elasticsearch(
    hosts=[{'host': es_host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection)

doc = {"referenceNumber": "00000001"}

# res = es.index(index="rightcall", doc_type='_doc', id=2, body=doc)
res = es.exists("rightcall", doc_type="_doc", id="")
print(res['result'])

