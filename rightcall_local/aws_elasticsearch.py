from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import configparser

path = '~/.aws/credentials'
config = configparser.ConfigParser()
config.read(path)


# REGION = 'eu-central-1'
# host = 'https://search-rightcall-kzg2r25sxih6fyibq4alvpvilm.eu-central-1.es.amazonaws.com'
# awsauth = AWS4Auth(YOUR_ACCESS_KEY, YOUR_SECRET_KEY, REGION, 'es')

# es = Elasticsearch(
#     hosts=[{'host': host, 'port': 443}],
#     http_auth=awsauth,
#     use_ssl=True,
#     verify_certs=True,
#     connection_class=RequestsHttpConnection
# )
# print(es.info())
