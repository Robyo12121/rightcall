import os
import json
import logging
from odigo import search_by_range

username = os.environ.get('PROSODIE_USERNAME')
passwd = os.environ.get('PROSODIE_PASSWORD')

# Logging
logging.basicConfig()
logger = logging.getLogger()
if os.getenv('LOG_LEVEL') == 'DEBUG':
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """ New MP3 file uploaded to 'mp3.rightcall'
    Event sent to this lambda function from s3 bucket
    """
    logger.info("Received event: {}".format(str(json.dumps(event, indent=2))))
    response = {}
    return response
