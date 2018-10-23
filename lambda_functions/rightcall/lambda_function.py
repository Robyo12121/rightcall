from urllib.parse import unquote_plus
import json
import boto3
import os
import logging

from transcribe import transcribe_mp3
from comprehend import get_sentiment
from text import check_promo

# Logging
logging.basicConfig()
logger = logging.getLogger()
if os.getenv('LOG_LEVEL') == 'DEBUG':
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


class ThrottlingException(Exception):
    pass

# TRANSCRIPTS = os.environ.get('TRANSCRIPTS')
# MP3S = os.environ.get('MP3S')
# COMPREHEND = os.environ.get('COMPREHEND')


TRANSCRIPTS = 'transcribe.rightcall'
MP3S = 'mp3.rightcall'
COMPREHEND = 'comprehend.rightcall'
print(MP3S, TRANSCRIPTS, COMPREHEND)


def Transcribe(event):
    """
    S3 object created event received
    Get URI of file and send it to transcribe
    """
    isSuccessful = False
    bucket = event['Records'][0]['s3']['bucket']
    logger.info("Bucket Event: {}".format(str(bucket['name'])))
    if bucket['name'] == MP3S:
        key = unquote_plus(
            event['Records'][0]['s3']['object']['key'],
            encoding='utf-8')
        logger.info("Bucket: {}, Key: {}".format(
            str(bucket['name']),
            str(key)))
        uri = 'https://s3-' + event['Records'][0]['awsRegion'] + \
            '.amazonaws.com/' + bucket['name'] + '/' + key
        logger.info("URI: {}".format(str(uri)))
    else:
        logger.info("Wrong Bucket")
    try:
        response = transcribe_mp3(uri, TRANSCRIPTS)
        logger.info(response)
        job = response['TranscriptionJob']['TranscriptionJobName']

    except Exception as e:
        logger.error(str(e))
        raise e
    else:
        isSuccessful = True
        return {'success': isSuccessful,
                'job_name': job}


def Comprehend(event):
    """
    Cloudwatch 'Transcribe finished' event received.
    Go to destination bucket and get <job name>.json
    """
    # Fetch json file related to completed transcribe event from DESTINATION
    # s3 bucket
    s3 = boto3.client('s3')
    filename = event['detail']['TranscriptionJobName'] + '.json'
    logger.info("Filename: {}".format(str(filename)))
    try:
        data = s3.get_object(Bucket=TRANSCRIPTS, Key=filename)
        data = data['Body'].read().decode('utf-8')
    except Exception as e:
        logger.error(str(e))
        raise e

    try:
        data = json.loads(data)
    except Exception as e:
        logger.error(str(e))
        raise e

    logger.info(str(data.keys()))

    # Give the transcript text to comprehend.py
    try:
        text = data['results']['transcripts'][0]['transcript']
    except Exception as e:
        logger.error(str(e))
        raise e

    r = {}
    r['ref'] = event['detail']['TranscriptionJobName'].split('--')[0]
    r['text'] = text

    # Get sentiment using AWS Comprehend
    sentiment = get_sentiment(text)
    r['sentiment'] = sentiment
    logger.info("Sentiment: {}".format(str(r['sentiment'])))
    # Check promotion
    promo = check_promo(text)
    r['promo'] = promo
    logger.info("r promo: {}".format(str(r['promo'])))
    # Save to json file in 'comprehend.rightcall' bucket
    try:
        response = s3.put_object(Body=json.dumps(r, indent=2),
                                 Bucket=COMPREHEND,
                                 Key=event['detail']['TranscriptionJobName'])
    except Exception as e:
        logger.error(str(e))
        raise e
    else:
        return response


def event_type_transcribe_job_status(event):
    """Check if event is from Cloudwatch
        If 'aws.transcribe' event from Cloudwatch return True
        Else return False
    """
    if 'source' in event and event['source'] == 'aws.transcribe':
        logger.info("Job: {} Status: {}".format(
            str(event['detail']['TranscriptionJobName']),
            str(event['detail']['TranscriptionJobStatus'])))
        return True
    else:
        return False


def Rightcall(event):
    """Determine event type (S3 or Cloudwatch) and
    take appropriate action"""
    response = {}
    if event_type_transcribe_job_status(event):
        response = Comprehend(event)
    else:
        logger.info("New mp3 in bucket. Sending to Transcribe")
        response = Transcribe(event)
    return response


def lambda_handler(event, context):
    """ New MP3 file uploaded to 'mp3.rightcall'
    Event sent to this lambda function from s3 bucket
    """
    logger.info("Received event: {}".format(str(json.dumps(event, indent=2))))
    response = Rightcall(event)
    return response


if __name__ == '__main__':
    transcribe_job_status_event = {
       "version": "0",
       "id": "event ID",
       "detail-type": "Transcribe Job State Change",
       "source": "aws.transcribe",
       "account": "account ID",
       "time": "timestamp",
       "region": "region",
       "resources": [],
       "detail": {
         "TranscriptionJobName": "b76152TVd00246--3bc99ed9-e035-4316-9a05",
         "TranscriptionJobStatus": "COMPLETE"
       }
     }
    s3_new_object_event = {
               "Records": [
                  {
                     "eventVersion": "2.0",
                     "eventSource": "aws:s3",
                     "awsRegion": "eu-west-1",
                     "eventTime": "1970-01-01T00:00:00.000Z",
                     "eventName": "ObjectCreated:Put",
                     "userIdentity": {
                        "principalId": "AIDAJDPLRKLG7UEXAMPLE"
                     },
                     "requestParameters": {
                        "sourceIPAddress": "127.0.0.1"
                     },
                     "responseElements": {
                        "x-amz-request-id": "C3D13FE58DE4C810",
                     },
                     "s3": {
                        "s3SchemaVersion": "1.0",
                        "configurationId": "testConfigRule",
                        "bucket": {
                           "name": "mp3.rightcall",
                           "ownerIdentity": {
                              "principalId": "A3NL1KOZZKExample"
                           },
                           "arn": "arn:aws:s3:::mp3.rightcall"
                        },
                        "object": {
                           "key": "jobs/bda5cbTVd10162.mp3",
                           "size": 1024,
                           "eTag": "d41d8cd98f00b204e9800998ecf8427e",
                           "versionId": "096fKKXTRTtl3on89fVO.nfljtsv6qko",
                           "sequencer": "0055AED6DCD90281E5"
                        }
                     }
                  }
               ]
            }
    response = lambda_handler(s3_new_object_event, None)
    print(response)
