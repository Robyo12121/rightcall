import json
import boto3
import os
import logging
import sys

from . import text
from . import comprehend
from . import transcribe

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
    try:
        body = json.loads(event['Records'][0]['body'])
    except Exception as e:
        raise e
    if 'Records' not in body:
        logger.error('Records not in Body, likely s3 test event. Ignoring')
        logger.info("""Doing nothing so that this message will be deleted
        from queue.""")
        return False
    else:
        if len(body['Records']) > 1:
            logger.error("More than one record. May be missing a job here!")
        body = body['Records'][0]

    bucket = body['s3']['bucket']['name']
    key = body['s3']['object']['key']
    logger.info("Bucket Event: {}".format(str(bucket)))
    if bucket == MP3S:
        logger.info("Bucket: {}, Key: {}".format(
            str(bucket),
            str(key)))
        uri = 'https://s3-' + event['Records'][0]['awsRegion'] + \
            '.amazonaws.com/' + bucket + '/' + key
        logger.info("URI: {}".format(str(uri)))
    else:
        logger.info("Wrong Bucket")
    try:
        response = transcribe.transcribe_mp3(uri, TRANSCRIPTS)
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
        logger.debug(f"Trying to get {filename} from {TRANSCRIPTS}")
        data = s3.get_object(Bucket=TRANSCRIPTS, Key=filename)
        data = data['Body'].read().decode('utf-8')
    except Exception as e:
        logger.error(str(e))
        raise e
    else:
        logger.debug("Success")

    try:
        data = json.loads(data)
    except Exception as e:
        logger.error(str(e))
        raise e

    logger.debug(f"Keys of object: {str(data.keys())}")

    # Give the transcript text to comprehend.py
    try:
        transcript_text = data['results']['transcripts'][0]['transcript']
    except Exception as e:
        logger.error(str(e))
        raise e

    if sys.getsizeof(transcript_text) <= 25:
        logger.warning(f"Transcript is empty for {filename}. Exiting.")
        return False

    r = {}
    logger.debug(f"Creating record")
    r['reference_number'] = event['detail']['TranscriptionJobName'] \
        .split('--')[0]
    logger.debug(f"Ref: {r['reference_number']}")
    r['text'] = transcript_text
    logger.debug(f"Text: {r['text']}")
    # Get sentiment using AWS Comprehend
    sentiment = comprehend.get_sentiment(transcript_text)
    r['sentiment'] = sentiment
    logger.debug("Sentiment: {}".format(str(r['sentiment'])))
    # Check promotion
    promo = text.check_promo(transcript_text)
    r['promotion'] = promo
    logger.debug("r promo: {}".format(str(r['promotion'])))
    # Get entities
    r['entities'] = comprehend.get_entities(transcript_text)
    logger.debug(f"Text: {r['entities']}")
    # Get Key Phrases
    r['keyphrases'] = comprehend.get_key_phrases(transcript_text)
    logger.debug(f"Text: {r['keyphrases']}")
    # Save to json file in 'comprehend.rightcall' bucket
    logger.debug(f"Finished creating record")
    logger.debug(f"Saving to {COMPREHEND} s3 bucket")
    try:
        response = s3.put_object(Body=json.dumps(r, indent=2),
                                 Bucket=COMPREHEND,
                                 Key=event['detail']['TranscriptionJobName'] 
                                 + '.json')
    except Exception as e:
        logger.error(str(e))
        raise e
    else:
        logger.debug("Success")
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


def event_type_sqs_s3_new_object(event):
    """
    Check if event is a new object event from s3 delivered by sqs
    """
    body = json.loads(event['Records'][0]['body'])
    if 'Records' not in body:
        return False
    if 'Records' in event.keys():
        if event['Records'][0]['eventSource'] == 'aws:sqs':
            return True
        else:
            return False
    return False


def Rightcall(event):
    """Determine event type (S3 or Cloudwatch) and
    take appropriate action"""
    response = {}
    if event_type_transcribe_job_status(event):
        logger.info("Transcribe job event received. Sending to Comprehend.")
        response = Comprehend(event)
    elif event_type_sqs_s3_new_object(event):
        logger.info("New mp3 uploaded. Sending to Transcribe.")
        response = Transcribe(event)
    else:
        logger.info("Unknown Event Type. Ignoring")
        response = False
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
    sqs_test_event = {
        "Records": [
            {
                "messageId": "4227a528-e786-46a7-8906-0c6a9260ebc0",
                "receiptHandle": "AQEBDOvsT+wgZOTQzDFAgZiJXIREBOMC/X9lyyQXnhFbVZLSwmbGPe5ZkowPPUpIv5ZnEWAUIDt+YsDNsOlaV+uDXBh7/JKG09/m4kuH3CB8XAbXnNCH2X0HDCf+cwAN8sLxBGQtcZ5tcYyPD+ZwD55qSeeXfK6RVVBrOaLZHush75ZO102F8ciYzyQHosylXsD3lTG1HJaVsMyyAEqBZbWcO4U3p3U3NxHapKbOB80IrwdJ4wgjVGgasIEsTIH+sMlImjjowyM6hbnfvU8B6pIAXyHrnqPCCgrnAHL49DjBFIauzU4F528RVSAGbM9Trm9MkmdQNW5w8HWw8GfELjz+O0aRDl3Cylrti1wAk/Du/h9Co+F00vd/q3f+vGfmuJSuH2aIOkSUGFJ2TCPkPAQ7TA==",
                "body": "{\"Service\":\"Amazon S3\",\"Event\":\"s3:TestEvent\",\"Time\":\"2018-10-25T10:21:09.859Z\",\"Bucket\":\"mp3.rightcall\",\"RequestId\":\"B30EA916318276B2\",\"HostId\":\"+7iVNqh64HZ8/+QSK4i5CV5WtiYIGi8Xgpsb6jXYGcPPtUB0SdjECoWenb/vK3+Duuf1qENMOm8=\"}",
                "attributes": {
                    "ApproximateReceiveCount": "137",
                    "SentTimestamp": "1540462870055",
                    "SenderId": "AIDAJVEO32BJMF27H2JKW",
                    "ApproximateFirstReceiveTimestamp": "1540462870055"
                        },
                "messageAttributes": {},
                "md5OfBody": "802614776f1fdea805f34291a274c685",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:eu-west-1:868234285752:RightcallTranscriptionJobs",
                "awsRegion": "eu-west-1"
            }
        ]
    }
    sqs_event = {
            "Records": [
             {
                "messageId": "1b498e27-3a64-4847-89bd-e5026604b212",
                "receiptHandle": "AQEBJuUjL8Q2Z93UOS03uX/ynEUWt35ySRWqgFltcsiyv3bcSqeM+HdQc/aoNzIYHf3Z5gCtMpXRZ3EFo2Z/jCJHQAX5e8lH/Wot8VvZ3iBUzVL/GgV5Sg5mU/i+XrtDvuprzE2uvrDYMIAuzUfzoYUBJyPBn6uZRed/0gfK7XhXSeiBhRnMo1Qjjzuguiton6inehUaDUK3Nre+h5+yDDjU/ZBZmFfGi+mfUIJ6+z6n6MbxqC3yF8Oh2zOWF5QavEk9dxVAT/+t1p6wtagvXjT0od1WHZ1U8K9aUzSFePcdA+/hr0SIrpcpO1011/9iJtT837o+AvgONxsg7HEM9oUsAO/nH/erQVn45/q+Kdq57iVKHkzLNM+SJM6CQcHx9UlCdrZoT7IricptnlOGz6MTjQ==",
                "body": "{\"Records\":[{\"eventVersion\":\"2.0\",\"eventSource\":\"aws:s3\",\"awsRegion\":\"eu-west-1\",\"eventTime\":\"2018-10-26T09:30:14.124Z\",\"eventName\":\"ObjectCreated:Put\",\"userIdentity\":{\"principalId\":\"AWS:AIDAJFUNXNZ3MF2LQLH4O\"},\"requestParameters\":{\"sourceIPAddress\":\"185.31.48.30\"},\"responseElements\":{\"x-amz-request-id\":\"CC8C87308642EBE5\",\"x-amz-id-2\":\"+yybnPTMtJ4vslctD/uUXfeI//dT/4qbCF8c6O5QaSzAbWSsr4fB164HcgbkQT3jol9Ul2kn0g8=\"},\"s3\":{\"s3SchemaVersion\":\"1.0\",\"configurationId\":\"2116bf90-74fc-4c00-a60d-d3c2c5a35132\",\"bucket\":{\"name\":\"mp3.rightcall\",\"ownerIdentity\":{\"principalId\":\"A2VMRRM44J4WYZ\"},\"arn\":\"arn:aws:s3:::mp3.rightcall\"},\"object\":{\"key\":\"jobs/c9c4bfTOd00994.mp3\",\"size\":784656,\"eTag\":\"8c31f6b56802fa76346d04cdcfdeb430\",\"sequencer\":\"005BD2DEA55CFDC057\"}}}]}",
                "attributes": {
                   "ApproximateReceiveCount": "1",
                   "SentTimestamp": "1540546214173",
                   "SenderId": "AIDAJVEO32BJMF27H2JKW",
                   "ApproximateFirstReceiveTimestamp": "1540546214174"
                   },
                "messageAttributes": {},
                "md5OfBody": "94f778ba57b38e0080a56466c7727f96",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:eu-west-1:868234285752:RightcallTranscriptionJobs",
                "awsRegion": "eu-west-1"
                }
            ]
        }
    unhandled = {
        "Records": [
            {
                "messageId": "861e8a55-322d-47a0-b17b-0c291904206c",
                "receiptHandle": "AQEBysB8VVt2s2pAP2h9WQERLVwB7egK3VYCITBnMSYw5HaB98YGrkfMW4+of/9t7sJE9Ghd0E2fxYxFP8aYTGMF4xxKDxSfrHWczI3hiXCxScQPDppIQDWK2KrJ6jc3exzh0nJeXS2qZ9zwSjn+yjkUp5XGeGgj8CyZY/6uGC3UqBUa6+KxMdssPc7oPC1/sMribgcBNelrRDIv2p1X53ofL7afYEzuCgHVy8RO2hiFF6EPRpw7/3BnlmII4I/3zWoIbWulk50LEvlv8C53nZk+0JGxvlhUJyVLX16KKdL/GoUsmwU6h0KYFaermLdaqZi0i2znQeMw2Ye8XSvJL8jQs33IkFBCiiwLFvvuCKzQiVJnOdsfKJ3u9BF1aVglDod9zYqT2QvZxpxo6RIRjuXH+g==",
                "body": "{\"Service\":\"Amazon S3\",\"Event\":\"s3:TestEvent\",\"Time\":\"2018-10-31T14:08:15.242Z\",\"Bucket\":\"mp3.rightcall\",\"RequestId\":\"BB78EBDA2952667F\",\"HostId\":\"+8NUd+txRSmPQlapdO69n47dxJi5DQqqZ+kFV//ooNOkj0iBnFbONOSN4/XO6TfJf54exLEJtRo=\"}",
                "attributes": {
                    "ApproximateReceiveCount": "1",
                    "SentTimestamp": "1540994895388",
                    "SenderId": "AIDAJVEO32BJMF27H2JKW",
                    "ApproximateFirstReceiveTimestamp": "1540994895398"
                    },
                "messageAttributes": {},
                "md5OfBody": "90bca7ebfeff23e8c6e9458ef7a80907",
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:eu-west-1:868234285752:RightcallTranscriptionJobs",
                "awsRegion": "eu-west-1"
                }
            ]
        }
    response = lambda_handler(unhandled, None)
    print(response)
