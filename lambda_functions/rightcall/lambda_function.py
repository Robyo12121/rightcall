from urllib.parse import unquote_plus
import json
import boto3

from transcribe import transcribe_mp3
from comprehend import get_sentiment
from text import check_promo
from sheets import append_row


#DESTINATION = os.environ.get('DESTINATION')
#SOURCE = os.environ.get('SOURCE')

TRANSCRIPTS = 'transcribe.rightcall'
MP3S = 'mp3.rightcall'
print(MP3S, TRANSCRIPTS)


def Transcribe(event):
    """
    S3 object created event received
    Get URI of file and send it to transcribe
    """
    bucket = event['Records'][0]['s3']['bucket']
    print(bucket['name'])
    if bucket['name'] == MP3S:
        key = unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        print(bucket['name'], key)
        uri = 'https://s3-' + event['Records'][0]['awsRegion'] + \
        '.amazonaws.com/'+ bucket['name'] + '/' + key
        print(uri)
    else:
        return "Wrong Bucket"
    try:
        response = transcribe_mp3(uri, TRANSCRIPTS)
        return response
    except Exception as e:
        print(e)
        raise e

def Comprehend(event):
    """
    Cloudwatch 'Transcribe finished' event received.
    Go to destination bucket and get <job name>.json
    
    """
    # Fetch json file related to completed transcribe event from DESTINATION s3 bucket
    s3 = boto3.client('s3')
    filename = event['detail']['TranscriptionJobName'] + '.json'
    
    print("Filename: {}".format(filename))
    try:
        data = s3.get_object(Bucket=TRANSCRIPTS,
                                 Key=filename)
        data = data['Body'].read().decode('utf-8')
    except Exception as e:
        raise e

    try:
        data = json.loads(data)
    except Exception as e:
        raise e

    print(data.keys())
    # Give the transcript text to comprehend.py

    try:
        text = data['results']['transcripts'][0]['transcript']
    except Exception as exception:
        raise(exception)
    r = {}
    r['ref'] = event['detail']['TranscriptionJobName'].split('--')[0]
    r['text'] = text
    # Get sentiment
    sentiment = get_sentiment(text)
    r['sentiment'] = sentiment
    print("Sentiment: {}".format(r['sentiment']))
    # Check promotion
    promo = check_promo(text)
    r['promo'] = promo
    print("r promo: {}".format(r['promo']))
    # Save to Google Sheets
    values = [r['ref'], r['text'], r['promo'], r['sentiment']]#,
              #r['url']]
    append_row(values)

def event_type_transcribe_job_status(event):
    """Check if event is from Cloudwatch
        If 'aws.transcribe' event from Cloudwatch return True
        Else return False
    """
    if 'source' in event and event['source'] == 'aws.transcribe':
        print("Job: {} Status: {}".format(event['detail']['TranscriptionJobName'],
                                          event['detail']['TranscriptionJobStatus']))
        print()
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
        print("New mp3 in bucket. Sending to Transcribe")
        response = Transcribe(event)
    return response

def lambda_handler(event, context):
    """ New MP3 file uploaded to 'mp3.rightcall'
    Event sent to this lambda function from s3 bucket
    """
    print("Received event: " + json.dumps(event, indent=2))
    response = Rightcall(event)
    return response


if __name__ == '__main__':
    transcribe_job_status_event = {
       "version": "0",
       "id": "event ID",
       "detail-type":"Transcribe Job State Change",
       "source": "aws.transcribe",
       "account": "account ID",
       "time": "timestamp",
       "region": "region",
       "resources": [],
       "detail": {
         "TranscriptionJobName": "b76152TVd00246--3bc99ed9-e035-4316-9a05-196d74b9a25a",
         "TranscriptionJobStatus": "COMPLETE"
       }
     }
    s3_new_object_event ={  
               "Records":[  
                  {  
                     "eventVersion":"2.0",
                     "eventSource":"aws:s3",
                     "awsRegion":"eu-west-1",
                     "eventTime":"1970-01-01T00:00:00.000Z",
                     "eventName":"ObjectCreated:Put",
                     "userIdentity":{  
                        "principalId":"AIDAJDPLRKLG7UEXAMPLE"
                     },
                     "requestParameters":{  
                        "sourceIPAddress":"127.0.0.1"
                     },
                     "responseElements":{  
                        "x-amz-request-id":"C3D13FE58DE4C810",
                        "x-amz-id-2":"FMyUVURIY8/IgAtTv8xRjskZQpcIZ9KG4V5Wp6S7S/JRWeUWerMUE5JgHvANOjpD"
                     },
                     "s3":{  
                        "s3SchemaVersion":"1.0",
                        "configurationId":"testConfigRule",
                        "bucket":{  
                           "name":"mp3.rightcall",
                           "ownerIdentity":{  
                              "principalId":"A3NL1KOZZKExample"
                           },
                           "arn":"arn:aws:s3:::mp3.rightcall"
                        },
                        "object":{  
                           "key":"b9823rr20302r2.mp3",
                           "size":1024,
                           "eTag":"d41d8cd98f00b204e9800998ecf8427e",
                           "versionId":"096fKKXTRTtl3on89fVO.nfljtsv6qko",
                           "sequencer":"0055AED6DCD90281E5"
                        }
                     }
                  }
               ]
            } 
    response = lambda_handler(transcribe_job_status_event, None)
    print(response)
