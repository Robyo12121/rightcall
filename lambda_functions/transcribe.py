import urllib
from os.path import basename, join
import uuid
import json
import boto3

s3 = boto3.client('s3')


DESTINATION = os.environ.get('transcribe.rightcall')


def transcribe_mp3(src, dst=DESTINATION, job_name=None, language_code='en-US'):
    """Transcribe mp3 file with AWS Transcribe.
    Input:
        src -- S3 location of the src mp3 file (required | type: str). Example:
               'https://s3-eu-west-1.amazonaws.com/examplebucket/example.mp3';
        dst -- S3 bucket where the transcription is stored (not required |
               type: str). Example:
               'example-bucket';
        job_name -- the name of the job (not required | type: str);
        language_code -- language code for the language used in the input mp3
                         file (not required | type: str | default: 'en-US').

    """
    transcribe = boto3.client('transcribe')
    if not job_name:
        job_name = '--'.join([basename(src).replace('.mp3',''), str(uuid.uuid4())])

    try:
        response = transcribe.start_transcription_job(
                TranscriptionJobName=job_name, Media={'MediaFileUri': src},
                MediaFormat='mp3', LanguageCode=language_code,
                OutputBucketName = dst,
                Settings={'ShowSpeakerLabels': True, 'MaxSpeakerLabels': 2,
                          'VocabularyName': "teva-vocab"})
    except Exception as exception:
        print(exception)
        return
    
    return response

def Transcribe(event):
    bucket = event['Records'][0]['s3']['mp3.rightcall']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(bucket, key)
##    try:
##        response = transcribe_mp3(event['mp3'], DESTINATION)
##        return response
##    except Exception as e:
##        print(e)
##        raise e
    
def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    response = Transcribe(event)
    return response


if __name__ == '__main__':
    event['mp3'] = 'https://s3-eu-west-1.amazonaws.com/mp3.rightcall/bd1b2dTVd10549.mp3'
    response = lambda_handler(event, None)
    print(response)


