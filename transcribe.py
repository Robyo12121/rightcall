#!/usr/bin/env python2
# -*- coding: utf-8 -*-

from urllib.parse import urlparse
from os.path import basename, join

import boto3

destination = 'transcribe.rightcall'

def transcribe_dir(src, dst=None, language_code='en-US'):
    """Transcribe all mp3 file/files from directory (recursively) on AWS S3 with
       AWS Transcribe.
    Input:
        src -- S3 location of the dir (required | type: str). Example:
               'https://s3-eu-west-1.amazonaws.com/examplebucket/';
        dst -- S3 bucket where the transcription is stored (not required |
               type: str);
        language_code -- language code for the language used in the input mp3
                         file (not required | type: str | default: 'en-US').

    """
    while src.startswith('/'): 
        src = src[1:]
    if not src.startswith('http'):
        bucket_name = src.split('/')[0]
        dir_name =  src.replace(bucket_name, '')
        while dir_name.startswith('/'):
            dir_name = dir_name[1:]
        location = boto3.client('s3') \
                .get_bucket_location(Bucket=bucket_name)['LocationConstraint']
        base_url = join('https://s3-%s.amazonaws.com' % location, bucket_name).replace("\\", '/')
    else:
        path = urlparse(src).path
        while path.startswith('/'):
            path = path[1:]
        bucket_name = path.split('/')[0]
        dir_name = path.replace(bucket_name, '')
        while dir_name.startswith('/'):
            dir_name = dir_name[1:]
        base_url = join('https://%s' % urlparse(src).netloc,
                        bucket_name).replace("\\", '/')
        
    for bucket in boto3.resource('s3').buckets.all():
        if bucket.name == bucket_name:
            for key in bucket.objects.all():
                if key.key.startswith(dir_name) and key.key.endswith('.mp3'):
                    url = join(base_url, key.key).replace("\\", '/')
                    transcribe_mp3(url, dst)

def transcribe_mp3(src, dst=None, job_name=None, language_code='en-US'):
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
        job_name = basename(src).replace('.mp3', '')
    if not dst:
        try:
            response = transcribe.start_transcription_job(
                    TranscriptionJobName=job_name, Media={'MediaFileUri': src},
                    MediaFormat='mp3', LanguageCode=language_code,
                    Settings={'ShowSpeakerLabels': True, 'MaxSpeakerLabels': 2})
        except Exception as exception:
            print(exception)
            return
    else:
        try:
            response = transcribe.start_transcription_job(
                    TranscriptionJobName=job_name, Media={'MediaFileUri': src},
                    MediaFormat='mp3', LanguageCode=language_code,
                    OutputBucketName = dst,
                    Settings={'ShowSpeakerLabels': True, 'MaxSpeakerLabels': 2})
        except Exception as exception:
            print(exception)
            return
    return response

# Example. Transcribe
# 'https://s3-eu-west-1.amazonaws.com/examplebucket/example.mp3' file
#transcribe_mp3('https://s3-eu-west-1.amazonaws.com/examplebucket/example.mp3')

# Example. Transcribe
# 'https://s3-eu-west-1.amazonaws.com/examplebucket/example.mp3' file to
# 'examplebucket' bucket on AWS S3
#transcribe_mp3('https://s3-eu-west-1.amazonaws.com/examplebucket/example.mp3',
#               'examplebucket')

# Example. Transcribe all mp3 file from
# 'https://s3-eu-west-1.amazonaws.com/examplebucket/examples/' directory
# (recursively) on AWS S3 with AWS Transcribe
#transcribe_dir('https://s3-eu-west-1.amazonaws.com/examplebucket/examples/')
# or
#transcribe_dir('examplebucket/examples/')

# Example. Transcribe all mp3 file from
# 'https://s3-eu-west-1.amazonaws.com/examplebucket/examples/' directory
# (recursively) on AWS S3 to 'examplebucket' bucket on AWS S3
# with AWS Transcribe
#transcribe_dir('https://s3-eu-west-1.amazonaws.com/examplebucket/examples/',
#               'examplebucket')
# or
#transcribe_dir('examplebucket/examples/', 'examplebucket')
#'https://s3-eu-west-1.amazonaws.com/training-rightcall'
if __name__ == '__main__':
##    transcribe_mp3('https://s3-eu-west-1.amazonaws.com/mp3.rightcall/jobs/b76152TVd00246.mp3',
##                   'transcribe.rightcall', job_name='robins_test')
##    transcribe_dir('https://s3-eu-west-1.amazonaws.com/mp3.rightcall/jobs',
##                   'transcribe.rightcall')

