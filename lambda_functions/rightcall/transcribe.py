from os.path import basename
import os
from .id_gen import id_generator
import boto3
import logging

# Logging
logging.basicConfig()
logger = logging.getLogger()
if os.getenv('LOG_LEVEL') == 'DEBUG':
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


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
    logger.info("Transcribing mp3...")
    transcribe = boto3.client('transcribe')
    if not job_name:
        logger.debug("Generating Id...")
        logger
        job_name = '--'.join(
            [basename(src).replace('.mp3', ''), id_generator()])

    try:
        logger.debug("trying job...")
        response = transcribe.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={'MediaFileUri': src},
                MediaFormat='mp3', LanguageCode=language_code,
                OutputBucketName=dst,
                Settings={'ShowSpeakerLabels': True, 'MaxSpeakerLabels': 2,
                          'VocabularyName': "teva-vocab"})
    except Exception as exception:
        raise exception
    return response
