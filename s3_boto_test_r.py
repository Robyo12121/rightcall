import boto3

transcribe = boto3.client('transcribe')

transcribe_bucket = 'transcribe.rightcall'
mp3_bucket = 'mp3.rightcall'



response = transcribe.get_transcription_job(TranscriptionJobName='robin_custom_output_bucket')
print(response)
