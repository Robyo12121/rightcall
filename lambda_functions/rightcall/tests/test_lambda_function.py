import unittest
from unittest.mock import patch
import sys
import os.path
sys.path.append(os.path.dirname(os.path.realpath(__file__)) + "/../")
import lambda_function


class test_Lambda_Function(unittest.TestCase):

    def test_Transcribe(self):
        pass

    def test_Comprehend(self):
        pass

    def test_event_type_transcribe_job_status(self):
        true_event = {'source': 'aws.transcribe',
                 'detail': {'TranscriptionJobName': 'Test_Event',
                            'TranscriptionJobStatus': 'testing'}}
        self.assertTrue(lambda_function.event_type_transcribe_job_status(true_event))
        false_event = {'source': 'aws.transcribe',
                 'detail': {'TranscriptionJobName': 'Test_Event',
                            'TranscriptionJobStatus': 'testing'}}

    def test_event_type_sqs_s3_new_object(self):
        pass

    def test_Rightcall(self):
        pass

    def test_lambda_handler(self):
        pass


def main():
    unittest.main()


if __name__ == '__main__':
    main()