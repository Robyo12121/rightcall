#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# 1. Download mp3 files from www.prosodie.com to local machine.
# 2. Upload mp3 files from local machine to 'mp3.rightcall' AWS S3 bucket.
# 3. Transcribe mp3 files from 'mp3.rightcall' AWS S3 bucket to
# 'transcribe.rightcall' AWS S3 bucket.
# 4. Save data to Google Sheets.

import json
from os import remove
from os.path import basename, join

import boto3

from comprehend import get_sentiment
from sheets import append_row
from text import check_promo

transcribe_bucket_name = 'transcribe.rightcall'
mp3_bucket_name = 'mp3.rightcall'

def main(transcribe_bucket_name, mp3_bucket_name):
    """Right Call/Contact Center Monitoring written in Python"""

    s3 = boto3.resource('s3')
    for bucket in s3.buckets.all(): # Loop through all buckets
        if bucket.name == transcribe_bucket_name: # Find transcribe bucket
            for key in bucket.objects.all(): # Loop through keys
                print(key)
                if key.key.endswith('.json'): # Find json files
                    print(f"Key: {key.key}")
                    r = {}
                    # Get reference number
                    reference = basename(key.key).replace('.json', '') # Find name of file
                    r['ref'] = reference
                    print(f"r: {r}")
                    # Get URL
                    location = boto3.client('s3') \
                            .get_bucket_location(
                            Bucket=mp3_bucket_name)['LocationConstraint'] 
                    print(f"Location: {location}")
                    base_url = join('https://s3-%s.amazonaws.com' % location, 
                            mp3_bucket_name).replace("\\", '/')
                    print(f"Base URL: {base_url}")
                    url = join(base_url, key.key.replace('.json', '.mp3')).replace("\\", '/')
                    print(f"MP3 URL: {url}")
                    r['url'] = url
                    print(f"r: {r}")
                    # Download json file
                    try:
                        s3.Bucket(transcribe_bucket_name) \
                          .download_file(key.key, key.key) 
                    except Exception as exception:
                        print(exception)
                    # Get text
                    try:
                        with open(key.key, 'r') as f:
                            data = json.load(f)
                    except Exception as exception:
                        print(exception)

                    try:
                        text = data['results']['transcripts'][0]['transcript']
                    except Exception as exception:
                        print(exception)
                    r['text'] = text
                    # Get sentiment
                    sentiment = get_sentiment(text)
                    r['sentiment'] = sentiment
                    print(f"r sentiment: {r['sentiment']}")
                    # Check promotion
                    promo = check_promo(text)
                    r['promo'] = promo
                    print(f"r promo: {r['promo']}")
                    # Save to Google Sheets
                    values = [r['ref'], r['text'], r['promo'], r['sentiment'],
                              r['url']]
                    append_row(values)
                    # Remove tmp json file from local machine
                    remove(key.key)

main(transcribe_bucket_name, mp3_bucket_name)
