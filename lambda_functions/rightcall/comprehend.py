#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import boto3
from sys import getsizeof
import logging
import os

# COMPREHEND_SIZE_LIMIT = int(os.environ.get('COMPREHEND_SIZE_LIMIT'))
COMPREHEND_SIZE_LIMIT = 4974

# Logging
logging.basicConfig()
logger = logging.getLogger()
if os.getenv('LOG_LEVEL') == 'DEBUG':
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)


def sum_sentiments(ResultList, weights=None):
    """
    Takes the sentiment values for each separated string
    and sums them together to give an overall result for
    the rejoined string.
    Using weights calculated from length of string segment
    ensures short strings with a particular sentiment
    don't overpower the overall results.
    """
    logger.info("Summing sentiments")
    sums = {'Positive': 0,
            'Negative': 0,
            'Neutral': 0,
            'Mixed': 0}
    if weights is None:
        logger.info("No weights passed")
        for result in ResultList:
            for k in sums.keys():
                sums[k] += result['SentimentScore'][k]
    else:
        logger.info("Using weights")
        for result in ResultList:
            logger.debug("Working on {}".format(result))
            for k in sums.keys():
                sums[k] += result['SentimentScore'][k] * \
                                    weights[result['Index']]

    return sums


def chunkify(string, chunk_size=4970):
        """Separates a string into chunks small
        enough to fit into COMPREHEND's 5000byte/string limit
        Returns dictionary:
            '0': {'text': 'chunk 1 text'
                  'weight': <chunkweight>},
            '1': { etc}
            """
        logger.info("Chunkifying...")
        chunks = {}
        weights = {}
        start = 0
        num = 0
        for i in range(0, len(string), chunk_size):
            chunks[num] = {'text': string[start:start+chunk_size]}
            start = start+chunk_size
            num += 1
        for k, v in chunks.items():
            weights[k] = len(v['text'])/len(string)
        return chunks, weights


def best_sentiment(sent_dict):
    logger.info("Finding best sentiment")
    sentiment = None
    highest = 0
    for k, v in sent_dict.items():
        if v > highest:
            highest = v
            sentiment = k
    return sentiment


def create_set(ent_list):
    """
    Create a set of entities without repeating elements but retain
    the frequency with which they are detected.
    """
    ent_set = {}
    for ent in ent_list:
        if ent['Text'] not in ent_set.keys():
                ent_set[ent['Text']] = ent
                ent_set[ent['Text']]['Count'] = 1
        else:
            ent_set[ent['Text']]['Count'] += 1
    return ent_set


def get_sentiment(text, language_code='en'):
    """Inspects text and returns an inference of the prevailing sentiment
    (positive, neutral, mixed, or negative).
    Input:
        text -- UTF-8 text string. Each string must contain fewer that
                5,000 bytes of UTF-8 encoded characters (required | type: str);
        language_code -- language of text (not required | type: str |
                         default: 'en').
    Output:
        sentiment -- sentiment: positive, neutral, mixed, or negative
                     (type: str).

    """
    comprehend = boto3.client('comprehend')
    logger.info("Getting sentiment...")
    if getsizeof(text) >= COMPREHEND_SIZE_LIMIT:
        logger.debug("Too big! Proceeding with chunkification")
        chunks, weights = chunkify(text, COMPREHEND_SIZE_LIMIT)
        try:
            r = comprehend.batch_detect_sentiment(
                            TextList=[val['text'] for val in chunks.values()],
                            LanguageCode=language_code)
            logger.debug("Result List: {}".format(r['ResultList']))
        except Exception as e:
            logger.error(str(e))
            raise e
        else:
            sums = sum_sentiments(r['ResultList'], weights)
            logger.debug("Sums: {}".format(sums))
            sentiment = best_sentiment(sums).lower()

    else:
        try:
            r = comprehend.detect_sentiment(Text=text, LanguageCode='en')
            sentiment = r['Sentiment'].lower()
        except Exception as e:
            logger.error(str(e))
            raise e
    return sentiment


def get_entities(text, language_code='en'):
    comprehend = boto3.client('comprehend')
    print("Getting entites...")
    if getsizeof(text) >= COMPREHEND_SIZE_LIMIT:
        print("Too big! Proceeding with chunkification")
        combined = []
        chunks, weights = chunkify(text, COMPREHEND_SIZE_LIMIT)
        try:
            r = comprehend.batch_detect_entities(
                        TextList=[val['text'] for val in chunks.values()],
                        LanguageCode=language_code)
            entities = []
            for ent_list in r['ResultList']:
                combined = combined + ent_list['Entities']

        except Exception as e:
            print(str(e))
            raise e
        else:
            return list(create_set(combined))
    else:
        try:
            r = comprehend.detect_entities(Text=text, LanguageCode='en')
            entities = r['Entities']
        except Exception as e:
            print(str(e))
            raise e
        return list(create_set(entities))


def get_key_phrases(text, language_code='en'):
    comprehend = boto3.client('comprehend')
    print("Getting key phrases...")
    if getsizeof(text) >= COMPREHEND_SIZE_LIMIT:
        print("Too big! Proceeding with chunkification")
        chunks, weights = chunkify(text, COMPREHEND_SIZE_LIMIT)
        try:
            r = comprehend.batch_detect_key_phrases(
                        TextList=[val['text'] for val in chunks.values()],
                        LanguageCode=language_code)
            kps = []
            for kp_list in r['ResultList']:
                kps = kps + kp_list['KeyPhrases']

        except Exception as e:
            print(str(e))
            raise e
        else:
            list(return create_set(kps))
    else:
        try:
            r = comprehend.detect_key_phrases(Text=text, LanguageCode='en')
            kps = r['KeyPhrases']
        except Exception as e:
            print(str(e))
            raise e
        return list(create_set(kps))

# Example. Get sentiment of text below:
# "I ordered a small and expected it to fit just right but it was a little bit
# more like a medium-large. It was great quality. It's a lighter brown than
# pictured but fairly close. Would be ten times better if it was lined with
# cotton or wool on the inside."
# text = "I ordered a small and expected it to fit just right but it was a \
#        little bit more like a medium-large. It was great quality. It's a \
#        lighter brown than pictured but fairly close. Would be ten times \
#        better if it was lined with cotton or wool on the inside."
# get_sentiment(text)


if __name__ == '__main__':
    text = """I ordered a small and expected it to fit just right but it was a little bit
 more like a medium-large. It was great quality. It's a lighter brown than
 pictured but fairly close. Would be ten times better if it was lined with
 cotton or wool on the inside."""
    call_text = """Hello, heavy several services. My name is barbara can providing your full name or previous-ticket-number if you have one. Yeah. Ah, christopher pink-button hm. Okay, just a moment, please. So yeah. Fifteen. Your last name. Okay. Goodness, g o o d a good man. Oh, yeah. Oh! Hm. Yeah, oh, mhm, yeah, yeah, yeah, okay, yeah, you can send me your manager's name, Outlook say that again. O.K., please tell-me your manager's name. Yeah, maria southerly. Okay, and, uh, i'm going to send me a phone number, just in case you get. You can just kind of, yeah. Ah, two, one, five, 5, 1, 8 three, nine, seven one, seven one are you going to located in ah northwest, too ? Yes, okay, how may i assist ? Um, i need to have my, i guess, my password. Except for oracle. I sent iExpense, okay, and they use your name is the same as the the main one or a different. I think it's the same at the door, may see equipment here. One. Okay, i'm going to search for that one, to take a few moments, okay ? Mhm, yeah, yeah. Yeah. Oh, yeah. Yeah. Yeah. Oh, yeah. Oh! So i'm gonna search for seeing goodman a one. Point. Yeah, yeah, yeah, mhm. Oh, yeah. Mhm. Yeah, yeah, yeah. Oh, oh, yeah! Oh, yeah, yeah. Okay, mhm. Okay. Yeah, yeah. Yeah, yeah, mhm, mhm. Okay, so i see a change your password. I will give you a temporary one. It's seven thirty one, two, three, four, five. Okay, right, yeah, yeah, yeah, mhm. Oh, yeah. Okay. Okay, okay. They, uh, yeah, yeah, yeah, mhm, yeah, yeah. Mhm. Yeah. Yeah. Yeah, yeah, yeah. Oh, okay. Oh, yeah, is it working ? Oh, L.M.S.. Mhm. Okay. Oh. Meanwhile, i want to ask you if you have a try to contact-number virtual-assistant yeah, it's, another way of contacting you, the service at the help desk. Mhm. Yeah, yes, i've done that before. We're satisfied with the the the virtual seas. Yeah, i'm i'm in, i'm in oracle, and, yes, i contact virtual. I used a virtual process before also, okay, and now they're up, i think, it's, uh, t b, so you can try to use one of those on time. Okay, so is seventy O.K. regarding the the password, correct ? Okay, is there anything else i may assist ? Tell-me i'm good for now. Thank you. Okay, thank you for calling have-a-nice-day. Thanks bye."""
    r = {}
    r['sentiment'] = get_sentiment(call_text)
    r['key_phrases'] = get_key_phrases(call_text)
    r['entities'] = get_entities(call_text)
    print(r)
