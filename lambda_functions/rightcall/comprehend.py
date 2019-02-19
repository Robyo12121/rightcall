#!/usr/bin/env python3
import boto3
from sys import getsizeof
import logging
import os
import text as text_processing

try:
    COMPREHEND_SIZE_LIMIT = int(os.environ.get('COMPREHEND_SIZE_LIMIT'))
except Exception:
    COMPREHEND_SIZE_LIMIT = 4974

logger = logging.getLogger(__name__)


def sum_sentiments(ResultList, weights=None):
    """
    Takes the sentiment values for each separated string
    and sums them together to give an overall result for
    the rejoined string.
    Using weights calculated from length of string segment
    ensures short strings with a particular sentiment
    don't overpower the overall results.
    INPUTS:
        ResultList
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
        INPUTS:
            string: <str> text of transcript
            chunk_size: <int> number of bytes for each chunk
        OUTPUT:
            chunks: <dict> chunkified version on input string
            weights: <dict> the relative size of each chunk
                compared to the length of the whole input string
        """
        logger.info("Chunkifying...")
        chunks = {}
        weights = {}
        start = 0
        num = 0
        for i in range(0, len(string), chunk_size):
            chunks[num] = {'text': string[start:start + chunk_size]}
            start = start + chunk_size
            num += 1
        for k, v in chunks.items():
            weights[k] = len(v['text']) / len(string)
        return chunks, weights


def best_sentiment(sent_dict):
    """
    Examines all the sentients of
    """
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
    size = getsizeof(text)
    logger.debug(f"Size of text {size}")
    if size >= COMPREHEND_SIZE_LIMIT:
        logger.debug(f"Proceeding with chunkification")
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
    logger.info("Getting entites...")
    if getsizeof(text) >= COMPREHEND_SIZE_LIMIT:
        logger.info("Too big! Proceeding with chunkification")
        entities = []
        chunks, weights = chunkify(text, COMPREHEND_SIZE_LIMIT)
        try:
            r = comprehend.batch_detect_entities(
                TextList=[val['text'] for val in chunks.values()],
                LanguageCode=language_code)
            for ent_list in r['ResultList']:
                entities = entities + ent_list['Entities']

        except Exception as e:
            logger.error(str(e))
            raise e
    else:
        try:
            r = comprehend.detect_entities(Text=text, LanguageCode='en')
            entities = r['Entities']
        except Exception as e:
            logger.error(str(e))
            raise e
    return list(set(x['Text'] for x in entities
                    if x['Type'] not in
                    ['PERSON', 'QUANTITY', 'DATE', 'LOCATION']))


def get_key_phrases(text, language_code='en'):
    comprehend = boto3.client('comprehend')
    logger.info("Getting key phrases...")
    if getsizeof(text) >= COMPREHEND_SIZE_LIMIT:
        logger.info("Too big! Proceeding with chunkification")
        chunks, weights = chunkify(text, COMPREHEND_SIZE_LIMIT)
        try:
            r = comprehend.batch_detect_key_phrases(
                TextList=[val['text'] for val in chunks.values()],
                LanguageCode=language_code)
            kps = []
            for kp_list in r['ResultList']:
                kps = kps + kp_list['KeyPhrases']

        except Exception as e:
            logger.error(str(e))
            raise e
    else:
        try:
            r = comprehend.detect_key_phrases(Text=text, LanguageCode='en')
            kps = r['KeyPhrases']
        except Exception as e:
            logger.error(str(e))
            raise e

    return text_processing.clean(list(set(x['Text'] for x in kps)))

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
    mytext = "Here is a neutral sentence. Things are ok. This company is ok"
    print(get_sentiment(mytext))
