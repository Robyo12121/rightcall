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
                            LanguageCode='en')
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
    long_text = """Hello, you reached diversity. Sisk, this is amanda speaking. Can you please provide in your phone name or the previous ticket number ? Hi, this is one of my name is tony. I'm in one, [SILENCE] okay. Can't just verifying your manager name. Uh, paltry julie calling on okay. And your telephone number. In case we get disconnected. And it's, um, it's three, two, one. See that's, one, one, eight, six, three is my extension number. Okay, thanks very much. How may i assist you today ? It's the same thing every every time. It's, just my love on sometimes it won't take my passport. They have a ticket open with this issue or before that. Oh, yeah, it's. Like one of those three weeks ago. Sam can give me that to a good number for this. The part of tens and it's, uh, kind of lucky. A mess significant. And give me the ticket number. Ten minutes is your user name. Uh, it's. Um, t c. Just the physical system. And on me, yeah. [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] They want to change the faster also, or you want to try it again. Yeah, you can taste with the change that last part. I have on the acura dot com. And now i would do your best. [SILENCE] Okay. Now you are in the office. Yes, okay. Mhm! [SILENCE] [SILENCE] [SILENCE] [SILENCE] Just a second, please. Our system goes a little bit slow. Yeah. [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] Okay, well, thank you for waiting. Can you please, uh, again with us where they were ? One, two, three, four, ten, thirty. Okay. Okay. Okay. Dollars. Log on. Should it be tight in the process forward in some baseball and that's, just a s, um, looking, please one more time with your past within the player. Seven one, two, three, four yes, you can try with this one, please, and they look like on, um, removed help, okay. And after that one time log on. [SILENCE] Yep, responsible again, what you're saying, [SILENCE] response one is up. Mhm. Can you tell me, please ? Your computer name. It should have this on my p s i e. One zero one two three three. Okay. I have here, uh, one zero, one, two, three, three, yes, at one zero, one, two, three, three. Response. One zero, sixty eight, [SILENCE] zero, six, six, eight zero, six, six, eight, zero, seven, three one. Zero seven three one. Nine zero seven. Three what's this. So far, as most is zero. Zero, six, six, eight, zero, seven. Two years. One, nine years, you know. I do okay. And craig, who now on jobs. And then, if there's a challenge. Yes, it's. Zero zero, nine, five yes, one, six, nine, three, yes, eight zero three. Zero zero nine five one six nine three eight zero three. Okay, and response to is. Three, four, five, three. And there's. A it's, it's. A bad long, but could you ? What was the first digits again to me. Three, four, five, three. Three four five three three five five three. Zero. Three, five, five. Three no it's three, five three zero. Three, five, three, zero okay, seven, two, eight, nine. Two eight nine nine six two six. Nine six two six zero three two three. Zero three two three nine three four. Zero. Nine three. Zero eight eight four. Zero is four it's zero eight eight four. Zero [SILENCE] it's four two, one. One, one, two, one. I was pretty two zero is it for two, one at the end ? Two one. Okay, though, oh, number three, four, five, three, three, five, three, zero, seven, two, eight, nine, nine, six, two, six, zero, three, two, three, nine, three, four, zero, zero, eight, eight, four, two, one yes, what include one ? Okay, please. Yes, sir. Okay, i was talking to. Okay, bye. I would wait and see if you will succeed lookin. Okay. [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] So it's up to the other user is come up [SILENCE] something for me to put in the passport. When i put in seven one, two, three four yesterday with this one. Manage your password names and correct. I'm trying my own password. Yes, try way through your all the password. So what you said either. So it's going to take the new password ? Okay. No, we'll take any faster it's going to be. And what is ? Their message is received. Hm. What is the error message that you received ? And, you know, seven, one, two, three, four, [SILENCE] one, three, four. Cool. He was in my password was incorrect if you've got your passport easily, because the jersey password manager okay, can you try one more time with best with their, ah one, two, three, four, five with capital t. [SILENCE] Competency. Yes, yeah. And one, two, three, four, four, one, two, three, four. Okay. [SILENCE] [SILENCE] Okay, i think. It was. It seems to be, if it's going to pass it as a design error message saying, welcome, okay, so what's. The second one is what's really uses it for five cents. Okay, now we can't change your password with within you. Okay, okay, anything else ? How may i assist you with well, no that's. Perfect. Okay, thank you, thank you. Thanks for calling to cities this kevin easy. I have like, bye bye. Thank you, bye.
                Hello, you reached diversity. Sisk, this is amanda speaking. Can you please provide in your phone name or the previous ticket number ? Hi, this is one of my name is tony. I'm in one, [SILENCE] okay. Can't just verifying your manager name. Uh, paltry julie calling on okay. And your telephone number. In case we get disconnected. And it's, um, it's three, two, one. See that's, one, one, eight, six, three is my extension number. Okay, thanks very much. How may i assist you today ? It's the same thing every every time. It's, just my love on sometimes it won't take my passport. They have a ticket open with this issue or before that. Oh, yeah, it's. Like one of those three weeks ago. Sam can give me that to a good number for this. The part of tens and it's, uh, kind of lucky. A mess significant. And give me the ticket number. Ten minutes is your user name. Uh, it's. Um, t c. Just the physical system. And on me, yeah. [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] They want to change the faster also, or you want to try it again. Yeah, you can taste with the change that last part. I have on the acura dot com. And now i would do your best. [SILENCE] Okay. Now you are in the office. Yes, okay. Mhm! [SILENCE] [SILENCE] [SILENCE] [SILENCE] Just a second, please. Our system goes a little bit slow. Yeah. [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] Okay, well, thank you for waiting. Can you please, uh, again with us where they were ? One, two, three, four, ten, thirty. Okay. Okay. Okay. Dollars. Log on. Should it be tight in the process forward in some baseball and that's, just a s, um, looking, please one more time with your past within the player. Seven one, two, three, four yes, you can try with this one, please, and they look like on, um, removed help, okay. And after that one time log on. [SILENCE] Yep, responsible again, what you're saying, [SILENCE] response one is up. Mhm. Can you tell me, please ? Your computer name. It should have this on my p s i e. One zero one two three three. Okay. I have here, uh, one zero, one, two, three, three, yes, at one zero, one, two, three, three. Response. One zero, sixty eight, [SILENCE] zero, six, six, eight zero, six, six, eight, zero, seven, three one. Zero seven three one. Nine zero seven. Three what's this. So far, as most is zero. Zero, six, six, eight, zero, seven. Two years. One, nine years, you know. I do okay. And craig, who now on jobs. And then, if there's a challenge. Yes, it's. Zero zero, nine, five yes, one, six, nine, three, yes, eight zero three. Zero zero nine five one six nine three eight zero three. Okay, and response to is. Three, four, five, three. And there's. A it's, it's. A bad long, but could you ? What was the first digits again to me. Three, four, five, three. Three four five three three five five three. Zero. Three, five, five. Three no it's three, five three zero. Three, five, three, zero okay, seven, two, eight, nine. Two eight nine nine six two six. Nine six two six zero three two three. Zero three two three nine three four. Zero. Nine three. Zero eight eight four. Zero is four it's zero eight eight four. Zero [SILENCE] it's four two, one. One, one, two, one. I was pretty two zero is it for two, one at the end ? Two one. Okay, though, oh, number three, four, five, three, three, five, three, zero, seven, two, eight, nine, nine, six, two, six, zero, three, two, three, nine, three, four, zero, zero, eight, eight, four, two, one yes, what include one ? Okay, please. Yes, sir. Okay, i was talking to. Okay, bye. I would wait and see if you will succeed lookin. Okay. [SILENCE] [SILENCE] [SILENCE] [SILENCE] [SILENCE] So it's up to the other user is come up [SILENCE] something for me to put in the passport. When i put in seven one, two, three four yesterday with this one. Manage your password names and correct. I'm trying my own password. Yes, try way through your all the password. So what you said either. So it's going to take the new password ? Okay. No, we'll take any faster it's going to be. And what is ? Their message is received. Hm. What is the error message that you received ? And, you know, seven, one, two, three, four, [SILENCE] one, three, four. Cool. He was in my password was incorrect if you've got your passport easily, because the jersey password manager okay, can you try one more time with best with their, ah one, two, three, four, five with capital t. [SILENCE] Competency. Yes, yeah. And one, two, three, four, four, one, two, three, four. Okay. [SILENCE] [SILENCE] Okay, i think. It was. It seems to be, if it's going to pass it as a design error message saying, welcome, okay, so what's. The second one is what's really uses it for five cents. Okay, now we can't change your password with within you. Okay, okay, anything else ? How may i assist you with well, no that's. Perfect. Okay, thank you, thank you. Thanks for calling to cities this kevin easy. I have like, bye bye. Thank you, bye."""
    sentiment = get_sentiment(text)
    print(sentiment)
