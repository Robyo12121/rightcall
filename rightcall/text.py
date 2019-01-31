#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
# import logging
import os
import json


def clean(input_list, exclude_list=[]):
    """Returns list with  digits, numberwords and useless words from list of
        key phrases removed
        Inputs:
            input_list -- List of strings containing key phrases to be processed
            exclude_list -- List of strings containing words to be removed
        Output:
            output_list -- List of key phrases with words from exclude_list removed


    """
    nums = ['zero', 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten'
            'eleven', 'twelve', 'thirteen', 'fourteen', 'fifteen', 'sixteen', 'seventeen'
            'eighteen', 'nineteen', 'twenty', 'thirty', 'fourty', 'fifty', 'sixty',
            'seventy', 'eighty', 'ninty', 'hundred']
    stopwords = ['mhm', 'yeah', 'um', 'ah', 'a', 'the', 'o.k.', 'your', 'hm', 'oh'
                 'okay', 'my', 'that', 'i', 'alright', 'bye', 'uh', 'i i', 'oh']
    if not exclude_list:
        exclude_list = nums + stopwords
    output_list = []
    for phrase in input_list[:]:
        split_phrase = phrase.split(' ')
        for word in split_phrase[:]:
            if word.lower() in exclude_list or word.isdigit():
                split_phrase.remove(word)
        new_phrase = ' '.join(split_phrase)
        if new_phrase is '':
            continue
        else:
            output_list.append(new_phrase.lower())
    return output_list


def check_promo(text):
    """Inspects text and returns an inference of the promotion
    ('success', 'none', 'fail').
    Input:
        text -- UTF-8 text string (required | type: str).
    Output:
        promo -- promotion: 'success', 'none', 'fail' (type: str).

    """
    promo = 'none'
    password_problem_phrases = ['reset password', 'new-password', 'locked-me-out', 'password reset',
                                'forgot password', 'forgot my password', 'currently-locked-out', 'password-expired']
    password_problem_words = ['password', 'pass', 'parole', 'reset', 'locked', 'expired']

    promo_phrases = ['virtual assistant', 'virtual agent', 'virtual-assistant', 'vehicle assistance',
                     'new-tool', 'ask-chat', 'ask chat']
    promo_words = ['virtual', 'assistant', 'agent', 'chat']

    word_pattern = re.compile("(?:[a-zA-Z]+[-–’'`ʼ]?)*[a-zA-Z]+[’'`ʼ]?")
    words = word_pattern.findall(text)

    for item in password_problem_phrases:
        if item in text:
            promo = 'fail'
            break
    if promo == 'none':
        for item in password_problem_words:
            if item in words:
                promo = 'fail'
                break
    # Removed if promo == 'fail' so that it always checks as agents can promote virtual assistant even if the callers problem was not password related
    for item in promo_phrases:
        if item in text:
            promo = 'success'
            break
    for item in promo_words:
        if item in words:
            promo = 'success'
            break

    results = {}
    results['Promo'] = promo
    return results


def check_promotion_score(text):
    """Inspects text and returns an inference of the promotion
    ('success', 'none', 'fail').
    Input:
        text -- UTF-8 text string (required | type: str).
    Output:
        promo -- promotion: 'success', 'none', 'fail' (type: str).

    """
    results = {}
    results['password_error_score'] = 0
    results['agent_promoted_score'] = 0
    results['Promo'] = 'none'
    PHRASE_AMOUNT = 1
    WORD_AMOUNT = 1

    PHRASE_MULTIPLIER = 10
    WORD_MULTIPLIER = 5

    PASSWORD_THRESHOLD = 10
    PROMO_THRESHOLD = 15

    password_problem_phrases = ['reset password', 'new-password', 'locked-me-out', 'password-reset',
                                'forgot password', 'forgot-my-password', 'currently-locked-out', 'password-expired',
                                'change-your-password', 'temporary-one']
    password_problem_words = ['password', 'pass', 'parole', 'reset', 'locked', 'expired',
                              'temporary', 'forgot']

    promo_phrases = ['virtual assistant', 'virtual agent', 'virtual-assistant',
                     'new-tool', 'ask-chat', 'ask chat', 'ask-i', 'ask-it', 'chat-with-us']

    promo_words = ['virtual', 'assistant', 'agent', 'chat', 'technology', 'service-now', 'tool',
                   'vehicle', 'assistance']

    word_pattern = re.compile("(?:[a-zA-Z]+[-–’'`ʼ]?)*[a-zA-Z]+[’'`ʼ]?")
    words = word_pattern.findall(text)
    words = [x.lower() for x in words]
    results['password_triggers'] = []
    results['agent_triggers'] = []

    for item in password_problem_phrases:
        if item in text:
            results['password_triggers'].append(item)
            results['password_error_score'] += PHRASE_AMOUNT * PHRASE_MULTIPLIER

    for item in password_problem_words:
        if item in words:
            results['password_triggers'].append(item)
            results['password_error_score'] += WORD_AMOUNT * WORD_MULTIPLIER

    for item in promo_phrases:
        if item in text:
            results['agent_triggers'].append(item)
            results['agent_promoted_score'] += PHRASE_AMOUNT * PHRASE_MULTIPLIER

    for item in promo_words:
        if item in words:
            results['agent_triggers'].append(item)
            results['agent_promoted_score'] += WORD_AMOUNT * WORD_MULTIPLIER

    if results['password_error_score'] >= PASSWORD_THRESHOLD:
        results['Promo'] = 'fail'
    if results['agent_promoted_score'] >= PROMO_THRESHOLD:
        results['Promo'] = 'success'
    return results


def tokanize_aws_transcript(transcribe_data):
    """Divides transcript into sentences based on end of sentence punctuation,
    the speaker changing, a gap without a word spoken exceeds the threshold or
    the size limit threshold is reached.

    Input: AWS Transcribe input json document:
            {'results': {
                    'transcripts': [],
                    'items': [{'start_time': }]
                    'speaker_labels': {
                        'segments': []

    Output: """
    # transcript = transcribe_data['results']['transcripts'][0]
    items = transcribe_data['results']['items']
    contents = ""
    # timedata = []

    prevEndTime = -1
    paragraphGap = 1.5
    prevStartTime = -1
    newParagraph = False
    prevSpeaker = 'spk_0'

    hasSpeakerLabels = False
    speakerMapping = []

    if 'speaker_labels' in transcribe_data['results']:
        hasSpeakerLabels = True
        for i in range(len(transcribe_data['results']['speaker_labels']['segments'])):
            speakerLabel = transcribe_data['results']['speaker_labels']['segments'][i]
            speakerMapping.append({"speakerLabel": speakerLabel['speaker_label'],
                                   "endTime": float(speakerLabel['end_time'])})

    speakerIndex = 0

    retval = []

    # Repeat the loop for each item (word and punctuation)
    # The transcription will be broken out into a number of sections that are referred to
    # below as paragraphs.It is broken out by punctionation, speaker changes, a long pause
    # in the audio, or overall length
    for i in range(len(items)):
        reason = ""

        if items[i]['type'] == 'punctuation':
            if items[i]["alternatives"][0]["content"] == '.':
                newParagraph = True

            contents += items[i]["alternatives"][0]["content"]

        # Add the start time to the string -> timedata
        if 'start_time' in items[i]:
            speakerLabel = 'spk_0'

            if prevStartTime == -1:
                prevStartTime = float(items[i]["start_time"])

            # gap refers to the amount of time between spoken words
            gap = float(items[i]["start_time"]) - prevEndTime

            if hasSpeakerLabels:
                while speakerIndex < (len(speakerMapping) - 1) and speakerMapping[speakerIndex + 1]['endTime'] < float(items[i]["start_time"]):
                    speakerIndex += 1

                speakerLabel = speakerMapping[speakerIndex]['speakerLabel']

            # Change paragraphs if the speaker changes
            if speakerLabel != prevSpeaker:
                newParagraph = True
                reason = "Speaker Change from " + prevSpeaker + " to " + speakerLabel
            # the gap exceeds a preset threshold
            elif gap > paragraphGap:
                newParagraph = True
                reason = "Time gap"
            # There are over 4900 words (The limit for comprehend is 5000)
            elif len(contents) > 4900:
                newParagraph = True
                reason = "Long paragraph"
            else:
                newParagraph = False

            if prevEndTime != -1 and newParagraph:

                # append the block of text to the array. Call comprehend to get
                # the keyword tags for this block of text
                retval.append({
                    "startTime": prevStartTime,
                    "endTime": prevEndTime,
                    "text": contents,
                    "gap": gap,
                    "reason": reason,
                    "speaker": prevSpeaker,
                    "len": len(contents)
                })
                # Reset the contents and the time mapping
                # print('paragraph:' + contents)
                contents = ""
                # timedata = []
                prevEndTime = -1
                prevStartTime = -1
                newParagraph = False
            else:
                prevEndTime = float(items[i]["end_time"])

            prevSpeaker = speakerLabel

            # If the contents is not empty, prepend a space
            if contents != "":
                contents += " "

            # Always assume the first guess is right.
            word = items[i]["alternatives"][0]["content"]

            contents += word

    return retval


def generate_path(path):
    """Generate open file path for each file in
    given directory if it is json file"""
    if '.json' in path:
            try:
                with open(path, 'r') as file:
                    data = json.load(file)
                    yield data
            except Exception as err:
                raise err
    else:
        for item in os.listdir(path):
            if '.json' in item:
                try:
                    with open(path + item, 'r') as file:
                        data = json.load(file)
                        yield data
                except Exception as err:
                    raise err


if __name__ == '__main__':
    base_path = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/transcripts/test/'
    total_docs = 0
    total_promos = 0
    for data in generate_path(base_path):
        print()
        results = check_promotion_score(data['results']['transcripts'][0]['transcript'])
        print(results['Promo'])
        if results['Promo'] == 'success':
            total_promos += 1
        total_docs += 1
    print(f"Total Promotions: {total_promos} out of {total_docs} files")
