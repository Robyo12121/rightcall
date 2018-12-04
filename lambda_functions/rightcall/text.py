#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import logging



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
    password_problem_phrases = ['reset password', 'new-password','locked-me-out','password reset',
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

    

    password_problem_phrases = ['reset password', 'new-password','locked-me-out','password-reset',
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






