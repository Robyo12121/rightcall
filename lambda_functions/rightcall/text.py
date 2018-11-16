#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import logging

def check_promo(text):
    """Inspects text and returns an inference of the promotion
    ('success', 'none', 'fail').
    Input:
        text -- UTF-8 text string (required | type: str).
    Output:
        promo -- promotion: 'success', 'none', 'fail' (type: str).

    """

    promo = 'none'
    context_multi = ['reset password', 'password reset', 'forgot password',
                     'forgot my password']
    context_single = ['password', 'pass', 'parole', 'reset']
    promo_multi = ['virtual assistant', 'virtual agent']
    promo_single = ['virtual', 'assistant', 'agent']
    word_pattern = re.compile("(?:[a-zA-Z]+[-–’'`ʼ]?)*[a-zA-Z]+[’'`ʼ]?")
    words = word_pattern.findall(text)
    # Check `context_single`
    for el in context_multi:
        if el in text:
            promo = 'fail'
            break
    if promo == 'none':
        # Check `context_single`
        for el in context_single:
            if el in words:
                promo = 'fail'
                break
    # Check `promo_multi`
    if promo == 'fail':
        for el in promo_multi:
            if el in text:
                promo = 'success'
                break
    # Check `promo_single`
    if promo == 'fail':
        for el in promo_single:
            if el in words:
                promo = 'success'
                break
    return promo


def clean(input_list, exclude_list=[]):
    """Returns lits with  digits, numberwords and useless words from list of
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
    useless = ['mhm', 'yeah', 'um', 'ah', 'a', 'the', 'o.k.', 'your', 'hm', 'oh'
               'okay', 'my', 'that', 'i', 'alright', 'bye', 'uh', 'i i', 'oh']
    if not exclude_list:
        exclude_list = nums + useless
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




