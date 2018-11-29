#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import logging
import nltk

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

def tokanize_by_sentence(text):
    """Tokanize call transcript by sentences"""
    
    return sentences

def generate_path(base_path):
    """Generate open file path for each file in
    given directory if it is json file"""
    for item in os.listdir(base_path):
        if '.json' in item:
            with open(base_path+item, 'r') as file:
                data = json.load(file)
                yield data

        
    
if __name__ == '__main__':
##    import os
##    import json
##    base_path = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/comprehend/Promo/'
##    for data in generate_path(base_path):
##        text = data['text']
##        ref  = data['reference_number']
##        results = check_promotion_score(text)
##        print(f"{ref} -- Password score: {results['password_error_score']} -- Password triggers: {results['password_triggers']}")
##        print(f"{ref} -- Agent Promoted Score: {results['agent_promoted_score']} -- Agent triggers: {results['agent_triggers']}")
##        print(f"{ref} -- Promotion: {results['Promo']}")
##        print()
            


            
##    promo, words = check_promo(text)
##    score = check_promotion_score(text)
##    print(score)

    raw_text = """What kind of teva-service-desk the sense to speaking with you, miss survivor with their phone anymore ? Previous-ticket-number oh, i'm sorry, no. Ticket-number,
            i'm locked-me-out new-password oh, even. The windows. Yet. Okay, could you please tell-me or, uh, user name ? Uh, yeah, kind of linklater. Yeah. Oh. Yeah, okay. Okay,
            you're calling from star will stoneville correct ? Yes, okay, could you please tell me that i'm off here ? My steven wright. Okay, lemme see here one phone number of years.
            It's uh, one, four, one, six, two, nine, one, eight, eight, eight, eight. Correct. Okay, i take your account so. Mhm. Mhm. Oh! Yeah. Oh, mhm, yeah. Oh! Yes, she smoked a
            unlock it now. Okay, that would be good. Yes, my, you could try, though, okay, mhm. Oh, hm, hm. It looks like it's booting up good mhm. Okay, i am in. Thank you very much.
            Do you get you get number in florida ? Assistance for something else ? Million fine. Okay, as, uh, can i ask ? You haven't ever try to use our virtual-assistant
            teva-service-desk now, where you click on technology. I'm sorry say that again. Yes, it's. Uh, i wanna visit virtual-assistant it's all have a service now where you open
            the laws, do you see it on your rice side ? It says, you're crazy. It is something like child with us in real time. If you have, uh, for example, other issue i, which is
            there i have in the past, and, yeah, it's, just it's, really good, actually makes it very simple. Excellent. Okay, thank you, thank you, we should, i say, have a nice day,
            thank you, bye bye bye, right ?"""
    word_pattern = re.compile("(?:[a-zA-Z]+[-–’'`ʼ]?)*[a-zA-Z]+[’'`ʼ]?")
    words = word_pattern.findall(raw_text)
    # Normalise
    norm = set(w.lower() for w in words)
    print(norm)
