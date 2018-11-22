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

if __name__ == '__main__':
    text = """What kind of teva-service-desk the sense to speaking with you, miss survivor with their phone anymore ? Previous-ticket-number oh, i'm sorry, no. Ticket-number, i'm locked-me-out new-password oh, even. The windows. Yet. Okay, could you please tell-me or, uh, user name ? Uh, yeah, kind of linklater. Yeah. Oh. Yeah, okay. Okay, you're calling from star will stoneville correct ? Yes, okay, could you please tell me that i'm off here ? My steven wright. Okay, lemme see here one phone number of years. It's uh, one, four, one, six, two, nine, one, eight, eight, eight, eight. Correct. Okay, i take your account so. Mhm. Mhm. Oh! Yeah. Oh, mhm, yeah. Oh! Yes, she smoked a unlock it now. Okay, that would be good. Yes, my, you could try, though, okay, mhm. Oh, hm, hm. It looks like it's booting up good mhm. Okay, i am in. Thank you very much. Do you get you get number in florida ? Assistance for something else ? Million fine. Okay, as, uh, can i ask ? You haven't ever try to use our virtual-assistant teva-service-desk now, where you click on technology. I'm sorry say that again. Yes, it's. Uh, i wanna visit virtual-assistant it's all have a service now where you open the laws, do you see it on your rice side ? It says, you're crazy. It is something like child with us in real time. If you have, uh, for example, other issue i, which is there i have in the past, and, yeah, it's, just it's, really good, actually makes it very simple. Excellent. Okay, thank you, thank you, we should, i say, have a nice day, thank you, bye bye bye, right ?"""

    promo, words = check_promo(text)
    print(words)
