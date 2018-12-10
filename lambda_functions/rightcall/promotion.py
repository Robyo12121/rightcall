#!/usr/bin/env python3
import re
import logging
import numpy as np
from math import sqrt
import sys
import os
import json
import nltk
from nltk import PorterStemmer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

if os.environ.get("AWS_EXECUTION_ENV") is not None:
    nltk.data.path.append("/nltk_data")
    from text import tokanize_aws_transcript






def setupLogging(LOGLEVEL):
    levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    if LOGLEVEL not in levels:
        raise ValueError(f"Invalid log level choice {LOGLEVEL}")
    logger = logging.getLogger(__name__)
    logger.setLevel(LOGLEVEL)
    # create console handler and set level to LOGLEVEL
    ch = logging.StreamHandler()
    ch.setLevel(LOGLEVEL)
    # create file handler and set level to DEBUG
    fh = logging.FileHandler(__file__.split('.')[0] + '.log')
    fh.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
    # add formatter to ch
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)
    # add ch to logger
    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


def get_stems(sentence):
    """Given a sentence in a string:
    Get all individual words using regex then stem those words
    using PorterStemmer from NLTK
    Return: List of stems of words"""
    word_pattern = re.compile("(?:[a-zA-Z]+[-–’'`ʼ]?)*[a-zA-Z]+[’'`ʼ]?")
    words = word_pattern.findall(sentence)
    porter_stemmer = PorterStemmer()
    stems = [porter_stemmer.stem(word) for word in words]
    stem_sentence = ' '.join(stems)
    return stem_sentence


def generate_path(path):
    """Generate open file path for each file in
    given directory if it is json file"""
    if '.json' in path:
            try:
                with open(path, 'r') as file:
                    data = json.load(file)
                    yield data
            except Exception as err:
                logger.error(str(err))
                raise err
    else:
        for item in os.listdir(path):
            logger.debug(item)
            if '.json' in item:
                try:
                    with open(path+item, 'r') as file:
                        data = json.load(file)
                        yield data
                except Exception as err:
                    logger.error(str(err))
                    raise err


def bagofwords(sentence_words, vocab):
    """Given tokenized, words and a vocab
    Checks if those words occur in vocab
    Returns term frequency array"""
    bag = np.zeros(len(vocab))
    for sw in sentence_words:
        for i, word in enumerate(vocab):
            if word == sw:
                bag[i] += 1
    return np.array(bag)


def preprocess(raw_sentence):
    """Do all preprocessing of text:
        Tokenize words
        Stem words
        Remove stop words"""
    # Get words
    tokens = word_tokenize(raw_sentence)
    words = [w.lower() for w in tokens]
    # Stem words
    porter_stemmer = PorterStemmer()
    stems = [porter_stemmer.stem(word) for word in words]
    # remove stop words
    stop_words = set(stopwords.words('english'))
    filtered_words = [w for w in stems if w not in stop_words]
    word_pattern = re.compile("(?:[a-zA-Z]+[-–’'`ʼ]?)*[a-zA-Z]+[’'`ʼ]?")
    filtered_words = word_pattern.findall(' '.join(filtered_words))
    return filtered_words


def normalize_tf(tf_vector):
    if not any(tf_vector):
        return False
    tf_sum_squares = sum([x**2 for x in tf_vector])
    square_root = sqrt(tf_sum_squares)
    normalized_vector = [x / square_root for x in tf_vector]
    return normalized_vector


def calculate_cosine_similarity(norm_vec_a, norm_vec_b):
    """Just dot product if vectors are normalized"""
    cosine_similarity = np.dot(norm_vec_a, norm_vec_b)
    return cosine_similarity


def construct_vocab(words1, words2):
    """Combines words from two sentences into a single
        dictionary
        Input: words1 - List of strings
               words2 - List of strings
        Output: vocab - dictionary where key is word,
                            value is weight of word"""
    all_words = set(words1 + words2)
    vocab = {}
    for word in all_words:
        # Give words in this list higher weighting as it almost certainly means a promotion
        if word in ['virtual-assist']:
            vocab[word] = 2
        else:
            vocab[word] = 1
    return vocab


def get_sentences(data):
    """return a list of sentences
    given a json document, a list or
    a single string"""
    logger = logging.getLogger()
    sentences = []
    if type(data) is dict:
        transcript_name = data['jobName'].split('--')[0]
        logger.info(transcript_name)
        # Custom sentence tokanizer using AWS transcribe data
        transcribe_sents = tokanize_aws_transcript(data)
        sentences = [sentence['text'] for sentence in transcribe_sents]

    elif type(data) is list:
        for sentence in data:
            sentences = [{'text': data[i]} for i, text in enumerate(data)]

    elif type(data) is str:
        sentences.append({'text': data})

    else:
        logger.error("TypeError")
        return False
    return sentences


def sentence_similarity(sentence, keywords):
    # Preprocess both items (tokenize, stem, stopwords, lower-case)
    words1 = preprocess(sentence)
    words2 = preprocess(keywords)
    if not words1:
        return 0
    # Create vocab containing terms of both sentences
    vocab = construct_vocab(words1, words2)
    # Create term frequency vectors of each sentence compared to the vocabulary
    tf_words1 = bagofwords(words1, vocab.keys())
    tf_words2 = bagofwords(words2, vocab.keys())

    # Normalize vectors so all elements add up to 1 (eliminates effect of longer sentences)
    tf_words1_norm = normalize_tf(tf_words1)
    tf_words2_norm = normalize_tf(tf_words2)
    # Calculate Cosine Similarity between two vectors
    similarity = calculate_cosine_similarity(tf_words1_norm, tf_words2_norm)
    return similarity


def count_hits_in_sentence(sentence, keywords_string):
    words = preprocess(sentence)
    keywords = preprocess(keywords_string)
    count = 0
    hits = []
    for keyword in keywords:
        if keyword in words:
            count += 1
            hits.append(keyword)
    return count, hits


def document_similarity(document, keywords_string, threshold=0.4):

##    logger = logging.getLogger()
    sentences = get_sentences(document)
    similarity_sum = 0
    VA_BOOST = threshold
    COUNT_BOOST = 0.2
    num_sentence_hits = 0
    unique_word_hit_count = 0
    virtual_assistant_hit = False
    unique_word_hits = set()

    for sentence in sentences:
        # For each sentence in data, get its cosine similarity to promo_set
        similarity = sentence_similarity(sentence, keywords_string)
        if similarity > 0:
            num_sentence_hits += 1
            logger.debug(f"Similarity Score: {similarity} -- Sentence: {sentence}")
            # Summing similarity scores for each sentence to get score for entire document (may reintroduce effect of longer sentences having higher score
            # that normalizing was supposed to eliminate
            similarity_sum += similarity
            if 'virtual-assistant' in sentence and virtual_assistant_hit is False:
                virtual_assistant_hit = True
                logger.debug(f"'virtual-assistant' detected. Increasing similarity sum by {VA_BOOST}")
                similarity_sum += VA_BOOST

            count, hits = count_hits_in_sentence(sentence, keywords_string)
            unique_word_hits.update(hits)

    # If low number of sentences hits and no virtual assistant detected, add extra boost
    # based on number of individual word hits
    if num_sentence_hits <= 3 and virtual_assistant_hit is False:
        logger.debug("Sentence Hits 2 or less and no virtual_assistant detected")
        logger.debug(f"Document Unique Word Hit Count: {len(unique_word_hits)} - Words : {unique_word_hits} - Increasing similarity sum by {len(unique_word_hits) * COUNT_BOOST}")
        similarity_sum += len(unique_word_hits) * COUNT_BOOST

    logger.info(f"Similarity Sum for file: {similarity_sum} - Threshold: {threshold}")
    if similarity_sum > threshold:
        return True
    else:
        return False


def Promotion(transcript_text):
    LOGLEVEL = 'DEBUG'
    smaller_promo_words = ['technology', 'tool', 'virtual-assistant',
                           'new-tool', 'ask-chat', 'chat', 'chat-with-us', 'pink-button']

    promo_sent = ' '.join(smaller_promo_words)
    SIMILARITY_THRESHOLD = 0.4
    promotion = document_similarity(transcript_text, promo_sent, SIMILARITY_THRESHOLD)
    results = {}
    if promotion:
        results['Promo'] = 'success'
    else:
        results['Promo'] = 'none'
    return results


if __name__ == '__main__':
    logger = setupLogging('DEBUG')
    base_path = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/transcripts/test/'

##    promo_words = ['virtual', 'chat', 'technology', 'tool',
##                   'vehicle', 'virtual agent', 'virtual-assistant',
##                     'new-tool', 'ask-chat', 'chat', 'chat-with-us',
##                   'pink-button', 'ask-I.T.']

##    # Keep this intact - do not modify
    promo_words = ['technology', 'tool', 'virtual-assistant',
                           'new-tool', 'ask-chat', 'chat', 'chat-with-us', 'pink-button']

    promo_sent = ' '.join(promo_words)
    SIMILARITY_THRESHOLD = 0.4
    total_promos = 0
    total_docs = 0
    for data in generate_path(base_path):
        print()
        promotion = document_similarity(data, promo_sent, SIMILARITY_THRESHOLD)
        logger.info(f"Promotion: {promotion}")
        if promotion:
            total_promos += 1
        total_docs += 1
    logger.info(f"Total Matches: {total_promos} out of {total_docs} files")
