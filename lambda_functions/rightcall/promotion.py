#!/usr/bin/env python3
import re
import logging
from math import sqrt
import os
import os.path
import json
from text import tokanize_aws_transcript
import nltk
from nltk import PorterStemmer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize


def setupEnv(env):
    """Append '/nltk_data' to nltk data path and return True
     if executing as lambda function
     """
    if env.get('AWS_EXECUTION_ENV') is not None:
        try:
            nltk.data.path.append('/nltk_data')
        except Exception as e:
            raise e
        else:
            return True
    else:
        return False


def setupLogging(LOGLEVEL='DEBUG', name=__name__, lambda_env=False):
    levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
    if LOGLEVEL not in levels:
        raise ValueError(f'Invalid log level choice {LOGLEVEL}')
    logger = logging.getLogger(name)
    logger.setLevel(LOGLEVEL)
    ch = logging.StreamHandler()
    ch.setLevel(LOGLEVEL)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    if not lambda_env:
        fh = logging.FileHandler(__file__.split('.')[0] + '.log')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger


def get_stems(sentence):
    """
    INPUT: string -- Input string
    OUTPUT: string -- stemmed
    Returns stemmed version of the input sentence"""
    word_pattern = re.compile("(?:[a-zA-Z]+[-–’'`ʼ]?)*[a-zA-Z]+[’'`ʼ]?")
    words = word_pattern.findall(sentence)
    porter_stemmer = PorterStemmer()
    stems = [porter_stemmer.stem(word) for word in words]
    stem_sentence = ' '.join(stems)
    return stem_sentence


def get_data(path):
    try:
        with open(path, 'r') as file:
            data = json.load(file)
            return data
    except Exception as err:
        logger.error(str(err))
        raise err


def generate_path(path):
    """Generate open file path for each file in
    given directory if it is json file
    CORRECT USAGE: for data in promotion.generate_path(path):
            //do stuff
    INCORRECT USAGE: data = promotion.generate_path(path)"""
    logger = logging.getLogger(__name__)
    logger.debug(f"{generate_path.__name__} Called with '{path}'")

    if '.json' in path:  # Single file
        logger.debug(f"{generate_path.__name__}: Single file at this path")
        logger.debug(f"{generate_path.__name__}: Returning data")
        return get_data(path)
    else:  # Directory of files
        logger.debug(f"{generate_path.__name__}: Directory file at this path")
        for item in os.listdir(path):
            logger.debug(f"{generate_path.__name__}: Working on {item}")
            if '.json' in item:
                yield get_data(path + item)


def bagofwords(sentence_words, vocab):
    """Given tokenized, words and a vocab
    Returns term frequency array"""
    bag = [0] * len(vocab)
    for sw in sentence_words:
        for i, word in enumerate(vocab):
            if word == sw:
                bag[i] += 1
    return bag


def preprocess(raw_sentence):
    """Do all preprocessing of text:
        Tokenize words
        Stem words
        Remove stop words
    INPUT: string
    OUPUT: list of strings"""
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
    """Returns the normalized version of input vector
    INPUT: list
    OUTPUT: list
    """
    if type(tf_vector) is not list:
        raise ValueError(f"Incorrect parameter type: {type(tf_vector)}")
    if not any(tf_vector):
        return False
    tf_sum_squares = sum([x**2 for x in tf_vector])
    square_root = sqrt(tf_sum_squares)
    normalized_vector = [x / square_root for x in tf_vector]
    return normalized_vector


def dot_prod(a, b):
    """Returns the dot vector product of two lists of numbers"""
    if type(a) is not list:
        raise ValueError(f"Incorrect parameter type: {type(a)}")
    elif type(b) is not list:
        raise ValueError(f"Incorrect parameter type: {type(b)}")
    else:
        return sum([a[i] * b[i] for i in range(len(b))])


def construct_vocab(words1, words2):
    """Combines words from two sentences into a single
        dictionary and assigns weighting of 1 to all words except
        'virtual-assist' which should get 2
        NOTE: weighting is unused by rest of program. Should be removed
        Input: words1 - List of strings
               words2 - List of strings
        Output: vocab - dictionary where key is word,
                            value is weighting given two word
    """
    if type(words1) is not list:
        raise ValueError(f"Incorrect parameter type: {type(words1)}")
    if type(words2) is not list:
        raise ValueError(f"Incorrect parameter type: {type(words2)}")
    # Gives 'virtual-assist' key a higher weighting
    # Weighting in dict is unused by program should be removed
    vocab = {w: (2 if w in ['virtual-assist'] else 1) for w in words1 + words2}
    return vocab


def get_sentences(data):
    """return a list of sentences
    given a json document, a list or
    a single string"""
    logger = logging.getLogger()
    sentences = []
    if type(data) is dict:
        transcript_name = data['jobName'].split('--')[0]
        logger.info(f"{get_sentences.__name__} : {transcript_name}")
        # Custom sentence tokanizer using AWS transcribe data
        transcribe_sents = tokanize_aws_transcript(data)
        sentences = [sentence['text'] for sentence in transcribe_sents]

    elif type(data) is list:
        for sentence in data:
            sentences = [{'text': data[i]} for i, text in enumerate(data)]

    elif type(data) is str:
        sentences.append({'text': data})

    else:
        logger.error(f'{get_sentences.__name__}: TypeError')
        raise TypeError(f'Unknown input type: {type(data)}')
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
    similarity = dot_prod(tf_words1_norm, tf_words2_norm)
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
    """Returns True if similarity between document and
    keywords string score above the threshold.
        INPUT: document (dict, list or string) - transcript text
               keywords_string (string) - combined from desired keywords
              threshold - (float)
        OUTPUT: boolean"""
    logger = logging.getLogger(__name__)
    logger.debug(f"{document_similarity.__name__} : called with {type(document)}, {type(keywords_string)}, {type(threshold)}")
    if type(document) not in [dict, list, str]:
        raise TypeError(f"{document_similarity.__name__} : Invalid argument: {type(document)}")
    elif type(keywords_string) is not str:
        raise TypeError(f"{document_similarity.__name__} : Invalid argument: {type(keywords_string)}")
    sentences = get_sentences(document)
    similarity_sum = 0
    VA_BOOST = threshold
    COUNT_BOOST = 0.2
    num_sentence_hits = 0
    virtual_assistant_hit = False
    unique_word_hits = set()

    for sentence in sentences:
        # For each sentence in data, get its cosine similarity to promo_set
        similarity = sentence_similarity(sentence, keywords_string)
        if similarity > 0:
            num_sentence_hits += 1
            logger.debug(f'{document_similarity.__name__} : Similarity Score: {similarity} -- Sentence: {sentence}')
            # Summing similarity scores for each sentence to get score for
            # entire document (may reintroduce effect of longer sentences
            # having higher score that normalizing was supposed to eliminate
            similarity_sum += similarity
            if 'virtual-assistant' in sentence and virtual_assistant_hit is False:
                virtual_assistant_hit = True
                logger.debug(f'{document_similarity.__name__} : "virtual-assistant" detected. Increasing similarity sum by {VA_BOOST}')
                similarity_sum += VA_BOOST

            count, hits = count_hits_in_sentence(sentence, keywords_string)
            unique_word_hits.update(hits)

    # If low number of sentences hits and no virtual assistant detected,
    # add extra boost based on number of individual word hits
    if num_sentence_hits <= 3 and virtual_assistant_hit is False:
        logger.debug(f'{document_similarity.__name__} : Sentence Hits 2 or less and no virtual_assistant detected')
        logger.debug(f'{document_similarity.__name__} : Document Unique Word Hit Count - {len(unique_word_hits)} - Words - {unique_word_hits} - Increasing similarity sum by {len(unique_word_hits) * COUNT_BOOST}')
        similarity_sum += len(unique_word_hits) * COUNT_BOOST

    logger.info(f'{document_similarity.__name__} : Similarity Sum for file -  {similarity_sum} - Threshold: {threshold}')
    if similarity_sum > threshold:
        return True
    else:
        return False


def Promotion(transcript_text, return_bool=False):
    """Lambda entry point
    If 'return_bool' is True this function returns True or False
    If False, this function returns a string 'success' or 'none'"""
    # LOGLEVEL = 'DEBUG'
    logger = logging.getLogger(__name__)
    logger.debug(f"{Promotion.__name__} : return_bool - {return_bool}")
    smaller_promo_words = ['technology', 'tool', 'virtual-assistant',
                           'new-tool', 'ask-chat', 'chat', 'chat-with-us',
                           'pink-button']

    promo_sent = ' '.join(smaller_promo_words)
    SIMILARITY_THRESHOLD = 0.4
    promotion = document_similarity(transcript_text,
                                    promo_sent,
                                    SIMILARITY_THRESHOLD)
    if return_bool:
        return promotion
    else:
        results = {}
        if promotion:
            results['Promo'] = 'success'
        else:
            results['Promo'] = 'none'
        logger.info(f"{Promotion.__name__} : Results - {results['Promo']}")
        return results


def check_directory(dir_path, function, *args, **kwargs):
    """Given a directory and a function with a True/False return value,
    applies it to all files in directory keeping track of results """
    logger = logging.getLogger(__name__)
    logger.debug(f"{check_directory.__name__} : Called with '{dir_path}', and function '{function.__name__}'")
    path = str(dir_path)
    total_hits = 0
    total_docs = 0
    logger.debug(f"{path} is directory")
    for data in generate_path(path):
        result = function(data, *args, **kwargs)
        logger.debug(f"Result: {result}, {type(result)}")
        if result:
            total_hits += 1
        total_docs += 1
    return total_hits, total_docs


if __name__ == '__main__':
    # Single file
    logger = setupLogging('DEBUG', name=__name__)
    # transcript = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/transcripts/Promo/b76152TVd00246.json'
    # with open(transcript, 'r') as file:
    #     data = json.load(file)
    # promo = Promotion(data)
    # logger.debug(promo)

    # Directory with files
    base_path = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/transcripts/Promo/'
    total_promo, total_docs = check_directory(base_path, Promotion, return_bool=True)
    logger.info(f'Total Matches - {total_promo} out of {total_docs} files')
