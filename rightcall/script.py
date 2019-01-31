#!/usr/bin/env python3
"""Checks how well the agent followed the script and hit certain
criteria such as:
    Appropriate Greeting - 'Hello and welcome to Teva service desk. My name is . Could I get your full name
    or previous ticket number?'
    First and last name taken
    Asked for phone number in case disconnected
    Asking location of caller
    Confirming name of caller's manager
    No shortcuts or abbreviations used in call"""
import promotion
import logging


def check_greeting(transcript, threshold=0.7):
    """Check if appropriate greeting was used in transcript"""
    logger = logging.getLogger(__name__)
    logger.debug(f"{check_greeting.__name__} : called with argument: {type(transcript)}")
    # 1. Get first sentence of transcript (as we know this will be the first thing said in the call)
    if type(transcript) is not dict:
        raise TypeError(f"Incorrect Data Type: {transcript.__name__} is of type {type(transcript)}")
    sentences = promotion.get_sentences(transcript)
    first_sentence = sentences[0]

    example_greeting = 'Hello and welcome to Teva service desk. My name is . Could I get your full name or previous ticket number?'

    # 2. Compare similarity of example and first sentence of transcript
    logger.debug(f"{check_greeting.__name__} : {type(first_sentence)} : {first_sentence}")
    logger.debug(f"{check_greeting.__name__} : {type(example_greeting)} : {example_greeting}")
    similarity = promotion.sentence_similarity(first_sentence, example_greeting)
    logger.debug(f"{check_greeting.__name__} : Similarity - {similarity} - Threshold - {threshold}")
    # 4. Return True if sufficiently similary
    if similarity >= threshold:
        return True
    else:
        return False


if __name__ == '__main__':
    logger = promotion.setupLogging('DEBUG', name=__name__)
    # base_path = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/transcripts/Promo/b76152TVd00246.json'
    # data = promotion.get_data(base_path)
    base_path = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/transcripts/Promo/'
    SIMILARITY_THRESHOLD = 0.1
    total_greetings, total_docs = promotion.check_directory(base_path, check_greeting, threshold=SIMILARITY_THRESHOLD)
    logger.info(f"Total successful greetings: {total_greetings} out of {total_docs} docs")
