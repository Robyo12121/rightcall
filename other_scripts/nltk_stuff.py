import re
import logging
import numpy as np
from math import sqrt
import sys

sys.path.append('../')
from lambda_functions.rightcall.text import tokanize_aws_transcript

from nltk import PorterStemmer, WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.probability import FreqDist

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity



def get_vectors(*strs):
    text = [t for t in strs]
    vectorizer = CountVectorizer(text)
    vectorizer.fit(text)
    return vectorizer.transform(text).toarray()


def get_stems(sentence):
    word_pattern = re.compile("(?:[a-zA-Z]+[-–’'`ʼ]?)*[a-zA-Z]+[’'`ʼ]?")
    words = word_pattern.findall(sentence)

    porter_stemmer = nltk.PorterStemmer()
    stems = [porter_stemmer.stem(word) for word in words]
    stem_sentence = ' '.join(stems) 
    return stem_sentence


def get_lemmas(sentence):
    wordnet_lemmatizer = nltk.WordNetLemmatizer()
    word_pattern = re.compile("(?:[a-zA-Z]+[-–’'`ʼ]?)*[a-zA-Z]+[’'`ʼ]?")
    words = word_pattern.findall(sentence)
    lemmas = [wordnet_lemmatizer.lemmatize(word) for word in words]
    return lemmas


def generate_path(base_path):
    """Generate open file path for each file in
    given directory if it is json file"""
    if '.' in base_path:
        if '.json' in base_path:
            with open(base_path, 'r') as file:
                data = json.load(file)
                yield data
    else:
        for item in os.listdir(base_path):
            if '.json' in item:
                with open(base_path+item, 'r') as file:
                    data = json.load(file)
                    yield data

def bagofwords(sentence_words, vocab):
    """Given tokenized, words and a vocab
    Checks if those words occur in vocab
    Returns term frequency array"""
    bag = np.zeros(len(vocab))
    for sw in sentence_words:
        for i,word in enumerate(vocab):
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
    filtered_words = [w for w in stems if not w in stop_words]
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
    all_words = set(words1 + words2)
    vocab={}
    for i,word in enumerate(all_words):
        vocab[word] = i
    return vocab


def get_sentences(data):
    """return a list of sentences
    given a json document, a list or
    a single string"""
    sentences = []
    
    if type(data) is dict:
        transcript_name = data['jobName'].split('--')[0]
        print()
        print(transcript_name)
        # Custom sentence tokanizer using AWS transcribe data
        transcribe_sents = tokanize_aws_transcript(data)
        sentences = [sentence['text'] for sentence in transcribe_sents]
        
    elif type(data) is list:
        for sentence in data:
            # Default NLTK sentence tokenizer
            sentences = [{'text': data[i]} for i, text in enumerate(data)]
            
    elif type(data) is str:
        sentences.append({'text': data})
        
    else:
        print("TypeError")
        return False
    return sentences


def process(sent1, sent2, debug=False):
    # Preprocess both items (tokenize, stem, stopwords, lower-case)
    sent1 = preprocess(sent1)
    sent2 = preprocess(sent2)
    if not sent1 or not sent2:
        return 0

    # Create vocab containing terms of both sentences
    vocab = construct_vocab(sent1, sent2)

    # Create term frequency vectors of each sentence compared to the vocabulary
    tf_sent1 = bagofwords(sent1, vocab.keys())
    tf_sent2 = bagofwords(sent2, vocab.keys())

    # Normalize vectors so all elements add up to 1 (eliminates effect of longer sentences)
    tf_sent1_norm = normalize_tf(tf_sent1)
    tf_sent2_norm = normalize_tf(tf_sent2)
    # Calculate Cosine Similarity between two vectors
    similarity = calculate_cosine_similarity(tf_sent1_norm, tf_sent2_norm)
    if debug:
        print(f"Sent 1: {sent1}")
        print(f"Sent 2: {sent2}")
        print(vocab)
        print(f"TF of Sent 1: {tf_sent1}")
        print(f"TF of Sent 2: {tf_sent2}")        
    return similarity
    

    
if __name__ == '__main__':
    import os
    import json
##    base_path = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/transcripts/b76152TVd00246.json'  
    base_path = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/transcripts/'
    promo_words = ['virtual', 'agent', 'chat', 'technology', 'service-now', 'tool',
                   'vehicle', 'virtual assistant', 'virtual agent', 'virtual-assistant',
                     'new-tool', 'ask-chat', 'ask chat', 'ask-i', 'ask-it', 'chat-with-us',
                   'contact']

    promo_sent = ' '.join(promo_words)
   

##    test = 'Have you ever tried contacting our virtual assistant?'
##    similarity = process(test, promo_sent)
##    print(f"Similarity: {similarity}")
    
    SIMILARITY_THRESHOLD = 0.06
    
    for data in generate_path(base_path):
        hits = []
        sentences = get_sentences(data)
        
        for sentence in sentences:
            print(f"Raw Text: {sentence}")
            # For each sentence in data, get its cosine similarity to promo_set
            similarity = process(sentence, promo_sent, debug=False)
            print(f"Similarity Score: {similarity}")
            if similarity >= SIMILARITY_THRESHOLD:
                hits.append(sentence)

        print()
        print(hits)
    


