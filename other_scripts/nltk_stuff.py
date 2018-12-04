import re
import logging
import numpy as np
from math import sqrt
from nltk import PorterStemmer, WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.probability import FreqDist

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

def tokanize_sentences(transcribe_data):
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
    transcript = transcribe_data['results']['transcripts'][0]
    items = transcribe_data['results']['items']
    contents = ""
    timedata = []

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
                while speakerIndex < (len(speakerMapping) - 1) and speakerMapping[speakerIndex + 1]['endTime'] < \
                      float(items[i]["start_time"]):

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
                timedata = []
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

def reassemble_sentence(word_list):
    if type(word_list) is not 'list':
        return F

def get_lemmas(sentence):
    wordnet_lemmatizer = nltk.WordNetLemmatizer()
    word_pattern = re.compile("(?:[a-zA-Z]+[-–’'`ʼ]?)*[a-zA-Z]+[’'`ʼ]?")
    words = word_pattern.findall(sentence)
    lemmas = [wordnet_lemmatizer.lemmatize(word) for word in words]
    return lemmas


def generate_path(base_path):
    """Generate open file path for each file in
    given directory if it is json file"""
    for item in os.listdir(base_path):
        if '.json' in item:
            with open(base_path+item, 'r') as file:
                data = json.load(file)
                yield data

def bagofwords(sentence_words, vocab):
    """Give tokenized, words and a vocab
    Checks if those words occur in vocab
    Returns term frequency array"""
    bag = np.zeros(len(vocab))
    for sw in sentence_words:
        for i,word in enumerate(vocab):
            if word == sw:
                bag[i] += 1
    return np.array(bag)

def preprocess(raw_sentence):
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

       
    
if __name__ == '__main__':
    import os
    import json
    base_path = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/transcripts/'

##    password_words = ['password', 'pass', 'parole', 'reset', 'locked', 'expired',
##                              'temporary', 'forgot', 'reset password', 'new-password','locked-me-out','password-reset',
##                 'forgot password', 'forgot-my-password', 'currently-locked-out', 'password-expired',
##                 'change-your-password', 'temporary-one']
    
    
    promo_words = ['virtual', 'agent', 'chat', 'technology', 'service-now', 'tool',
                   'vehicle', 'virtual assistant', 'virtual agent', 'virtual-assistant',
                     'new-tool', 'ask-chat', 'ask chat', 'ask-i', 'ask-it', 'chat-with-us']

    stemmer = PorterStemmer()
##    password_stems = [stemmer.stem(word) for word in password_words]
    promo_stems = [stemmer.stem(word) for word in promo_words]
    result = {}

    tf_promo = FreqDist([word for word in promo_stems])
    promo_vocab = dict(tf_promo)
    
    
    for data in generate_path(base_path):
        transcript_name = data['jobName'].split('--')[0]
        print(transcript_name)
        # Custom sentence tokanizer using AWS transcribe data
        sentences = tokanize_sentences(data)
        result[transcript_name] = {}
        for i,sentence in enumerate(sentences):
            print(f"Working on sentence {i+1}")
            text = sentence['text']
            filtered_words = preprocess(text)
            freqdist = FreqDist([word for word in filtered_words])
            vocab = {**promo_vocab, **dict(freqdist)}

            tf_vec_sentence = bagofwords(filtered_words, vocab)
            tf_vec_promo = bagofwords(promo_stems, vocab)
            
            tf_vec_sentence_normalized = normalize_tf(tf_vec_sentence)
            tf_vec_promo_normalized = normalize_tf(tf_vec_promo)
            print(f"tf_vec_sentence_normalized: {tf_vec_sentence_normalized}")
            print(f"tf_vec_promo_normalized: {tf_vec_promo_normalized}")
            
            similarity = calculate_cosine_similarity(tf_vec_sentence_normalized,
                                                     tf_vec_promo_normalized)
            print(similarity)

            
##            if result[transcript_name].get('total_tf_vector') is None:
##                result[transcript_name]['total_tf_vector'] = tf_vector
##            else:
##                result[transcript_name]['total_tf_vector'] += tf_vector

##        print(f"Total term frequency vector: {result[transcript_name]['total_tf_vector']}")
            

            
##    promo, words = check_promo(text)
##    score = check_promotion_score(text)
##    print(score)

##    raw_text = """What kind of teva-service-desk the sense to speaking with you, miss survivor with their phone anymore ? Previous-ticket-number oh, i'm sorry, no. Ticket-number,
##            i'm locked-me-out new-password oh, even. The windows. Yet. Okay, could you please tell-me or, uh, user name ? Uh, yeah, kind of linklater. Yeah. Oh. Yeah, okay. Okay,
##            you're calling from star will stoneville correct ? Yes, okay, could you please tell me that i'm off here ? My steven wright. Okay, lemme see here one phone number of years.
##            It's uh, one, four, one, six, two, nine, one, eight, eight, eight, eight. Correct. Okay, i take your account so. Mhm. Mhm. Oh! Yeah. Oh, mhm, yeah. Oh! Yes, she smoked a
##            unlock it now. Okay, that would be good. Yes, my, you could try, though, okay, mhm. Oh, hm, hm. It looks like it's booting up good mhm. Okay, i am in. Thank you very much.
##            Do you get you get number in florida ? Assistance for something else ? Million fine. Okay, as, uh, can i ask ? You haven't ever try to use our virtual-assistant
##            teva-service-desk now, where you click on technology. I'm sorry say that again. Yes, it's. Uh, i wanna visit virtual-assistant it's all have a service now where you open
##            the laws, do you see it on your rice side ? It says, you're crazy. It is something like child with us in real time. If you have, uh, for example, other issue i, which is
##            there i have in the past, and, yeah, it's, just it's, really good, actually makes it very simple. Excellent. Okay, thank you, thank you, we should, i say, have a nice day,
##            thank you, bye bye bye, right ?"""
