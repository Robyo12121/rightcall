from nltk.tokenize import sent_tokenize, PunktSentenceTokenizer
import json
from itertools import zip_longest
base_path = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/transcripts/'


##with open(base_path + 'c9caf2TVd10618--73YF8D.json', 'r') as file:
##    data = json.load(file)

raw_text = r"""What kind of teva-service-desk the sense to speaking with you, miss survivor with their phone anymore ? Previous-ticket-number oh, i'm sorry, no. Ticket-number,
        i'm locked-me-out new-password oh, even. The windows. Yet. Okay, could you please tell-me or, uh, user name ? Uh, yeah, kind of linklater. Yeah. Oh. Yeah, okay. Okay,
        you're calling from star will stoneville correct ? Yes, okay, could you please tell me that i'm off here ? My steven wright. Okay, lemme see here one phone number of years.
        It's uh, one, four, one, six, two, nine, one, eight, eight, eight, eight. Correct. Okay, i take your account so. Mhm. Mhm. Oh! Yeah. Oh, mhm, yeah. Oh! Yes, she smoked a
        unlock it now. Okay, that would be good. Yes, my, you could try, though, okay, mhm. Oh, hm, hm. It looks like it's booting up good mhm. Okay, i am in. Thank you very much.
        Do you get you get number in florida ? Assistance for something else ? Million fine. Okay, as, uh, can i ask ? You haven't ever try to use our virtual-assistant
        teva-service-desk now, where you click on technology. I'm sorry say that again. Yes, it's. Uh, i wanna visit virtual-assistant it's all have a service now where you open
        the laws, do you see it on your rice side ? It says, you're crazy. It is something like child with us in real time. If you have, uh, for example, other issue i, which is
        there i have in the past, and, yeah, it's, just it's, really good, actually makes it very simple. Excellent. Okay, thank you, thank you, we should, i say, have a nice day,
        thank you, bye bye bye, right ?"""

##transcript_text = data['results']['transcripts'][0]['transcript']
##sentences = sent_tokenize(transcript_text)

tokenizer = PunktSentenceTokenizer(raw_text)
punkt_sentences = tokenizer.tokenize(raw_text)
sentences = sent_tokenize(raw_text)

##for i, sent in enumerate(sentences):
##    print(f"{i} : {sent}")

for combined in zip_longest(punkt_sentences, sentences):
    print(combined[0])
    print(combined[1])

print(len(sentences))
print(len(punkt_sentences))
