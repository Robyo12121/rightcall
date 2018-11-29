import json



base_path = 'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/data/transcripts/'

with open(base_path + 'c47fa6TVd10510--MP6KR5.json', 'r') as file:
    data = json.load(file)





def tokanize_sentences(transcribe_data):
    
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
    # below as paragraphs. The paragraph is the unit text that is stored in the 
    # elasticsearch index. It is broken out by punctionation, speaker changes, a long pause
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


if __name__ == '__main__':
    sentences = tokanize_sentences(data)
    for item in sentences:
        print(f"Speaker: {item['speaker']} : {item['text']}")
                
                    
                    
                        
            
