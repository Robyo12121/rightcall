import json
import os


common_path = r'C:/Users/RSTAUNTO/Desktop/Python/projects/rightcall_robin/'
data = common_path + r'data/'
transcripts_dir = data + 'transcripts/'

data = []
for file in os.listdir(transcripts_dir):
    print(transcripts_dir + file)
    with open(transcripts_dir + file,'r') as f:
        data.append(json.load(f)['results'])

print(f"No. of files: {len(data)}")


def create_transcript(json_file):
    contents = ""
    items = json_file['items']
    speaker_mapping = getSpeakerMapping(json_file)
    speakerIndex = 0

    for i in range(len(items)):
        reason = ""
        # If the transcription detected the end of a sentence, we'll 
        if items[i]['type'] == 'punctuation':
            if items[i]["alternatives"][0]["content"] == '.':
                newParagraph = True

            # Always assume the first guess is right.
            contents += items[i]["alternatives"][0]["content"]

def getSpeakerMapping(json_file):
    items = json_file['items']
    speakerMapping = []
    for i in range(len(json_file['speaker_labels']['segments'])):
        speakerLabel = json_file['speaker_labels']['segments'][i]
        speakerMapping.append({
                "speakerLabel": speakerLabel['speaker_label'],
                "endTime": float(speakerLabel['end_time'])
            })
    return speakerMapping
                              


##transcript = {}
##for k,v in data.items():
##    transcript[k] = create_transcript(v)

