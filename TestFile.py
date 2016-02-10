import sys
import string
import re

import json


scores = {}
index = 0

def mapper(tweet_data):
    global index
    index += 1
    record1 = tweet_data["text"]
    words = re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",record1).split()
    print words
    for word in words:
        if word in scores:
            mr.emit_intermediate(index, scores)
        else:
            mr.emit_intermediate(index, 0)




def execute(self, tweet_data, mapper, reducer):
        # read each line from input file; call Map function on each record
        for line in tweet_data:
            record = json.loads(line)
            mapper(record)







if __name__ == '__main__':
    afinnfile = open("AFINN-111.txt")       # Make dictionary out of AFINN_111.txt file.
    for line in afinnfile:
        term, score  = line.split("\t")  # The file is tab-delimited. #\t means the tab character.
        scores[term] = int(score)  # Convert the score to an integer.
    tweet_data = open("nayankanangisubramanya_rao_first20.txt")
    execute(tweet_data, mapper)