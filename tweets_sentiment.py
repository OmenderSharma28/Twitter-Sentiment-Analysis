import sys
import string
import re
import MapReduce
import json

mr = MapReduce.MapReduce()
scores = {}
output = {}
index = 0

def mapper(tweet_data):
    global index
    index += 1
    record1 = tweet_data["text"]
    records = record1.encode('utf-8').translate(None, string.punctuation)
    records2 = records.lower()
    words = re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",records2).split()
    for word in words:
        if word in scores:
            mr.emit_intermediate(index,scores[word])
        else:
            mr.emit_intermediate(index, 0)




def reducer(key, list_of_values):
    print [key,sum(list_of_values)]



if __name__ == '__main__':
    afinnfile = open(sys.argv[1])       # Make dictionary out of AFINN_111.txt file.
    for line in afinnfile:
        term, score  = line.split("\t")  # The file is tab-delimited. #\t means the tab character.
        scores[term] = int(score)  # Convert the score to an integer.
    tweet_data = open(sys.argv[2])
    mr.execute(tweet_data, mapper, reducer)


