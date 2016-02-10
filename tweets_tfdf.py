import MapReduce
import sys
import re
import string
import json

mr = MapReduce.MapReduce()
num = {}
scores1 = {}
index = 0
count1 = 0
df = {}
Hash = {}
distinct = set()
k = 0
lineNum = 0

def mapper(tweet_data):
    global index,df,distinct,lineNum, Hash
    index+=1
    lineNum += 1
    record1 = tweet_data["text"]
    records = record1.encode('utf-8').translate(None, string.punctuation)
    tweet = re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)"," ",records).split()
    #print tweet
    #print df
    #raw_input()
    distinct.clear()

    for word in tweet:
        #distinct.add(word)
        if word in Hash:
            if lineNum in Hash[word]:
                Hash[word][lineNum] += 1
            else:
                Hash[word][lineNum] = 1
        else:
            Hash[word] = {}
            Hash[word][lineNum] = 1
    '''
    for key in Hash.keys():
        print key,':',Hash[key]
        #num = tweet.count(word)
     '''
    '''
    for word in distinct:
        try:
            df[word] += 1
        except:
            df[word] = 1
    '''
    for key in Hash.keys():
        mr.emit_intermediate(key,Hash[key])

    '''
    for word in words:


        num = words.count(word)
        #print word,num
        #raw_input()
        S.add(word)

        #scores1[index].append(num)
        #print scores1

        #mr.emit_intermediate1(index,num)
        #count1 = int(num)
        #count2 = len(count1)
        mr.emit_intermediate(word,num)
    '''

#def reducer1(key,list_of_values):
    #print [key,list_of_values]

def reducer(key, list_of_values):
    count1 = len(list_of_values[0])
    print [key,count1,list_of_values[0]]




if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

