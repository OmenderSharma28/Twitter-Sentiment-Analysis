import json

class MapReduce:
    def __init__(self):
        # initialize dictionary for intermediate values from Map task
        self.intermediate = {}
        # initialize list for results of Reduce task
        self.intermediate1 = {}

        self.result = []

    def emit_intermediate1(self, key, value):
        # if key not already in dictionary, set value to empty list
        self.intermediate1.setdefault(key, [])
        # add value to list associated with key
        self.intermediate1[key].append(value)

    def emit_intermediate(self, key, value):
        # if key not already in dictionary, set value to empty list
        self.intermediate.setdefault(key, [])
        # add value to list associated with key
        self.intermediate[key].append(value)

    def emit(self, value):
        # append value to list of results
        self.result.append(value) 

    def execute(self, tweet_data, mapper, reducer):
        # read each line from input file; call Map function on each record
        for line in tweet_data:
            record = json.loads(line)
            mapper(record)
        
        # for each key:valuelist in intermediate dictionary, call Reduce task
        #for key in self.intermediate1:      #Testpurpose
            #reducer1(key, self.intermediate1[key])


        for key in self.intermediate:
            reducer(key, self.intermediate[key])

        jenc = json.JSONEncoder()
        self.result.sort()
        # print each result in result list
        for item in self.result:
            print jenc.encode(item)
