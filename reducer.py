from pyspark import SparkContext

from pprint import pprint

import csv

from collections import defaultdict

sc = SparkContext(appName="PythonStreamingQueueStream")

climateRDD = sc.textFile('ghcnd-county-2019.csv', 32)

sc.setLogLevel("WARN")

def processLine(line, keys, values):

    res = []

    columns = list(csv.reader([line], delimiter=','))[0]

    size = len(columns)

    key = tuple()

    value = tuple()

    for i in range(len(columns)):

        if i in keys:
            key += tuple([columns[i]])
        
        if i in values:
            value += tuple([columns[i]])

    return (key, [value])

with open('result.txt', 'w') as f:

    headers = climateRDD.first()

    headerList = headers.split(",")

    headerList = sc.broadcast(headerList)

    keys = ['state', 'county', 'yearday']

    values = ['attribute', 'value']

    keyOrdinals = []

    valueOrdinals = [] 

    for i in range(len(headerList.value)):

        if headerList.value[i] in keys:
            keyOrdinals.append(i)
        
        if headerList.value[i] in values:
            valueOrdinals.append(i)

    climateRDD = climateRDD.filter(lambda line: line != headers)\
                            .filter(lambda line: len(line.split(',')[keyOrdinals[1]]) > 0)
    
    climateRDD = climateRDD.map(lambda line: processLine(line, keyOrdinals, valueOrdinals))

    climateRDD = climateRDD.reduceByKey(lambda a,b: a+b)
    
    pprint(climateRDD.collect(), f)