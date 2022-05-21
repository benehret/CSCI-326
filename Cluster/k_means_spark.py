import sys
from pyspark.sql import SparkSession
import math

def centers(inputName, outputName, k):
    session = SparkSession.builder.appName("Sum").getOrCreate()
    context  = session.sparkContext
    rdd = context.textFile(inputName)
    if inputName[-3:] == 'csv':
        numbers = rdd.map(lambda line: line.strip().split(',')[1:6])
    else:
        numbers = rdd.map(lambda line: line.strip().split())
    c = []
    
    def findAverage(vals, count):
        avgs = []
        for i in vals:
            avgs.append(i/count)
        return tuple(avgs)

    def findSum(x,y):
        newSums = []
        for i in range(len(x)):
            newSums.append(float(x[i]) + float(y[i]))
        return tuple(newSums)

    def findClosest(x,y):
        distanceList = []
        for center in range(len(y)):
            sums = 0
            for i in range(len(x)):
                sums += (float(y[center][i]) - float(x[i]))**2
            distanceList.append(math.sqrt(sums))
        minDist = min(distanceList)
        minIndex = distanceList.index(minDist)
        return tuple(y[minIndex])
    def findDistance(x,y):
        sums = 0
        for i in range(len(x)):
            sums += (float(y[i]) - float(x[i]))**2
        return math.sqrt(sums)


    c = numbers.takeSample(False,k)
    for i in range(len(c)):
        c[i] = tuple(c[i])
    pointNotChanged = False
    runCount = 1
    while pointNotChanged == False:
        tempnumbers = numbers.map(lambda x: (tuple(findClosest(x,c)),x))
        tempnumbers =  tempnumbers.mapValues(lambda v: (v,1)).reduceByKey(lambda x,y: (findSum(x[0],y[0]),x[1]+y[1])).mapValues(lambda v: findAverage(v[0],v[1]))
        newCs = tempnumbers.filter(lambda x: x[0] != x[1]).collect()
        if newCs == []:
            pointNotChanged = True
        else:
            for i in range(len(newCs)):
                key, avg = newCs[i]
                cIndex = c.index(key)
                newC = avg
                c[cIndex] = newC
        if runCount >= 100:
            pointNotChanged = True
        runCount += 1
        

    oList = []
    oList.append(c)
    tempnumbers = numbers.map(lambda x: tuple((tuple(findClosest(x,c)),tuple(x))))
    tempnumbers = tempnumbers.map(lambda x: findDistance(x[0],x[1])**2)
    tempnumbers = tempnumbers.reduce(lambda x,y: x+y)
    tempnumbers = tempnumbers / numbers.count()
    oList.append(tempnumbers)

    outrdd = context.parallelize(olist)
    outrdd.saveAsTextFile(outputName)

def main():
    inputName = sys.argv[1]
    k = int(sys.argv[3])
    outputName = sys.argv[2]
    centers(inputName, outputName, k)

if __name__ == '__main__':
    main()
